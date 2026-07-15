"""
backfill_market_data.py — One-time backfill of market_data from yfinance.

Sources:
  - nse_stock_list.txt            (STOCK tickers)
  - nse_etf_list.txt              (ETF tickers)
  - PORTFOLIO > DELISTED > col C  (delisted STOCK tickers, NSE:SYMBOL format)

Fetches daily OHLCV from yfinance from 2024-01-01 to today.
Upserts into db/trading.db > market_data table.
Retries critical failures up to 3 times with exponential backoff.
Sends email report with severity-split results.

Flags:
    --mode-stock        Fetch STOCK tickers only
    --mode-etf          Fetch ETF tickers only
    --mode-delisted     Fetch DELISTED tickers only
    (no mode flags)     Fetch all — STOCK + ETF + DELISTED
    --dry-run           Show tickers, no DB writes, no email
    --batch-size N      Tickers per batch (default 10, sleep 3s between)
"""

import argparse
import json
import logging
import smtplib
import sys
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import yfinance as yf
from google.oauth2.service_account import Credentials
import gspread

# ── Path setup ────────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "db"))

from db import get_conn, init_db, update_meta
from runtime_paths import get_creds_path, get_smtp_token_path

# ── Config ────────────────────────────────────────────────────────────────────
START_DATE        = "2024-01-01"
DEFAULT_BATCH     = 10
SLEEP_BETWEEN     = 3.0

STOCK_LIST        = _REPO_ROOT / "nse_stock_list.txt"
ETF_LIST          = _REPO_ROOT / "nse_etf_list.txt"
PORTFOLIO_SHEET   = "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s"
DELISTED_TAB      = "DELISTED"

# Email
FROM_EMAIL        = "sugamkuchhal@gmail.com"
SMTP_USER         = "sugamkuchhal@gmail.com"
SMTP_SERVER       = "smtp.gmail.com"
SMTP_PORT         = 587
TO_EMAIL          = "sugam.kuchhal.iimc@gmail.com"

# Validation
MIN_EXPECTED_ROWS = 300     # warning only — not critical
MAX_STALE_DAYS    = 7       # critical if last date older than this

# Retry
MAX_RETRIES       = 3
RETRY_BACKOFF     = [5, 15, 45]   # seconds between retries

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── Ticker loading ────────────────────────────────────────────────────────────

def _load_txt(path: Path, ticker_type: str) -> list[tuple[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Ticker file not found: {path}")
    tickers = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        symbol = f"NSE:{s}" if not s.startswith("NSE:") else s
        tickers.append((symbol.upper(), ticker_type))
    return tickers


def _load_delisted(creds_path: Path) -> list[tuple[str, str]]:
    log.info("Loading delisted tickers from PORTFOLIO > DELISTED...")
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    client = gspread.authorize(creds)
    ws     = client.open_by_key(PORTFOLIO_SHEET).worksheet(DELISTED_TAB)
    col_c  = ws.col_values(3)
    tickers = []
    for val in col_c[1:]:
        s = val.strip()
        if not s:
            continue
        symbol = s.upper() if s.startswith("NSE:") else f"NSE:{s.upper()}"
        tickers.append((symbol, "STOCK"))
    log.info(f"  {len(tickers)} delisted tickers loaded.")
    return tickers


def load_tickers(run_stock: bool, run_etf: bool, run_delisted: bool) -> list[tuple[str, str]]:
    tickers: dict[str, str] = {}
    if run_stock:
        for symbol, t in _load_txt(STOCK_LIST, "STOCK"):
            tickers[symbol] = t
    if run_etf:
        for symbol, t in _load_txt(ETF_LIST, "ETF"):
            tickers[symbol] = t
    if run_delisted:
        try:
            for symbol, t in _load_delisted(get_creds_path()):
                if symbol not in tickers:
                    tickers[symbol] = t
        except Exception as e:
            log.warning(f"Could not load delisted tickers (non-fatal): {e}")

    result = sorted(tickers.items())
    log.info(
        f"Total unique tickers: {len(result)} "
        f"(STOCK: {sum(1 for _,t in result if t=='STOCK')}, "
        f"ETF: {sum(1 for _,t in result if t=='ETF')})"
    )
    return result


# ── yfinance fetch ────────────────────────────────────────────────────────────

def _to_yf_symbol(symbol: str) -> str:
    return symbol.replace("NSE:", "") + ".NS"


def fetch_ohlcv(symbol: str) -> tuple[pd.DataFrame | None, str | None]:
    """
    Fetches OHLCV from 2024-01-01 to today.
    Returns (DataFrame, None) on success or (None, error_reason) on failure.
    """
    yf_sym = _to_yf_symbol(symbol)
    try:
        df = yf.download(
            yf_sym,
            start=START_DATE,
            end=datetime.now().strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=True,
            progress=False,
        )
        if df.empty:
            return None, "No data returned from yfinance"

        df = df.reset_index()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]

        df = df.rename(columns={
            "Date":   "date",
            "Close":  "close",
            "Low":    "low",
            "High":   "high",
            "Volume": "volume",
        })
        df["date"]   = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["volume"] = (df["volume"] * df["close"]) / 1e7
        df = df[["date", "close", "low", "high", "volume"]].copy()
        df = df.dropna(subset=["close"])

        if df.empty:
            return None, "All rows dropped after cleaning"

        return df, None

    except Exception as e:
        return None, str(e)


# ── DB upsert ─────────────────────────────────────────────────────────────────

def upsert_to_db(symbol: str, ticker_type: str, df: pd.DataFrame) -> int:
    now  = datetime.now(timezone.utc).isoformat()
    rows = [
        (row["date"], symbol, row["close"], row["low"],
         row["high"], row["volume"], ticker_type, now)
        for _, row in df.iterrows()
    ]
    with get_conn() as conn:
        conn.executemany("""
            INSERT INTO market_data (date, symbol, close, low, high, volume, type, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, symbol) DO UPDATE SET
                close      = excluded.close,
                low        = excluded.low,
                high       = excluded.high,
                volume     = excluded.volume,
                type       = excluded.type,
                updated_at = excluded.updated_at
        """, rows)
    return len(rows)


# ── Validation ────────────────────────────────────────────────────────────────

def validate_ticker(symbol: str) -> tuple[list[str], list[str]]:
    """
    Returns (critical_issues, warnings) for a ticker after upsert.

    Critical:
      - Stale data: last date > MAX_STALE_DAYS old
      - Null/zero values in any of: close, low, high, volume (any single row)

    Warning:
      - Row count below MIN_EXPECTED_ROWS
    """
    criticals = []
    warnings  = []
    today     = datetime.now().date()

    with get_conn() as conn:
        summary = conn.execute("""
            SELECT
                COUNT(*)                                                    AS row_count,
                MAX(date)                                                   AS last_date,
                SUM(CASE WHEN close  IS NULL OR close  = 0 THEN 1 ELSE 0 END) AS bad_close,
                SUM(CASE WHEN low    IS NULL OR low    = 0 THEN 1 ELSE 0 END) AS bad_low,
                SUM(CASE WHEN high   IS NULL OR high   = 0 THEN 1 ELSE 0 END) AS bad_high,
                SUM(CASE WHEN volume IS NULL OR volume = 0 THEN 1 ELSE 0 END) AS bad_volume
            FROM market_data
            WHERE symbol = ?
        """, (symbol,)).fetchone()

    if not summary or summary["row_count"] == 0:
        criticals.append("No rows found in DB after upsert")
        return criticals, warnings

    # Critical: stale data
    if summary["last_date"]:
        last_date  = datetime.strptime(summary["last_date"], "%Y-%m-%d").date()
        stale_days = (today - last_date).days
        if stale_days > MAX_STALE_DAYS:
            criticals.append(
                f"Stale data: last date is {summary['last_date']} ({stale_days} days ago)"
            )

    # Critical: null/zero per field
    for field in ["close", "low", "high", "volume"]:
        bad = summary[f"bad_{field}"]
        if bad and bad > 0:
            criticals.append(f"Null/zero {field}: {bad} row(s)")

    # Warning: low row count
    if summary["row_count"] < MIN_EXPECTED_ROWS:
        warnings.append(
            f"Low row count: {summary['row_count']} (expected ≥ {MIN_EXPECTED_ROWS})"
        )

    return criticals, warnings


# ── Pre-write DataFrame validation ───────────────────────────────────────────

def _validate_dataframe(symbol: str, df: pd.DataFrame) -> list[str]:
    """
    Validates a fetched DataFrame BEFORE writing to DB.
    Returns list of critical issues found in the raw data.
    Only checks fields that would be critical if written.
    """
    criticals = []
    for field in ["close", "low", "high", "volume"]:
        if field not in df.columns:
            criticals.append(f"Missing column: {field}")
            continue
        bad = df[field].isna().sum() + (df[field] == 0).sum()
        if bad > 0:
            criticals.append(f"Null/zero {field}: {int(bad)} row(s) in fetched data")
    return criticals


# ── Retry logic ───────────────────────────────────────────────────────────────

def fetch_and_upsert_with_retry(
    symbol: str,
    ticker_type: str,
) -> tuple[int, str | None, list[str], list[str]]:
    """
    Fetches, upserts, and validates with up to MAX_RETRIES retries on critical failure.

    Returns:
        (rows_upserted, fatal_reason, critical_issues, warnings)
        fatal_reason is None on success, string on permanent failure.
    """
    last_reason  = None
    last_crits   = []
    last_warns   = []

    for attempt in range(1, MAX_RETRIES + 1):
        if attempt > 1:
            sleep_secs = RETRY_BACKOFF[attempt - 2]
            log.warning(f"  Retry {attempt}/{MAX_RETRIES} for {symbol} in {sleep_secs}s...")
            time.sleep(sleep_secs)

        # Fetch fresh data
        df, fetch_error = fetch_ohlcv(symbol)
        if df is None:
            last_reason = fetch_error
            log.error(f"  Attempt {attempt}: fetch failed — {fetch_error}")
            continue

        # Validate the fetched data BEFORE writing to DB
        # Only upsert if the new fetch is clean — never overwrite good DB data with bad fetch
        temp_crits = _validate_dataframe(symbol, df)
        if temp_crits:
            last_crits = temp_crits
            last_reason = None
            for c in temp_crits:
                log.warning(f"  Attempt {attempt}: fetched data has critical issue — {c}")
            continue

        # Clean fetch — upsert into DB
        rows = upsert_to_db(symbol, ticker_type, df)
        log.info(f"  Attempt {attempt}: ✅ {rows} rows upserted.")

        # Validate what's now in the DB
        crits, warns = validate_ticker(symbol)
        if not crits:
            return rows, None, [], warns

        # DB still has issues (e.g. pre-existing bad rows not covered by this fetch)
        last_crits = crits
        last_warns = warns
        last_reason = None
        for c in crits:
            log.warning(f"  Attempt {attempt}: ⚠️  DB critical: {c}")
        # Don't retry further — new data was clean, DB issues are pre-existing
        return rows, None, crits, warns

    # All retries exhausted
    if last_reason:
        return 0, last_reason, [], []
    return 0, f"Critical issues unresolved after {MAX_RETRIES} attempts", last_crits, last_warns


# ── Email ─────────────────────────────────────────────────────────────────────

def _load_smtp_password() -> str | None:
    path = get_smtp_token_path()
    if not path.exists():
        log.warning(f"SMTP token not found at {path}")
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("smtp_password")
    except Exception as e:
        log.warning(f"Could not read SMTP token: {e}")
        return None


def send_email_report(
    modes_run:    list[str],
    total:        int,
    succeeded:    int,
    total_rows:   int,
    fatal:        list[tuple[str, str]],          # [(symbol, reason)]
    criticals:    dict[str, list[str]],            # {symbol: [issue, ...]}
    warnings:     dict[str, list[str]],            # {symbol: [warning, ...]}
    duration_secs: float,
):
    smtp_password = _load_smtp_password()
    if not smtp_password:
        log.warning("No SMTP password — skipping email report.")
        return

    now_str    = datetime.now().strftime("%d %b %Y %H:%M")
    has_crits  = bool(fatal or criticals)
    has_warns  = bool(warnings)
    status     = "🔴 Critical Issues" if has_crits else ("⚠️ Warnings" if has_warns else "✅ All Clear")
    subject    = f"[Backfill] {status} — {now_str} | {succeeded}/{total} tickers"

    def _section(title: str, color: str, body: str) -> str:
        return f"""
        <div style="margin:16px 0;padding:14px 18px;border-left:4px solid {color};
                    background:#fafafa;border-radius:4px;">
            <div style="font-weight:600;font-size:14px;color:{color};
                        margin-bottom:8px;">{title}</div>
            {body}
        </div>"""

    def _table(headers: list[str], rows: list[list[str]]) -> str:
        ths = "".join(
            f'<th style="text-align:left;padding:6px 12px;background:#f0f0f0;'
            f'font-size:12px;">{h}</th>' for h in headers
        )
        trs = ""
        for i, r in enumerate(rows):
            bg  = "#ffffff" if i % 2 == 0 else "#f9f9f9"
            tds = "".join(
                f'<td style="padding:6px 12px;font-size:12px;">{v}</td>' for v in r
            )
            trs += f'<tr style="background:{bg};">{tds}</tr>'
        return (
            f'<table style="border-collapse:collapse;width:100%;border:1px solid #e0e0e0;">'
            f'<thead><tr>{ths}</tr></thead><tbody>{trs}</tbody></table>'
        )

    # Summary
    summary_html = _section(
        "📊 Summary", "#1a73e8",
        _table(["Field", "Value"], [
            ["Modes run",          ", ".join(modes_run)],
            ["Tickers attempted",  str(total)],
            ["Succeeded",          str(succeeded)],
            ["Critical failures",  str(len(fatal) + len(criticals))],
            ["Warnings",           str(len(warnings))],
            ["Rows upserted",      f"{total_rows:,}"],
            ["Duration",           f"{duration_secs:.0f}s"],
        ])
    )

    # Critical: fatal fetches
    fatal_html = ""
    if fatal:
        fatal_html = _section(
            f"🔴 Fatal — No Data Fetched ({len(fatal)})", "#d93025",
            _table(
                ["Symbol", "Reason"],
                [[s, r] for s, r in fatal]
            )
        )

    # Critical: validation failures after retries
    crit_html = ""
    if criticals:
        rows = []
        for sym, issues in sorted(criticals.items()):
            for issue in issues:
                rows.append([sym, issue])
        crit_html = _section(
            f"🔴 Critical — Validation Failed After Retries ({len(rows)} issues)", "#d93025",
            _table(["Symbol", "Issue"], rows)
        )

    # Warnings: low row count
    warn_html = ""
    if warnings:
        rows = []
        for sym, warns in sorted(warnings.items()):
            for w in warns:
                rows.append([sym, w])
        warn_html = _section(
            f"⚠️ Warnings — Low Row Count ({len(rows)})", "#f9a825",
            _table(["Symbol", "Warning"], rows)
        )

    # All clear
    clear_html = ""
    if not has_crits and not has_warns:
        clear_html = _section(
            "✅ All tickers validated successfully — no issues found.", "#0f9d58", ""
        )

    html = f"""
    <html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
                       color:#1a1a1a;max-width:800px;margin:0 auto;padding:20px;">
        <h2 style="color:#1a1a2e;border-bottom:2px solid #e0e0e0;padding-bottom:10px;">
            Market Data Backfill Report
        </h2>
        <p style="color:#666;font-size:13px;">Run at {now_str}</p>
        {summary_html}
        {fatal_html}
        {crit_html}
        {warn_html}
        {clear_html}
    </body></html>
    """

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = FROM_EMAIL
        msg["To"]      = TO_EMAIL
        msg.attach(MIMEText("View this email in an HTML-capable client.", "plain"))
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, smtp_password)
            server.sendmail(FROM_EMAIL, [TO_EMAIL], msg.as_string())

        log.info(f"✅ Email report sent to {TO_EMAIL}")
    except Exception as e:
        log.error(f"Failed to send email: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill market_data from yfinance.")
    parser.add_argument("--mode-stock",    action="store_true", help="Fetch STOCK tickers.")
    parser.add_argument("--mode-etf",      action="store_true", help="Fetch ETF tickers.")
    parser.add_argument("--mode-delisted", action="store_true", help="Fetch DELISTED tickers.")
    parser.add_argument("--dry-run",       action="store_true", help="Show tickers, no writes.")
    parser.add_argument("--batch-size",    type=int, default=DEFAULT_BATCH)
    args = parser.parse_args()

    run_all      = not (args.mode_stock or args.mode_etf or args.mode_delisted)
    run_stock    = run_all or args.mode_stock
    run_etf      = run_all or args.mode_etf
    run_delisted = run_all or args.mode_delisted

    modes_run = []
    if run_stock:    modes_run.append("STOCK")
    if run_etf:      modes_run.append("ETF")
    if run_delisted: modes_run.append("DELISTED")

    init_db()
    tickers = load_tickers(run_stock, run_etf, run_delisted)

    if args.dry_run:
        log.info("DRY RUN — tickers that would be fetched:")
        for symbol, t in tickers:
            print(f"  {symbol:30s} {t}")
        return

    total        = len(tickers)
    total_rows   = 0
    fatal        = []           # [(symbol, reason)] — fetch never succeeded
    crit_map     = {}           # {symbol: [critical issues]} — after all retries
    warn_map     = {}           # {symbol: [warnings]}
    start_time   = time.time()

    log.info(f"Starting backfill — modes: {', '.join(modes_run)}")
    log.info(f"Tickers: {total} | Start: {START_DATE} | Batch: {args.batch_size} | "
             f"Sleep: {SLEEP_BETWEEN}s | Max retries: {MAX_RETRIES}")

    for i, (symbol, ticker_type) in enumerate(tickers, 1):
        log.info(f"[{i}/{total}] {symbol} ({ticker_type})")

        rows, fatal_reason, crits, warns = fetch_and_upsert_with_retry(symbol, ticker_type)

        if fatal_reason and not crits:
            # Fetch never returned data
            fatal.append((symbol, fatal_reason))
        elif crits:
            # Data fetched but critical validation issues remain after retries
            crit_map[symbol] = crits
            total_rows += rows
        else:
            total_rows += rows

        if warns:
            warn_map[symbol] = warns

        if i % args.batch_size == 0 and i < total:
            log.info(f"  Batch {i // args.batch_size} done — sleeping {SLEEP_BETWEEN}s...")
            time.sleep(SLEEP_BETWEEN)

    # Update meta
    with get_conn() as conn:
        update_meta(conn, "market_data", total_rows)

    duration  = time.time() - start_time
    succeeded = total - len(fatal) - len(crit_map)

    log.info(f"\n{'='*55}")
    log.info(f"✅ Backfill complete in {duration:.0f}s")
    log.info(f"   Modes:             {', '.join(modes_run)}")
    log.info(f"   Succeeded:         {succeeded}/{total}")
    log.info(f"   Fatal failures:    {len(fatal)}")
    log.info(f"   Critical issues:   {len(crit_map)}")
    log.info(f"   Warnings:          {len(warn_map)}")
    log.info(f"   Rows upserted:     {total_rows:,}")

    send_email_report(
        modes_run, total, succeeded, total_rows,
        fatal, crit_map, warn_map, duration
    )

    # Commit DB back to repo
    from git_utils import commit_file_if_changed
    commit_file_if_changed(
        filepath="db/trading.db",
        message="chore: update trading.db — backfill [skip ci]",
        repo_root=_REPO_ROOT,
    )


if __name__ == "__main__":
    main()
