"""
backfill_market_data.py — One-time backfill of market_data from yfinance.

Sources:
  - nse_stock_list.txt       (STOCK tickers)
  - nse_etf_list.txt         (ETF tickers)
  - PORTFOLIO > DELISTED > col C  (delisted STOCK tickers, NSE:SYMBOL format)

Fetches daily OHLCV from yfinance from 2024-01-01 to today.
Upserts into db/trading.db > market_data table.
Sends email report with validation results.

Usage:
    python3 db/backfill_market_data.py                          # run all modes
    python3 db/backfill_market_data.py --mode-stock             # stocks only
    python3 db/backfill_market_data.py --mode-etf               # ETFs only
    python3 db/backfill_market_data.py --mode-delisted          # delisted only
    python3 db/backfill_market_data.py --mode-stock --mode-etf  # combine modes
    python3 db/backfill_market_data.py --dry-run                # show tickers, no writes
    python3 db/backfill_market_data.py --batch-size 5           # slower, more peaceful
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

# Validation thresholds
MIN_EXPECTED_ROWS = 300     # ~18 months of trading days from 2024-01-01
MAX_STALE_DAYS    = 7       # last DB date should be within this many calendar days

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


def fetch_ohlcv(symbol: str) -> pd.DataFrame | None:
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
            log.warning(f"  No data returned for {symbol}")
            return None

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
        return df

    except Exception as e:
        log.error(f"  Failed to fetch {symbol}: {e}")
        return None


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

def validate_ticker(symbol: str) -> list[str]:
    """
    Runs post-upsert checks for a ticker. Returns list of warning strings.
    Checks:
      1. Row count below minimum
      2. Last date is stale (> MAX_STALE_DAYS old)
      3. Any null close prices
    """
    warnings = []
    today = datetime.now().date()

    with get_conn() as conn:
        row = conn.execute("""
            SELECT
                COUNT(*)                as row_count,
                MAX(date)               as last_date,
                SUM(CASE WHEN close IS NULL OR close = 0 THEN 1 ELSE 0 END) as bad_prices
            FROM market_data
            WHERE symbol = ?
        """, (symbol,)).fetchone()

    if not row or row["row_count"] == 0:
        warnings.append("No rows found in DB after upsert")
        return warnings

    if row["row_count"] < MIN_EXPECTED_ROWS:
        warnings.append(f"Low row count: {row['row_count']} (expected ≥ {MIN_EXPECTED_ROWS})")

    if row["last_date"]:
        last_date = datetime.strptime(row["last_date"], "%Y-%m-%d").date()
        stale_days = (today - last_date).days
        if stale_days > MAX_STALE_DAYS:
            warnings.append(f"Stale data: last date is {row['last_date']} ({stale_days} days ago)")

    if row["bad_prices"] and row["bad_prices"] > 0:
        warnings.append(f"Null/zero close prices: {row['bad_prices']} rows")

    return warnings


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
    modes_run: list[str],
    total: int,
    succeeded: int,
    total_rows: int,
    failed: list[tuple[str, str]],       # [(symbol, reason)]
    warnings: dict[str, list[str]],      # {symbol: [warning, ...]}
    duration_secs: float,
):
    smtp_password = _load_smtp_password()
    if not smtp_password:
        log.warning("No SMTP password — skipping email report.")
        return

    now_str   = datetime.now().strftime("%d %b %Y %H:%M")
    has_issues = bool(failed or warnings)
    status     = "⚠️ Issues Found" if has_issues else "✅ All Clear"
    subject    = f"[Backfill] {status} — {now_str} | {succeeded}/{total} tickers"

    # ── HTML ──────────────────────────────────────────────────────────────────
    def _section(title: str, color: str, body: str) -> str:
        return f"""
        <div style="margin:16px 0;padding:14px 18px;border-left:4px solid {color};
                    background:#fafafa;border-radius:4px;">
            <div style="font-weight:600;font-size:14px;color:{color};
                        margin-bottom:8px;">{title}</div>
            {body}
        </div>"""

    def _table(headers: list[str], rows: list[list[str]]) -> str:
        ths = "".join(f'<th style="text-align:left;padding:6px 12px;'
                      f'background:#f0f0f0;font-size:12px;">{h}</th>' for h in headers)
        trs = ""
        for i, r in enumerate(rows):
            bg = "#ffffff" if i % 2 == 0 else "#f9f9f9"
            tds = "".join(f'<td style="padding:6px 12px;font-size:12px;">{v}</td>' for v in r)
            trs += f'<tr style="background:{bg};">{tds}</tr>'
        return (f'<table style="border-collapse:collapse;width:100%;'
                f'border:1px solid #e0e0e0;">'
                f'<thead><tr>{ths}</tr></thead><tbody>{trs}</tbody></table>')

    # Summary section
    summary_rows = [
        ["Modes run",        ", ".join(modes_run)],
        ["Tickers attempted", str(total)],
        ["Succeeded",         str(succeeded)],
        ["Failed",            str(len(failed))],
        ["Rows upserted",     f"{total_rows:,}"],
        ["Duration",          f"{duration_secs:.0f}s"],
    ]
    summary_html = _section(
        "📊 Summary", "#1a73e8",
        _table(["Field", "Value"], summary_rows)
    )

    # Failed section
    failed_html = ""
    if failed:
        failed_html = _section(
            f"❌ Failed Fetches ({len(failed)})", "#d93025",
            _table(["Symbol", "Reason"], [[s, r] for s, r in failed])
        )

    # Warnings section
    warn_html = ""
    if warnings:
        warn_rows = []
        for sym, warns in sorted(warnings.items()):
            for w in warns:
                warn_rows.append([sym, w])
        warn_html = _section(
            f"⚠️ Validation Warnings ({len(warn_rows)})", "#f9a825",
            _table(["Symbol", "Warning"], warn_rows)
        )

    all_clear_html = ""
    if not has_issues:
        all_clear_html = _section(
            "✅ All tickers validated successfully", "#0f9d58", ""
        )

    html = f"""
    <html><body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
                       color:#1a1a1a;max-width:800px;margin:0 auto;padding:20px;">
        <h2 style="color:#1a1a2e;border-bottom:2px solid #e0e0e0;padding-bottom:10px;">
            Market Data Backfill Report
        </h2>
        <p style="color:#666;font-size:13px;">Run at {now_str}</p>
        {summary_html}
        {failed_html}
        {warn_html}
        {all_clear_html}
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
        log.error(f"Failed to send email report: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill market_data from yfinance.")
    parser.add_argument("--mode-stock",    action="store_true", help="Fetch STOCK tickers.")
    parser.add_argument("--mode-etf",      action="store_true", help="Fetch ETF tickers.")
    parser.add_argument("--mode-delisted", action="store_true", help="Fetch DELISTED tickers.")
    parser.add_argument("--dry-run",       action="store_true", help="Show tickers, no DB writes.")
    parser.add_argument("--batch-size",    type=int, default=DEFAULT_BATCH)
    args = parser.parse_args()

    # If no mode specified, run all
    run_all     = not (args.mode_stock or args.mode_etf or args.mode_delisted)
    run_stock   = run_all or args.mode_stock
    run_etf     = run_all or args.mode_etf
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
    failed       = []       # [(symbol, reason)]
    warn_map     = {}       # {symbol: [warnings]}
    start_time   = time.time()

    log.info(f"Starting backfill — modes: {', '.join(modes_run)}")
    log.info(f"Tickers: {total} | Start: {START_DATE} | Batch: {args.batch_size} | Sleep: {SLEEP_BETWEEN}s")

    for i, (symbol, ticker_type) in enumerate(tickers, 1):
        log.info(f"[{i}/{total}] {symbol} ({ticker_type})")
        df = fetch_ohlcv(symbol)

        if df is None or df.empty:
            failed.append((symbol, "No data returned from yfinance"))
            continue

        rows = upsert_to_db(symbol, ticker_type, df)
        total_rows += rows
        log.info(f"  ✅ {rows} rows upserted.")

        # Validate
        ticker_warnings = validate_ticker(symbol)
        if ticker_warnings:
            warn_map[symbol] = ticker_warnings
            for w in ticker_warnings:
                log.warning(f"  ⚠️  {w}")

        if i % args.batch_size == 0 and i < total:
            log.info(f"  Batch {i // args.batch_size} done — sleeping {SLEEP_BETWEEN}s...")
            time.sleep(SLEEP_BETWEEN)

    # Update meta
    with get_conn() as conn:
        update_meta(conn, "market_data", total_rows)

    duration = time.time() - start_time
    succeeded = total - len(failed)

    log.info(f"\n{'='*55}")
    log.info(f"✅ Backfill complete in {duration:.0f}s")
    log.info(f"   Modes run:          {', '.join(modes_run)}")
    log.info(f"   Tickers processed:  {succeeded}/{total}")
    log.info(f"   Rows upserted:      {total_rows:,}")
    log.info(f"   Validation warnings:{len(warn_map)}")
    if failed:
        log.warning(f"   Failed ({len(failed)}): {', '.join(s for s,_ in failed)}")

    # Send email report
    send_email_report(
        modes_run, total, succeeded, total_rows,
        failed, warn_map, duration
    )


if __name__ == "__main__":
    main()
