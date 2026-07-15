"""
fetch_market_data.py — Daily fetch of OHLCV data into market_data table.

Runs after EOD market close. For each ticker in nse_stock_list.txt and
nse_etf_list.txt, fetches today's OHLCV from yfinance and upserts into
market_data. After upsert, purges rows older than 30 months.

Usage:
    python3 db/fetch_market_data.py
    python3 db/fetch_market_data.py --dry-run
    python3 db/fetch_market_data.py --batch-size 5
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "db"))

from db import get_conn, init_db, update_meta
from runtime_paths import get_creds_path

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_BATCH   = 10
SLEEP_BETWEEN   = 3.0           # seconds between batches
PURGE_MONTHS    = 30            # delete rows older than this

STOCK_LIST      = _REPO_ROOT / "nse_stock_list.txt"
ETF_LIST        = _REPO_ROOT / "nse_etf_list.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── Ticker loading ────────────────────────────────────────────────────────────

def load_tickers() -> list[tuple[str, str]]:
    """Returns deduplicated list of (NSE:SYMBOL, type) from both txt files."""
    tickers: dict[str, str] = {}

    for path, ticker_type in [(STOCK_LIST, "STOCK"), (ETF_LIST, "ETF")]:
        if not path.exists():
            raise FileNotFoundError(f"Ticker file not found: {path}")
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s:
                continue
            symbol = f"NSE:{s}" if not s.startswith("NSE:") else s
            tickers[symbol.upper()] = ticker_type

    result = sorted(tickers.items())
    log.info(f"Tickers loaded: {len(result)} "
             f"(STOCK: {sum(1 for _,t in result if t=='STOCK')}, "
             f"ETF: {sum(1 for _,t in result if t=='ETF')})")
    return result


# ── yfinance fetch ────────────────────────────────────────────────────────────

def _to_yf_symbol(symbol: str) -> str:
    return symbol.replace("NSE:", "") + ".NS"


def fetch_today(symbol: str) -> pd.DataFrame | None:
    """
    Fetches last 5 days from yfinance and returns only today's row.
    Using 5d window avoids issues where today's data isn't available yet
    — falls back to last available trading day.
    """
    yf_sym = _to_yf_symbol(symbol)
    try:
        df = yf.download(
            yf_sym,
            period="5d",
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
        df = df[["date", "close", "low", "high", "volume"]].dropna(subset=["close"])

        # Return only the latest available row
        return df.tail(1)

    except Exception as e:
        log.error(f"  Failed to fetch {symbol}: {e}")
        return None


# ── DB upsert ─────────────────────────────────────────────────────────────────

def upsert_row(symbol: str, ticker_type: str, df: pd.DataFrame) -> int:
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


# ── Purge ─────────────────────────────────────────────────────────────────────

def purge_old_data() -> int:
    """Deletes rows older than PURGE_MONTHS. Returns count deleted."""
    with get_conn() as conn:
        cursor = conn.execute(f"""
            DELETE FROM market_data
            WHERE date < date('now', '-{PURGE_MONTHS} months')
        """)
        deleted = cursor.rowcount
    if deleted > 0:
        log.info(f"🗑️  Purged {deleted:,} rows older than {PURGE_MONTHS} months.")
    else:
        log.info(f"Purge: nothing to delete (no rows older than {PURGE_MONTHS} months).")
    return deleted


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch today's market data into SQLite.")
    parser.add_argument("--dry-run",    action="store_true", help="Show tickers, skip writes.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH)
    args = parser.parse_args()

    init_db()
    tickers = load_tickers()

    if args.dry_run:
        log.info("DRY RUN — tickers that would be fetched:")
        for symbol, t in tickers:
            print(f"  {symbol:30s} {t}")
        return

    total      = len(tickers)
    total_rows = 0
    failed     = []

    log.info(f"Fetching today's data for {total} tickers...")

    for i, (symbol, ticker_type) in enumerate(tickers, 1):
        log.info(f"[{i}/{total}] {symbol}")
        df = fetch_today(symbol)

        if df is None or df.empty:
            failed.append(symbol)
            continue

        rows = upsert_row(symbol, ticker_type, df)
        total_rows += rows

        if i % args.batch_size == 0 and i < total:
            log.info(f"  Batch {i // args.batch_size} done — sleeping {SLEEP_BETWEEN}s...")
            time.sleep(SLEEP_BETWEEN)

    # Purge old data
    purge_old_data()

    # Update meta
    with get_conn() as conn:
        update_meta(conn, "market_data", total_rows)

    log.info(f"\n{'='*50}")
    log.info(f"✅ Fetch complete.")
    log.info(f"   Tickers processed:  {total - len(failed)}/{total}")
    log.info(f"   Rows upserted:      {total_rows:,}")
    if failed:
        log.warning(f"   Failed ({len(failed)}): {', '.join(failed)}")

    # Commit DB back to repo
    from git_utils import commit_file_if_changed
    commit_file_if_changed(
        filepath="db/trading.db",
        message="chore: update trading.db — daily fetch [skip ci]",
        repo_root=_REPO_ROOT,
    )


if __name__ == "__main__":
    main()
