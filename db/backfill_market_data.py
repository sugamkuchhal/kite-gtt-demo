"""
backfill_market_data.py — One-time backfill of market_data from yfinance.

Sources:
  - nse_stock_list.txt       (STOCK tickers)
  - nse_etf_list.txt         (ETF tickers)
  - PORTFOLIO > Delisted > col C  (delisted STOCK tickers, NSE:SYMBOL format)

Fetches daily OHLCV from yfinance from 2020-01-01 to today.
Upserts into db/trading.db > market_data table.

Usage:
    python3 db/backfill_market_data.py
    python3 db/backfill_market_data.py --dry-run
    python3 db/backfill_market_data.py --batch-size 5
    python3 db/backfill_market_data.py --skip-delisted
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
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
from runtime_paths import get_creds_path

# ── Config ────────────────────────────────────────────────────────────────────
START_DATE      = "2024-01-01"
DEFAULT_BATCH   = 10
SLEEP_BETWEEN   = 3.0   # seconds between batches

STOCK_LIST      = _REPO_ROOT / "nse_stock_list.txt"
ETF_LIST        = _REPO_ROOT / "nse_etf_list.txt"
PORTFOLIO_SHEET = "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s"
DELISTED_TAB    = "DELISTED"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── Ticker loading ────────────────────────────────────────────────────────────

def _load_txt(path: Path, ticker_type: str) -> list[tuple[str, str]]:
    """Returns list of (NSE:SYMBOL, type) from a .txt file."""
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
    """Reads column C from PORTFOLIO > Delisted tab."""
    log.info("Loading delisted tickers from PORTFOLIO sheet...")
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
    client = gspread.authorize(creds)
    ws     = client.open_by_key(PORTFOLIO_SHEET).worksheet(DELISTED_TAB)
    col_c  = ws.col_values(3)   # column C = index 3 (1-based)

    tickers = []
    for val in col_c[1:]:       # skip header
        s = val.strip()
        if not s:
            continue
        symbol = s.upper() if s.startswith("NSE:") else f"NSE:{s.upper()}"
        tickers.append((symbol, "STOCK"))

    log.info(f"  {len(tickers)} delisted tickers loaded.")
    return tickers


def load_all_tickers(skip_delisted: bool = False) -> list[tuple[str, str]]:
    """Merges all sources, deduplicates. Returns list of (symbol, type)."""
    tickers: dict[str, str] = {}

    for symbol, t in _load_txt(STOCK_LIST, "STOCK"):
        tickers[symbol] = t

    for symbol, t in _load_txt(ETF_LIST, "ETF"):
        tickers[symbol] = t

    if not skip_delisted:
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
    """NSE:RELIANCE → RELIANCE.NS"""
    return symbol.replace("NSE:", "") + ".NS"


def fetch_ohlcv(symbol: str) -> pd.DataFrame | None:
    """Fetches daily OHLCV from 2020-01-01 to today. Returns DataFrame or None."""
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

        # Handle MultiIndex columns from yfinance
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
        df["volume"] = (df["volume"] * df["close"]) / 1e7   # → Cr.
        df = df[["date", "close", "low", "high", "volume"]].copy()
        df = df.dropna(subset=["close"])
        return df

    except Exception as e:
        log.error(f"  Failed to fetch {symbol}: {e}")
        return None


# ── DB upsert ─────────────────────────────────────────────────────────────────

def upsert_to_db(symbol: str, ticker_type: str, df: pd.DataFrame) -> int:
    """Upserts rows for one symbol into market_data. Returns rows upserted."""
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill market_data from yfinance.")
    parser.add_argument("--dry-run",       action="store_true", help="Show tickers, skip DB writes.")
    parser.add_argument("--batch-size",    type=int, default=DEFAULT_BATCH)
    parser.add_argument("--skip-delisted", action="store_true", help="Skip Delisted tab fetch.")
    args = parser.parse_args()

    init_db()
    tickers = load_all_tickers(skip_delisted=args.skip_delisted)

    if args.dry_run:
        log.info("DRY RUN — tickers that would be fetched:")
        for symbol, t in tickers:
            print(f"  {symbol:30s} {t}")
        return

    total      = len(tickers)
    total_rows = 0
    failed     = []

    log.info(f"Starting backfill: {total} tickers from {START_DATE} to today.")
    log.info(f"Batch size: {args.batch_size}, sleep: {SLEEP_BETWEEN}s between batches.")

    for i, (symbol, ticker_type) in enumerate(tickers, 1):
        log.info(f"[{i}/{total}] {symbol} ({ticker_type})")
        df = fetch_ohlcv(symbol)

        if df is None or df.empty:
            failed.append(symbol)
            continue

        rows = upsert_to_db(symbol, ticker_type, df)
        total_rows += rows
        log.info(f"  ✅ {rows} rows upserted.")

        if i % args.batch_size == 0 and i < total:
            log.info(f"  Batch {i // args.batch_size} done — sleeping {SLEEP_BETWEEN}s...")
            time.sleep(SLEEP_BETWEEN)

    with get_conn() as conn:
        update_meta(conn, "market_data", total_rows)

    log.info(f"\n{'='*50}")
    log.info(f"✅ Backfill complete.")
    log.info(f"   Tickers processed:  {total - len(failed)}/{total}")
    log.info(f"   Total rows upserted: {total_rows:,}")
    if failed:
        log.warning(f"   Failed ({len(failed)}): {', '.join(failed)}")


if __name__ == "__main__":
    main()
