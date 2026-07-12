#!/usr/bin/env python3
import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("nse_combined_fetcher")
atexit.register(log_end, _RUN_CTX)
"""
nse_combined_fetcher.py

Single script for NSE STOCK and ETF fetch:
- --mode stock|etf
- Stocks: default NSE list or --ticker-file
- ETFs: requires --ticker-file
- Uploads to Google Sheets
- Symbols normalized to uppercase with NSE: prefix everywhere
- Adaptive Google Sheets upload + full formatting
"""

import argparse
import concurrent.futures
import io
import logging
import os
import random
import sys
import time
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
pd.set_option("future.no_silent_downcasting", True)

import requests
import yfinance as yf
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from tqdm import tqdm

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

# gspread-formatting
try:
    from gspread_formatting import set_number_format, set_frozen
    from gspread_formatting import NumberFormat
except Exception:
    set_number_format = set_frozen = lambda *a, **k: None
    NumberFormat = lambda *a, **k: None

# ===== Editable defaults (set these once, or override via CLI) =====
DEFAULT_CREDENTIALS_FILE = str(get_creds_path())
DEFAULT_REF_SHEETS = "TICKER"
# ==================================================================

# ----------------- Logging -----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ----------------- Helpers -----------------
def read_custom_ticker_list(path: str) -> List[str]:
    """
    Load tickers from txt (one per line) or csv (SYMBOL or TICKER column).
    Normalize to uppercase and idempotently prefix "NSE:".
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Ticker file not found: {path}")
    ext = os.path.splitext(path)[1].lower()
    raw_symbols = []
    if ext == ".txt":
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s:
                    raw_symbols.append(s)
    elif ext == ".csv":
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        # prefer SYMBOL then TICKER
        if "SYMBOL" in df.columns:
            raw_symbols = df["SYMBOL"].astype(str).tolist()
        elif "TICKER" in df.columns:
            raw_symbols = df["TICKER"].astype(str).tolist()
        else:
            # fallback: use first column
            raw_symbols = df.iloc[:, 0].astype(str).tolist()
    else:
        raise ValueError("Ticker file must be .txt or .csv")

    # normalize: uppercase, strip, idempotent NSE: prefix
    norm = []
    for s in raw_symbols:
        s2 = s.strip().upper()
        if not s2:
            continue
        if s2.startswith("NSE:"):
            sym = s2
        else:
            sym = "NSE:" + s2
        norm.append(sym)
    logger.info(f"Loaded {len(norm)} symbols from {path}")
    return norm

def exponential_backoff(attempt: int) -> float:
    """Backoff seconds with jitter (cap at 30s)."""
    return min(30, (2 ** attempt) + random.uniform(0, 1))

# ----------------- Fetcher Base -----------------
class NSEBaseFetcher:
    def __init__(self, symbols: Optional[List[str]] = None, max_workers: int = 10):
        self.symbols = symbols or []
        self.max_workers = max_workers
        self.stock_data: List[Dict[str, Any]] = []
        self.failed_symbols: List[str] = []

    def _symbol_for_yahoo(self, symbol: str) -> str:
        # symbol is like "NSE:RELIANCE" -> return "RELIANCE.NS"
        bare = symbol.upper().replace("NSE:", "")
        return f"{bare}.NS"

    def fetch_stock_info_yahoo(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch only the 6 needed fields: Symbol, Company Name, Sector,
        Industry, Current Price, Market Cap. One .info call, no .history.
        """
        raw_for_yahoo = self._symbol_for_yahoo(symbol)
        try:
            info = yf.Ticker(raw_for_yahoo).info or {}

            current_price = (
                info.get("currentPrice")
                or info.get("regularMarketPrice")
                or info.get("previousClose")
            )

            if current_price is None and not info.get("longName") and not info.get("shortName"):
                raise RuntimeError(
                    f"unusable Yahoo response for {symbol}: no price, no name "
                    f"(info keys: {list(info.keys())[:5]})"
                )

            return {
                "Symbol":            symbol,
                "Company_Name":      info.get("longName") or info.get("shortName") or "",
                "Sector":            info.get("sector")   or "",
                "Industry":          info.get("industry") or "",
                "Market_Cap":        info.get("marketCap"),
                "Current_Price":     current_price,
                "Previous_Close":    None,                    # blanked
                "Day_High":          info.get("dayHigh"),
                "Day_Low":           info.get("dayLow"),
                "52_Week_High":      None,                    # blanked
                "52_Week_Low":       None,                    # blanked
                "Volume":            info.get("volume"),
                "Avg_Volume":        None,                    # blanked
                "PE_Ratio":          None,                    # blanked
                "Dividend_Yield":    None,                    # blanked
                "Profit_Margins":    None,                    # blanked
                "Operating_Margins": None,                    # blanked
                "EBITDA":            None,                    # blanked
                "Last_Updated":      datetime.now().strftime("%Y-%m-%d"),
            }
        except Exception:
            raise

    def _fetch_with_backoff(self, symbol: str) -> Optional[Dict[str, Any]]:
        attempts = 3
        for attempt in range(attempts):
            try:
                res = self.fetch_stock_info_yahoo(symbol)
                time.sleep(random.uniform(0.15, 0.4))  # small jitter
                return res
            except Exception as exc:
                backoff = exponential_backoff(attempt)
                logger.warning(f"Fetch error for {symbol} (attempt {attempt+1}/{attempts}): {exc}. Retrying in {backoff:.1f}s")
                time.sleep(backoff)
        logger.error(f"Permanent failure for {symbol} after {attempts} attempts")
        return None

    def fetch_all(self) -> None:
        self.stock_data = []
        self.failed_symbols = []
        total = len(self.symbols)
        if total == 0:
            logger.warning("No symbols to fetch.")
            return
        logger.info(f"Fetching {total} symbols with {self.max_workers} workers...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._fetch_with_backoff, s): s for s in self.symbols}
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Fetching Yahoo..."):
                sym = futures[future]
                try:
                    r = future.result()
                    if r:
                        self.stock_data.append(r)
                    else:
                        self.failed_symbols.append(sym)
                except Exception as e:
                    logger.error(f"Unexpected error for {sym}: {e}")
                    self.failed_symbols.append(sym)
        logger.info(f"Fetch complete. Success: {len(self.stock_data)}, Failed: {len(self.failed_symbols)}")

        # Second pass: failed symbols are usually rate-limit victims, not
        # bad tickers. After a cooldown, retry them once, single-threaded
        # and slower, so Yahoo's throttle has relaxed.
        if self.failed_symbols:
            cooldown = 60
            logger.info(f"Second pass: retrying {len(self.failed_symbols)} failed "
                        f"symbol(s) after {cooldown}s cooldown (single-threaded)...")
            time.sleep(cooldown)
            still_failed = []
            for sym in tqdm(self.failed_symbols, desc="Second pass..."):
                try:
                    r = self._fetch_with_backoff(sym)
                except Exception as e:
                    logger.error(f"Second-pass unexpected error for {sym}: {e}")
                    r = None
                if r:
                    self.stock_data.append(r)
                else:
                    still_failed.append(sym)
                time.sleep(random.uniform(0.5, 1.0))
            self.failed_symbols = still_failed
            logger.info(f"Second pass complete. Total success: {len(self.stock_data)}, "
                        f"still failed: {len(self.failed_symbols)}")
            if self.failed_symbols:
                logger.warning(f"Still-failed symbols: {', '.join(self.failed_symbols[:50])}"
                               f"{' ...' if len(self.failed_symbols) > 50 else ''}")

    def create_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.stock_data)
        if df.empty:
            return df
        df = df.replace({None: np.nan}, inplace=False)
        numeric_cols = ["Market_Cap", "Profit_Margins", "Operating_Margins", "EBITDA", "Volume", "Avg_Volume", "Current_Price"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        # Derived metrics
        if "Market_Cap" in df.columns:
            df["Market_cap (in Cr.)"] = df["Market_Cap"] / 1e7
        if "Profit_Margins" in df.columns:
            df["Profit_Margins (%age)"] = df["Profit_Margins"] * 100
        if "Operating_Margins" in df.columns:
            df["Operating_Margins (%age)"] = df["Operating_Margins"] * 100
        if "EBITDA" in df.columns:
            df["EBITDA (in Cr.)"] = df["EBITDA"] / 1e7
        if "Volume" in df.columns and "Current_Price" in df.columns:
            df["Volume (in Cr.)"] = (df["Volume"] * df["Current_Price"]) / 1e7
        if "Avg_Volume" in df.columns and "Current_Price" in df.columns:
            df["Avg_Volume (in Cr.)"] = (df["Avg_Volume"] * df["Current_Price"]) / 1e7
        # drop raw numeric intermediate cols
        df.drop(columns=[c for c in ["Market_Cap", "Profit_Margins", "Operating_Margins", "EBITDA", "Volume", "Avg_Volume"] if c in df.columns], inplace=True, errors="ignore")
        # Last_Updated as DD-MMM-YYYY string
        if "Last_Updated" in df.columns:
            try:
                df["Last_Updated"] = pd.to_datetime(df["Last_Updated"], errors="coerce").dt.strftime("%d-%b-%Y")
            except Exception:
                pass
        df = df.fillna("")
        return df

    # ----- Google Sheets helpers -----
    def setup_google_sheets(self, credentials_file: str, ref_sheets: str):
        scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet_id = resolve_sheet_id(ref_sheets)
        spreadsheet = client.open_by_key(spreadsheet_id)
        return client, spreadsheet

    def _col_letter(self, idx: int) -> str:
        """0-based idx to Excel column letter(s)."""
        # idx: 0 -> A
        letters = ""
        i = idx + 1
        while i > 0:
            i, remainder = divmod(i - 1, 26)
            letters = chr(65 + remainder) + letters
        return letters

    def upload_to_sheets(self,
                         df: pd.DataFrame,
                         credentials_file: str,
                         ref_sheets: str,
                         worksheet_name: str,
                         initial_batch_size: int = 200):
        """
        Adaptive batch upload with exponential backoff on rate-limit errors.
        Also applies full formatting: 2-decimals for numeric columns, date format for Last_Updated,
        and freezes header row.
        """
        client, spreadsheet = self.setup_google_sheets(credentials_file, ref_sheets)
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=max(1000, len(df) + 10), cols=max(10, len(df.columns)))

        # Prepare data (include header)
        data = [df.columns.tolist()] + df.values.tolist()
        total_rows = len(data)
        batch_size = initial_batch_size if initial_batch_size > 0 else 100
        i = 0

        # Upload loop with adaptive batch
        while i < total_rows:
            current_batch = data[i:i + batch_size]
            start_row = 1 + i
            end_row = start_row + len(current_batch) - 1
            # compute rightmost column letter based on df.columns length
            last_col_letter = self._col_letter(len(df.columns) - 1)
            range_name = f"A{start_row}:{last_col_letter}{end_row}"
            # Try uploading this batch with retries and batch shrink on rate-limit
            max_attempts = 5
            attempt = 0
            while attempt < max_attempts:
                try:
                    worksheet.update(values=current_batch, range_name=range_name, value_input_option="USER_ENTERED")
                    break
                except Exception as e:
                    txt = str(e).lower()
                    # detect rate limit/quota-ish errors
                    rate_limited = any(tok in txt for tok in ("rate limit", "quota", "429", "too many requests", "exceeded"))
                    attempt += 1
                    if rate_limited:
                        wait = exponential_backoff(attempt)
                        logger.warning(f"Rate limited or quota error during upload: {e}. Backing off {wait:.1f}s and retrying. Reducing batch size.")
                        time.sleep(wait)
                        # shrink batch to be more conservative
                        new_batch = max(10, batch_size // 2)
                        if new_batch < batch_size:
                            batch_size = new_batch
                        # reattempt
                        continue
                    else:
                        # non-rate error: retry with short backoff up to max_attempts, then raise
                        wait = exponential_backoff(attempt)
                        logger.warning(f"Upload error (attempt {attempt}/{max_attempts}): {e}. Retrying in {wait:.1f}s")
                        time.sleep(wait)
                        continue
            else:
                # if we exhausted attempts
                raise RuntimeError(f"Failed to upload batch starting at row {start_row} after {max_attempts} attempts.")
            # success for this batch
            i += batch_size
            # small jitter to avoid hammering
            time.sleep(0.25)

        # Formatting: determine columns by name and apply formats
        try:
            # freeze header row
            set_frozen(worksheet, rows=1)
        except Exception:
            logger.warning("Failed to set frozen header (gspread-formatting may be unavailable).")

        # Format numeric columns: Current_Price and Market_cap (in Cr.) to 2 decimals
        col_map = {col: self._col_letter(i) for i, col in enumerate(df.columns)}
        for col in ["Current_Price", "Market_cap (in Cr.)"]:
            if col in col_map:
                rng = f"{col_map[col]}2:{col_map[col]}{len(df) + 1}"
                try:
                    set_number_format(worksheet, rng, NumberFormat(type="NUMBER", pattern="0.00"))
                except Exception:
                    logger.debug(f"Could not set number format for {col}")

        logger.info(f"Upload to Google Sheets complete: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")

        return True

# ----------------- Stock/ETF Fetchers -----------------
class NSEStockDataFetcher(NSEBaseFetcher):
    DEFAULT_NSE_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

    def load_symbols(self, ticker_file: Optional[str] = None) -> None:
        if ticker_file:
            self.symbols = read_custom_ticker_list(ticker_file)
            return
        # fetch NSE list
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            resp = requests.get(self.DEFAULT_NSE_LIST_URL, headers=headers, timeout=15)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            if "SYMBOL" in df.columns:
                raw = df["SYMBOL"].dropna().astype(str).tolist()
            else:
                raw = df.iloc[:, 0].dropna().astype(str).tolist()
            # normalize
            symbols = []
            for s in raw:
                s2 = s.strip().upper()
                if not s2:
                    continue
                if s2.startswith("NSE:"):
                    symbols.append(s2)
                else:
                    symbols.append("NSE:" + s2)
            self.symbols = symbols
            logger.info(f"Loaded {len(self.symbols)} symbols from NSE list")
        except Exception as e:
            logger.error(f"Failed to download NSE list: {e}")
            raise

class NSEETFDataFetcher(NSEBaseFetcher):
    def load_symbols(self, ticker_file: Optional[str] = None) -> None:
        # ETFs MUST use custom ticker file per spec
        if not ticker_file:
            raise ValueError("ETF mode requires --ticker-file pointing to a txt or csv with tickers.")
        self.symbols = read_custom_ticker_list(ticker_file)

# ----------------- CLI / Main -----------------

def human_summary(mode: str, success_count: int, failed: List[str]):
    print()  # blank line
    print(f"✅ {mode.upper()} run completed")
    print(f"   Success: {success_count} tickers")
    print(f"   Failed: {len(failed)} tickers", end="")
    if len(failed) == 0:
        print()
    elif len(failed) <= 10:
        print(" → " + ", ".join(failed))
    else:
        print()

def main():
    parser = argparse.ArgumentParser(description="Combined NSE Stock & ETF Data Fetcher")
    parser.add_argument("--mode", choices=["stock", "etf"], required=True, help="Run mode: stock or etf")
    parser.add_argument("--ref-sheets", type=str, default=DEFAULT_REF_SHEETS, help="Resolver key from ref_sheets.json")
    parser.add_argument("--ticker-file", type=str, help="Custom ticker file (TXT or CSV). Required for ETF mode.")
    parser.add_argument("--worksheet", type=str, required=True, help="Worksheet/tab name to write (required)")
    parser.add_argument("--max-workers", type=int, default=10, help="Thread pool size for Yahoo fetches")
    parser.add_argument("--batch-size", type=int, default=200, help="Initial batch size for Sheets upload (adaptive)")
    args = parser.parse_args()

    mode = args.mode.lower()
    ref_sheets = args.ref_sheets

    if mode == "stock":
        fetcher = NSEStockDataFetcher(max_workers=args.max_workers)
        try:
            fetcher.load_symbols(args.ticker_file)
        except Exception as e:
            logger.error(f"Failed to load symbols for stock mode: {e}")
            sys.exit(1)
        worksheet = args.worksheet

    else:
        fetcher = NSEETFDataFetcher(max_workers=args.max_workers)
        try:
            fetcher.load_symbols(args.ticker_file)
        except Exception as e:
            logger.error(f"Failed to load symbols for etf mode: {e}")
            sys.exit(1)
        worksheet = args.worksheet

    # fetch
    fetcher.fetch_all()

    # create dataframe
    df = fetcher.create_dataframe()

    # Ensure failed list uses normalized NSE: prefix (they are already normalized)

    # upload to google sheets (always)
    try:
        fetcher.upload_to_sheets(df, DEFAULT_CREDENTIALS_FILE, ref_sheets, worksheet, initial_batch_size=args.batch_size)
    except Exception as e:
        logger.error(f"Google Sheets upload failed: {e}")
        # still continue to summary

    # final human-readable summary
    success_count = len(fetcher.stock_data)
    human_summary(mode, success_count, fetcher.failed_symbols)
    time.sleep(60)

if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        raise SystemExit(130)
    except Exception:
        logger.exception("nse_combined_fetcher failed.")
        raise SystemExit(1)
