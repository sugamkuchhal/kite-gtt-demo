#!/usr/bin/env python3
"""
nse_combined_fetcher.py

Single script for NSE STOCK and ETF fetch:
- --mode stock|etf
- Stocks: default NSE list or --ticker-file
- ETFs: requires --ticker-file
- Always outputs BOTH CSV + Google Sheets
- Symbols normalized to uppercase with NSE: prefix everywhere
- Adaptive Google Sheets upload + full formatting
- failed_symbols.csv always written (empty if none)
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
from retrying import retry
import gspread
from google.oauth2.service_account import Credentials
from tqdm import tqdm

# gspread-formatting
try:
    from gspread_formatting import set_number_format, set_frozen
    from gspread_formatting import NumberFormat
except Exception:
    set_number_format = set_frozen = lambda *a, **k: None
    NumberFormat = lambda *a, **k: None

# ===== Editable defaults (set these once, or override via CLI) =====
DEFAULT_CREDENTIALS_FILE = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"
DEFAULT_SPREADSHEET = "https://docs.google.com/spreadsheets/d/143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE"
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
        Single symbol fetch from Yahoo. Returns a dict (Symbol kept as NSE:...).
        Retries handled by caller wrapper with backoff.
        """
        raw_for_yahoo = self._symbol_for_yahoo(symbol)
        try:
            ticker = yf.Ticker(raw_for_yahoo)
            info = {}
            # .info can be flaky — guard it
            try:
                info = ticker.info or {}
            except Exception:
                info = {}

            # robust current price: try history then fast_info / info
            current_price = None
            try:
                hist = ticker.history(period="1d")
                if not hist.empty:
                    current_price = hist["Close"].iloc[-1]
            except Exception:
                current_price = None

            if current_price is None:
                current_price = (
                    getattr(ticker, "fast_info", {}) and ticker.fast_info.get("last_price")
                ) or info.get("regularMarketPrice") or info.get("previousClose")

            result = {
                "Symbol": symbol,  # keep NSE: prefix here everywhere
                "Company_Name": info.get("longName") if info.get("longName") is not None else "",
                "Sector": info.get("sector") if info.get("sector") is not None else "",
                "Industry": info.get("industry") if info.get("industry") is not None else "",
                "Market_Cap": info.get("marketCap"),
                "Current_Price": current_price,
                "Previous_Close": info.get("previousClose"),
                "Day_High": info.get("dayHigh"),
                "Day_Low": info.get("dayLow"),
                "52_Week_High": info.get("fiftyTwoWeekHigh"),
                "52_Week_Low": info.get("fiftyTwoWeekLow"),
                "Volume": info.get("volume"),
                "Avg_Volume": info.get("averageVolume"),
                "PE_Ratio": info.get("trailingPE"),
                "Dividend_Yield": info.get("dividendYield"),
                "Profit_Margins": info.get("profitMargins"),
                "Operating_Margins": info.get("operatingMargins"),
                "EBITDA": info.get("ebitda"),
                "Last_Updated": datetime.now().strftime("%Y-%m-%d")
            }
            return result
        except Exception as e:
            # bubble up to wrapper for retry/backoff
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

    def create_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.stock_data)
        if df.empty:
            return df
        # normalize Nones -> np.nan for numeric conversions
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

        # Ensure Last_Updated is date format string DD-MMM-YYYY for sheet formatting consistency
        if "Last_Updated" in df.columns:
            try:
                # current stored format is YYYY-MM-DD; convert to DD-MMM-YYYY (string form)
                df["Last_Updated"] = pd.to_datetime(df["Last_Updated"], errors="coerce").dt.strftime("%d-%b-%Y")
            except Exception:
                pass

        # fill NaNs with empty strings for CSV/Sheets (we'll apply numeric formatting in Sheets)
        df = df.fillna("")
        return df

    # ----- Google Sheets helpers -----
    def setup_google_sheets(self, credentials_file: str, spreadsheet_url_or_id: str):
        scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
        client = gspread.authorize(creds)
        if spreadsheet_url_or_id.startswith("http"):
            spreadsheet_id = spreadsheet_url_or_id.split("/d/")[1].split("/")[0]
        else:
            spreadsheet_id = spreadsheet_url_or_id
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
                         spreadsheet_url_or_id: str,
                         worksheet_name: str,
                         initial_batch_size: int = 200):
        """
        Adaptive batch upload with exponential backoff on rate-limit errors.
        Also applies full formatting: 2-decimals for numeric columns, date format for Last_Updated,
        and freezes header row.
        """
        client, spreadsheet = self.setup_google_sheets(credentials_file, spreadsheet_url_or_id)
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

        # Apply number formatting where appropriate.
        # We will map names to formatting rules per the spec.
        # Strings: Symbol, Company_Name, Sector, Industry -> plain text (no formatting)
        # Integers: (none mandatory in current DF)
        # Numbers 2 decimals: Market_cap (in Cr.), Current_Price, Previous_Close, Day_High, Day_Low,
        # 52_Week_High, 52_Week_Low, Volume (in Cr.), Avg_Volume (in Cr.), PE_Ratio, Dividend_Yield,
        # Profit_Margins (%age), Operating_Margins (%age), EBITDA (in Cr.)
        # Date: Last_Updated -> DD-MMM-YYYY

        # build a col_name -> column_letter map
        col_map = {col: self._col_letter(i) for i, col in enumerate(df.columns)}
        number_cols = [
            "Market_cap (in Cr.)", "Current_Price", "Previous_Close", "Day_High", "Day_Low",
            "52_Week_High", "52_Week_Low", "Volume (in Cr.)", "Avg_Volume (in Cr.)",
            "PE_Ratio", "Dividend_Yield", "Profit_Margins (%age)", "Operating_Margins (%age)", "EBITDA (in Cr.)"
        ]
        # Apply 2-decimal number format for present columns
        for col in number_cols:
            if col in col_map:
                rng = f"{col_map[col]}2:{col_map[col]}{len(df) + 1}"  # +1 because header occupies row1
                try:
                    set_number_format(worksheet, rng, NumberFormat(type="NUMBER", pattern="0.00"))
                except Exception:
                    logger.debug(f"Could not set number format for {col} ({rng})")

        # Date formatting for Last_Updated column
        if "Last_Updated" in col_map:
            rng = f"{col_map['Last_Updated']}2:{col_map['Last_Updated']}{len(df) + 1}"
            try:
                # pattern to show as DD-MMM-YYYY
                set_number_format(worksheet, rng, NumberFormat(type="DATE", pattern="dd-mmm-yyyy"))
            except Exception:
                logger.debug("Could not set date format for Last_Updated")

        logger.info(f"Upload to Google Sheets complete: https://docs.google.com/spreadsheets/d/{spreadsheet.id}")

        return True

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        df.to_csv(filename, index=False)
        logger.info(f"Saved CSV: {filename}")

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
def write_failed_symbols_file(failed: List[str], filename: str = "failed_symbols.csv"):
    # Overwrite single file each run. If empty, create empty file.
    if not failed:
        # create/overwrite empty file
        open(filename, "w", encoding="utf-8").close()
        logger.info(f"Failed symbols file created (empty): {filename}")
        return
    # else write one per line
    with open(filename, "w", encoding="utf-8") as f:
        for s in failed:
            f.write(f"{s}\n")
    logger.info(f"Wrote {len(failed)} failed symbols to {filename}")

def human_summary(mode: str, success_count: int, failed: List[str]):
    print()  # blank line
    print(f"✅ {mode.upper()} run completed")
    print(f"   Success: {success_count} tickers")
    print(f"   Failed: {len(failed)} tickers", end="")
    if len(failed) == 0:
        print()
        print("   Failed list saved to failed_symbols.csv (empty)")
    elif len(failed) <= 10:
        print(" → " + ", ".join(failed))
        print("   Failed list saved to failed_symbols.csv")
    else:
        print()
        print("   (Full list saved to failed_symbols.csv)")

def main():
    parser = argparse.ArgumentParser(description="Combined NSE Stock & ETF Data Fetcher")
    parser.add_argument("--mode", choices=["stock", "etf"], required=True, help="Run mode: stock or etf")
    parser.add_argument("--ticker-file", type=str, help="Custom ticker file (TXT or CSV). Required for ETF mode.")
    parser.add_argument("--worksheet", type=str, required=True, help="Worksheet/tab name to write (required)")
    parser.add_argument("--csv", type=str, required=True, help="CSV output filename (required). Example: /path/out.csv")
    parser.add_argument("--max-workers", type=int, default=10, help="Thread pool size for Yahoo fetches")
    parser.add_argument("--batch-size", type=int, default=200, help="Initial batch size for Sheets upload (adaptive)")
    args = parser.parse_args()

    mode = args.mode.lower()
    spreadsheet_to_use = DEFAULT_SPREADSHEET

    if mode == "stock":
        fetcher = NSEStockDataFetcher(max_workers=args.max_workers)
        try:
            fetcher.load_symbols(args.ticker_file)
        except Exception as e:
            logger.error(f"Failed to load symbols for stock mode: {e}")
            sys.exit(1)
        worksheet = args.worksheet
        csv_file = args.csv
    else:
        fetcher = NSEETFDataFetcher(max_workers=args.max_workers)
        try:
            fetcher.load_symbols(args.ticker_file)
        except Exception as e:
            logger.error(f"Failed to load symbols for etf mode: {e}")
            sys.exit(1)
        worksheet = args.worksheet
        csv_file = args.csv

    # fetch
    fetcher.fetch_all()

    # create dataframe
    df = fetcher.create_dataframe()

    # save CSV (always)
    try:
        fetcher.save_to_csv(df, csv_file)
    except Exception as e:
        logger.error(f"CSV save failed: {e}")

    # write failed_symbols.csv (overwrite single file)
    # Ensure failed list uses normalized NSE: prefix (they are already normalized)
    failed_symbols = fetcher.failed_symbols or []
    write_failed_symbols_file(failed_symbols, filename="failed_symbols.csv")

    # upload to google sheets (always)
    try:
        fetcher.upload_to_sheets(df, DEFAULT_CREDENTIALS_FILE, spreadsheet_to_use, worksheet, initial_batch_size=args.batch_size)
    except Exception as e:
        logger.error(f"Google Sheets upload failed: {e}")
        # still continue to summary

    # final human-readable summary
    success_count = len(fetcher.stock_data)
    human_summary(mode, success_count, failed_symbols)

if __name__ == "__main__":
    main()
