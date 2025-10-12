import argparse
import concurrent.futures
import io
import logging
import os
import random
import sys
import time
from typing import List, Optional, Tuple, Dict, Any

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime
from retrying import retry
import gspread
from google.oauth2.service_account import Credentials
from tqdm import tqdm

try:
    from gspread_formatting import set_number_format, set_frozen
    from gspread_formatting.dataframe import format_with_dataframe
    from gspread_formatting import NumberFormat
except ImportError:
    set_number_format = set_frozen = format_with_dataframe = NumberFormat = lambda *a, **k: None  # Dummy if not installed

# ------------------- CONFIGURABLE DEFAULTS ----------------------
DEFAULT_NSE_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
DEFAULT_CREDENTIALS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"
DEFAULT_MAX_WORKERS = 10
DEFAULT_BATCH_SIZE = 100
DEFAULT_UPLOAD_MODE = "replace"
RETRY_ATTEMPTS = 3
RETRY_WAIT_MS = 3000

# ------------------- LOGGING SETUP ----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------- HELPERS ----------------------

def read_custom_ticker_list(path: str) -> List[str]:
    """
    Supports .txt (1 symbol per line) and .csv (expects SYMBOL/TICKER col).
    """
    ext = os.path.splitext(path)[-1].lower()
    if ext == ".txt":
        with open(path, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
    elif ext == ".csv":
        df = pd.read_csv(path)
        for col in ['SYMBOL', 'TICKER']:
            if col in df.columns:
                tickers = df[col].dropna().astype(str).tolist()
                break
        else:
            raise ValueError("CSV must have a SYMBOL or TICKER column.")
    else:
        raise ValueError("Ticker file must be .txt or .csv")
    logger.info(f"Loaded {len(tickers)} symbols from custom ticker file.")
    return tickers

def write_temp_csv(csv_text: str, filename: str = "temp_equity.csv") -> None:
    """Writes the NSE list to disk for reference/debugging."""
    with open(filename, "w") as f:
        f.write(csv_text)
    logger.info(f"Wrote NSE stock list to {filename}")

def exponential_backoff(attempt: int) -> float:
    """Exponential backoff time in seconds with jitter."""
    return min(30, (2 ** attempt) + random.uniform(0, 1))

# ------------------- MAIN FETCHER CLASS ----------------------

class NSEDataFetcher:
    def __init__(self,
                 nse_list_url: str = DEFAULT_NSE_LIST_URL,
                 max_workers: int = DEFAULT_MAX_WORKERS):
        """
        Args:
            nse_list_url: URL to fetch the NSE stock symbol list.
            max_workers: Concurrency for Yahoo fetches.
        """
        self.nse_list_url = nse_list_url
        self.max_workers = max_workers
        self.nse_stocks: List[str] = []
        self.stock_data: List[Dict[str, Any]] = []
        self.failed_symbols: List[str] = []
        self.unknown_yahoo_keys: set = set()

    @retry(stop_max_attempt_number=RETRY_ATTEMPTS, wait_exponential_multiplier=RETRY_WAIT_MS)
    def fetch_nse_stock_list(self) -> bool:
        """Attempts to download NSE symbols. Writes temp CSV and keeps in-memory DataFrame."""
        headers = {'User-Agent': 'Mozilla/5.0'}
        logger.info("Fetching NSE stock list from web...")
        response = requests.get(self.nse_list_url, headers=headers, timeout=10)
        if response.status_code == 200:
            csv_text = response.text
            df = pd.read_csv(io.StringIO(csv_text))
            write_temp_csv(csv_text)
            self.nse_stocks = df["SYMBOL"].dropna().astype(str).tolist()
            logger.info(f"Found {len(self.nse_stocks)} NSE stocks.")
            return True
        logger.error(f"NSE symbol list download failed: {response.status_code}")
        return False

    def load_custom_ticker_list(self, ticker_file: str) -> None:
        """Loads tickers from user-supplied txt/csv file."""
        self.nse_stocks = read_custom_ticker_list(ticker_file)

    @retry(stop_max_attempt_number=RETRY_ATTEMPTS, wait_exponential_multiplier=RETRY_WAIT_MS)
    def fetch_stock_info_yahoo(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetches all available Yahoo Finance data for a single symbol."""
        try:
            ticker = yf.Ticker(f"{symbol}.NS")
            info = ticker.info

            # Validate required fields exist
            # required_keys = self.expected_yahoo_keys()
            # missing = [k for k in required_keys if k not in info or info[k] is None]
            # if missing:
            #     logger.warning(f"Missing required fields for {symbol}: {missing}")

            # Use robust fallback for current price
            current_price = None
            try:
                hist = ticker.history(period="1d")
                if not hist.empty:
                    current_price = hist["Close"].iloc[-1]
            except Exception:
                pass
            if current_price is None:
                current_price = (
                    ticker.fast_info.get("last_price")
                    or info.get("regularMarketPrice")
                    or info.get("previousClose")
                )
            stock_data = {
                "Symbol": symbol,
                "Company_Name": info.get("longName"),
                "Sector": info.get("sector"),
                "Industry": info.get("industry"),
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
                "Last_Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            return stock_data
        except Exception as e:
            logger.error(f"Yahoo fetch failed for {symbol}: {e}", exc_info=True)
            raise

    def fetch_all_stock_data(self, use_process_pool: bool = False) -> None:
        """
        Fetches Yahoo data for all stocks in parallel. Adds failed tickers to self.failed_symbols.
        """
        self.stock_data = []
        self.failed_symbols = []
        pool_cls = concurrent.futures.ProcessPoolExecutor if use_process_pool else concurrent.futures.ThreadPoolExecutor
        logger.info(f"Fetching data for {len(self.nse_stocks)} stocks with {self.max_workers} workers ({pool_cls.__name__})...")
        with pool_cls(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self._fetch_with_backoff, s): s for s in self.nse_stocks}
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Fetching Yahoo..."):
                symbol = futures[future]
                try:
                    result = future.result()
                    if result:
                        self.stock_data.append(result)
                except Exception as e:
                    logger.error(f"Permanent failure for {symbol}: {e}", exc_info=True)
                    self.failed_symbols.append(symbol)

        logger.info(f"Done. Success: {len(self.stock_data)}, Failed: {len(self.failed_symbols)}")
        if self.unknown_yahoo_keys:
            logger.warning(f"Unknown Yahoo fields detected: {self.unknown_yahoo_keys}")

    def _fetch_with_backoff(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Wrap Yahoo fetch with randomized sleep to avoid blacklisting."""
        for attempt in range(RETRY_ATTEMPTS):
            try:
                result = self.fetch_stock_info_yahoo(symbol)
                time.sleep(random.uniform(0.2, 0.5))  # Add jitter always
                return result
            except Exception:
                backoff = exponential_backoff(attempt)
                logger.warning(f"Retrying {symbol} in {backoff:.1f}s (attempt {attempt+1}/{RETRY_ATTEMPTS})...")
                time.sleep(backoff)
        return None

    @staticmethod
    def expected_yahoo_keys() -> set:
        """All keys handled in stock_data dict (for schema drift detection)."""
        return {
            "longName", "sector", "industry", "marketCap", "previousClose", "dayHigh",
            "dayLow", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "volume", "averageVolume",
            "trailingPE", "dividendYield", "profitMargins", "operatingMargins", "ebitda",
            "regularMarketPrice"
        }

    def create_dataframe(self) -> pd.DataFrame:
        """
        Assembles cleaned DataFrame, does division/null checks, logs any failures.
        Returns:
            DataFrame with canonical column order.
        """
        df = pd.DataFrame(self.stock_data)
        # Ensure correct nulls
        df.replace({None: np.nan}, inplace=True)
        # Numeric conversions (audit for nulls)
        numeric_cols = [
            "Market_Cap", "Profit_Margins", "Operating_Margins",
            "EBITDA", "Volume", "Avg_Volume", "Current_Price"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        # Audit nulls before division
        def safe_div(a, b, colname):
            arr = (a / b) if (pd.notna(a) and pd.notna(b) and b != 0) else np.nan
            if pd.notna(a) and (pd.isna(b) or b == 0):
                logger.warning(f"Null or zero division for {colname}")
            return arr

        df["Market_cap (in Cr.)"] = df.apply(lambda r: safe_div(r["Market_Cap"], 1e7, "Market_Cap"), axis=1)
        df["Profit_Margins (%age)"] = df["Profit_Margins"] * 100
        df["Operating_Margins (%age)"] = df["Operating_Margins"] * 100
        df["EBITDA (in Cr.)"] = df.apply(lambda r: safe_div(r["EBITDA"], 1e7, "EBITDA"), axis=1)
        df["Volume (in Cr.)"] = df.apply(lambda r: safe_div(r["Volume"] * r["Current_Price"], 1e7, "Volume"), axis=1)
        df["Avg_Volume (in Cr.)"] = df.apply(lambda r: safe_div(r["Avg_Volume"] * r["Current_Price"], 1e7, "Avg_Volume"), axis=1)

        # Fill nulls as np.nan for all further math, only fill empty strings for export
        df.drop(columns=["Market_Cap", "Profit_Margins", "Operating_Margins", "EBITDA", "Volume", "Avg_Volume"], inplace=True, errors="ignore")

        # Canonical order
        col_order = [
            "Symbol", "Company_Name", "Sector", "Industry", "Market_cap (in Cr.)",
            "Current_Price", "Previous_Close", "Day_High", "Day_Low", "52_Week_High", "52_Week_Low",
            "Volume (in Cr.)", "Avg_Volume (in Cr.)", "PE_Ratio", "Dividend_Yield",
            "Profit_Margins (%age)", "Operating_Margins (%age)", "EBITDA (in Cr.)", "Last_Updated"
        ]
        df = df[[c for c in col_order if c in df.columns]]
        return df

    @retry(stop_max_attempt_number=RETRY_ATTEMPTS, wait_exponential_multiplier=RETRY_WAIT_MS)
    def setup_google_sheets(self, credentials_file: str, spreadsheet_url_or_id: str):
        """
        Returns gspread client and spreadsheet object.
        """
        scopes = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
        client = gspread.authorize(creds)
        if spreadsheet_url_or_id.startswith("https://"):
            spreadsheet_id = spreadsheet_url_or_id.split("/d/")[1].split("/")[0]
        else:
            spreadsheet_id = spreadsheet_url_or_id
        spreadsheet = client.open_by_key(spreadsheet_id)
        return client, spreadsheet

    def upload_to_sheets(self, df: pd.DataFrame, credentials_file: str, spreadsheet_url: str,
                         worksheet_name: str = "NSE_Stock_Data",
                         mode: str = "replace", batch_size: int = DEFAULT_BATCH_SIZE) -> bool:
        """
        Uploads DataFrame to Google Sheets, with append or replace mode, exponential backoff, gspread-formatting.
        """
        client, spreadsheet = self.setup_google_sheets(credentials_file, spreadsheet_url)
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=len(df.columns))
        # Prepare data
        data = [df.columns.tolist()] + df.values.tolist()
        if mode == "replace":
            worksheet.clear()
            start_row = 1
        elif mode == "append":
            start_row = len(worksheet.get_all_values()) + 1
        else:
            raise ValueError("mode must be 'replace' or 'append'")
        # Dynamic batch size + exponential backoff on quota/rate error
        i = 0
        total = len(data)
        while i < total:
            current_batch_size = min(batch_size, total - i)
            batch = data[i:i + current_batch_size]
            range_start = start_row + i
            range_end = range_start + current_batch_size - 1
            range_name = f"A{range_start}:"
            range_name += chr(65 + len(df.columns) - 1) + f"{range_end}"
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    worksheet.update(values=batch, range_name=range_name, value_input_option="USER_ENTERED")
                    logger.info(f"Uploaded rows {range_start}-{range_end} (batch {i//batch_size + 1})")
                    break
                except Exception as e:
                    if "Rate Limit" in str(e) or "429" in str(e) or "Quota exceeded" in str(e):
                        logger.warning(f"Rate limited: {e}. Retrying batch after backoff.")
                        time.sleep(exponential_backoff(attempt))
                        # Reduce batch size if persistent
                        if batch_size > 10:
                            batch_size = max(10, batch_size // 2)
                        continue
                    else:
                        logger.error(f"Upload failed: {e}", exc_info=True)
                        raise
            i += current_batch_size
        # Formatting (only if gspread-formatting is present)
        try:
            # Set number/date formatting on columns
            set_number_format(worksheet, f"E2:E{total}", NumberFormat(type="NUMBER", pattern="0.00"))
            set_number_format(worksheet, f"F2:F{total}", NumberFormat(type="NUMBER", pattern="0.00"))
            set_number_format(worksheet, f"L2:L{total}", NumberFormat(type="NUMBER", pattern="0.00"))
            set_number_format(worksheet, f"M2:M{total}", NumberFormat(type="NUMBER", pattern="0.00"))
            set_number_format(worksheet, f"S2:S{total}", NumberFormat(type="NUMBER", pattern="0.00"))
            # Date column
            set_number_format(worksheet, f"S2:S{total}", NumberFormat(type="DATE_TIME", pattern="yyyy-mm-dd hh:mm:ss"))
            set_frozen(worksheet, rows=1)
        except Exception:
            logger.warning("gspread-formatting not available or formatting failed.")
        logger.info(f"Data uploaded to https://docs.google.com/spreadsheets/d/{spreadsheet.id}")
        return True

    def save_to_csv(self, df: pd.DataFrame, filename: str = "nse_stock_data.csv") -> None:
        df.to_csv(filename, index=False)
        logger.info(f"Data saved to {filename}")

# ------------------- MAIN + CLI ----------------------
def main():
    parser = argparse.ArgumentParser(description="NSE Stock Data Fetcher with Google Sheets and CSV upload.")
    parser.add_argument("--credentials", type=str, default=DEFAULT_CREDENTIALS_PATH, help="Google service account creds JSON")
    parser.add_argument("--spreadsheet", type=str, default="143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE", help="Google Sheets URL or ID")
    parser.add_argument("--worksheet", type=str, default="NSE_Stock_Data", help="Worksheet/tab name")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="Yahoo fetch concurrency")
    parser.add_argument("--process-pool", action="store_true", help="Use ProcessPoolExecutor instead of threads")
    parser.add_argument("--ticker-file", type=str, help="TXT or CSV file with tickers (see README)")
    parser.add_argument("--skip-web", action="store_true", help="Skip NSE list web fetch, use backup or custom")
    parser.add_argument("--upload-mode", type=str, choices=["replace", "append"], default=DEFAULT_UPLOAD_MODE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size for Sheets upload")
    args = parser.parse_args()

    fetcher = NSEDataFetcher(max_workers=args.max_workers)

    try:
        # Load tickers
        if args.ticker_file:
            fetcher.load_custom_ticker_list(args.ticker_file)
        elif not args.skip_web:
            ok = fetcher.fetch_nse_stock_list()
            if not ok:
                logger.error("Web NSE list failed. Provide --ticker-file or --skip-web for backup mode.")
                sys.exit(1)
        else:
            logger.error("No tickers loaded! Provide --ticker-file or unset --skip-web.")
            sys.exit(1)

        # ✅ NEW: Warm up Yahoo session with 1 serial .info request
        logger.info("Warming up Yahoo connection with first serial request...")
        try:
            warmup_symbol = fetcher.nse_stocks[0]
            _ = fetcher.fetch_stock_info_yahoo(warmup_symbol)
            logger.info(f"Warm-up complete: {warmup_symbol}")
        except Exception as e:
            logger.warning(f"Warm-up failed but continuing: {e}")

        # ✅ Pause slightly before going parallel
        time.sleep(5.0)

        # Main Yahoo fetch
        fetcher.fetch_all_stock_data(use_process_pool=args.process_pool)
        if fetcher.failed_symbols:
            logger.warning(f"Failed tickers: {fetcher.failed_symbols}")

        # DataFrame assembly + CSV
        df = fetcher.create_dataframe()
        fetcher.save_to_csv(df)
        df.replace({np.nan: ""}, inplace=True)

        # Google Sheets
        fetcher.upload_to_sheets(
            df,
            credentials_file=args.credentials,
            spreadsheet_url=args.spreadsheet,
            worksheet_name=args.worksheet,
            mode=args.upload_mode,
            batch_size=args.batch_size
        )
        logger.info("Process completed!")

    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Exiting gracefully.")
        sys.exit(2)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
