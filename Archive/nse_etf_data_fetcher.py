import pandas as pd
import yfinance as yf
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor, as_completed
from retrying import retry
from tqdm import tqdm
import logging
import warnings
import time
from gspread.utils import rowcol_to_a1

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NSEETFDataFetcher:
    def __init__(self):
        self.etf_symbols = []
        self.stock_data = []

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def get_stock_info_yahoo(self, symbol):
        suffix = ".BO" if symbol.isdigit() else ".NS"
        ticker = yf.Ticker(f"{symbol}{suffix}")
        info = ticker.info

        # Get current price using history, fallback to fast_info or previousClose
        try:
            hist = ticker.history(period="1d")
            current_price = hist["Close"].iloc[-1] if not hist.empty else None
        except Exception:
            current_price = None

        if current_price is None:
            current_price = (
                ticker.fast_info.get("last_price")
                or info.get("regularMarketPrice")
                or info.get("previousClose")
                or "N/A"
            )

        stock_data = {
            'Symbol': symbol,
            'Company_Name': info.get('longName', 'N/A'),
            'Sector': info.get('sector', 'N/A'),
            'Industry': info.get('industry', 'N/A'),
            'Market_Cap': info.get('marketCap', 'N/A'),
            'Current_Price': current_price,
            'Previous_Close': info.get('previousClose', 'N/A'),
            'Day_High': info.get('dayHigh', 'N/A'),
            'Day_Low': info.get('dayLow', 'N/A'),
            '52_Week_High': info.get('fiftyTwoWeekHigh', 'N/A'),
            '52_Week_Low': info.get('fiftyTwoWeekLow', 'N/A'),
            'Volume': info.get('volume', 'N/A'),
            'Avg_Volume': info.get('averageVolume', 'N/A'),
            'PE_Ratio': info.get('trailingPE', 'N/A'),
            'Dividend_Yield': info.get('dividendYield', 'N/A'),
            'Profit_Margins': info.get('profitMargins', 'N/A'),
            'Operating_Margins': info.get('operatingMargins', 'N/A'),
            'EBITDA': info.get('ebitda', 'N/A'),
            'Last_Updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return stock_data

    def fetch_all_stock_data(self, max_workers=5):
        logging.info(f"Fetching data for {len(self.etf_symbols)} ETFs...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self.get_stock_info_yahoo, symbol): symbol
                for symbol in self.etf_symbols
            }

            for future in tqdm(as_completed(future_to_symbol), total=len(future_to_symbol)):
                symbol = future_to_symbol[future]
                try:
                    stock_data = future.result()
                    self.stock_data.append(stock_data)
                except Exception as e:
                    logging.error(f"Error processing {symbol}: {e}")
                time.sleep(0.1)

    def create_dataframe(self):
        df = pd.DataFrame(self.stock_data)
        df = df.replace("N/A", pd.NA)

        numeric_cols = ["Market_Cap", "Profit_Margins", "Operating_Margins", "EBITDA", "Volume", "Avg_Volume", "Current_Price"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["Market_cap (in Cr.)"] = df["Market_Cap"] / 1e7
        df["Profit_Margins (%age)"] = df["Profit_Margins"] * 100
        df["Operating_Margins (%age)"] = df["Operating_Margins"] * 100
        df["EBITDA (in Cr.)"] = df["EBITDA"] / 1e7
        df["Volume (in Cr.)"] = (df["Volume"] * df["Current_Price"]) / 1e7
        df["Avg_Volume (in Cr.)"] = (df["Avg_Volume"] * df["Current_Price"]) / 1e7

        df.drop(columns=["Market_Cap", "Profit_Margins", "Operating_Margins", "EBITDA", "Volume", "Avg_Volume"], inplace=True, errors="ignore")
        df = df.fillna("")

        column_order = [
            'Symbol', 'Company_Name', 'Sector', 'Industry',
            'Market_cap (in Cr.)', 'Current_Price', 'Previous_Close',
            'Day_High', 'Day_Low', '52_Week_High', '52_Week_Low',
            'Volume (in Cr.)', 'Avg_Volume (in Cr.)',
            'PE_Ratio', 'Dividend_Yield',
            'Profit_Margins (%age)', 'Operating_Margins (%age)',
            'EBITDA (in Cr.)', 'Last_Updated'
        ]
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]

        return df

    def setup_google_sheets(self, credentials_file, spreadsheet_url_or_id):
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
            client = gspread.authorize(creds)
            spreadsheet_id = spreadsheet_url_or_id.split("/d/")[1].split("/")[0] if spreadsheet_url_or_id.startswith("https://") else spreadsheet_url_or_id
            spreadsheet = client.open_by_key(spreadsheet_id)
            return client, spreadsheet
        except Exception as e:
            logging.error(f"Google Sheets connection error: {e}")
            return None, None

    def upload_to_sheets(self, df, credentials_file, spreadsheet_url, worksheet_name="NSE_ETF_Data"):
        try:
            client, spreadsheet = self.setup_google_sheets(credentials_file, spreadsheet_url)
            if client is None:
                return False

            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                worksheet.clear()
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=len(df)+1, cols=len(df.columns))

            data = [df.columns.tolist()] + df.values.tolist()
            batch_size = 100
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                start_row = i + 1
                end_row = start_row + len(batch) - 1

                last_col = rowcol_to_a1(1, len(df.columns)).split('1')[0]
                range_name = f"A{start_row}:{last_col}{end_row}"
                worksheet.update(range_name, batch)
                time.sleep(1)

            logging.info(f"Uploaded {len(df)} rows to Google Sheets tab: {worksheet_name}")
            return True
        except Exception as e:
            logging.error(f"Upload error: {e}")
            return False

    def save_to_csv(self, df, filename="nse_etf_data.csv"):
        df.to_csv(filename, index=False)
        logging.info(f"Data saved to {filename}")

def main():
    fetcher = NSEETFDataFetcher()
    fetcher.etf_symbols = [
        "GILT5YBEES", "LIQUIDCASE", "ABSLPSE", "ALPHAETF", "ALPL30IETF",
        "AUTOIETF", "BFSI", "COMMOIETF", "CONSUMBEES",
        "CPSEETF", "DIVOPPBEES", "EVINDIA", "FMCGIETF", "HNGSNGBEES",
        "INFRAIETF", "ITBEES", "MAFANG", "MAHKTECH", "MAKEINDIA",
        "MASPTOP50", "METALIETF", "MIDCAPETF", "MNC",
        "MODEFENCE", "MOMENTUM50", "MOMOMENTUM", "MON100", "MONIFTY500",
        "MONQ50", "MOREALTY", "MOVALUE", "MULTICAP", "NEXT50IETF",
        "NIFTYBEES", "NIFTYQLITY", "OILIETF", "PHARMABEES", "PSUBNKBEES",
        "PVTBANIETF", "SHARIABEES", "TNIDETF", "TOP100CASE", "TOP10ADD",
        "GOLDBEES", "SILVERBEES"
    ]

    fetcher.fetch_all_stock_data(max_workers=3)
    df = fetcher.create_dataframe()
    fetcher.save_to_csv(df)

    credentials_file = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"
    spreadsheet_url = "https://docs.google.com/spreadsheets/d/143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE"
    worksheet_name = "NSE_ETF_Data"

    fetcher.upload_to_sheets(df, credentials_file, spreadsheet_url, worksheet_name)
    logging.info("ETF data fetch complete.")

if __name__ == "__main__":
    main()
