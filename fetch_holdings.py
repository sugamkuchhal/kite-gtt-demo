# fetch_holdings.py

import logging
from kite_session import get_kite

import gspread
from google.oauth2.service_account import Credentials

CREDS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"
SHEET_NAME = "SARAS Portfolio - Stocks"
TAB_NAME = "ZERODHA_PORTFOLIO"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def get_gsheet_client():
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)
    return gspread.authorize(creds)

def fetch_holdings():
    kite = get_kite()
    try:
        holdings = kite.holdings()
        logging.info(f"Fetched {len(holdings)} holdings from Zerodha.")
        return holdings
    except Exception as e:
        logging.error(f"❌ Failed to fetch holdings: {e}")
        return []

def write_to_gsheet(holdings):
    if not holdings:
        logging.warning("No holdings to write to Google Sheet.")
        return

    # Prepare data for Google Sheet
    headers = ["Tradingsymbol", "ISIN", "Quantity", "Used Quantity", "T1 Quantity", "Average Price", "Last Price", "P&L", "Product", "Exchange"]
    data = [headers]
    for h in holdings:
        row = [
            h.get("tradingsymbol"),
            h.get("isin"),
            h.get("quantity"),
            h.get("used_quantity"),
            h.get("t1_quantity"),
            h.get("average_price"),
            h.get("last_price"),
            h.get("pnl"),
            h.get("product"),
            h.get("exchange"),
        ]
        data.append(row)

    # Connect to Google Sheet
    gc = get_gsheet_client()
    sh = gc.open(SHEET_NAME)
    ws = sh.worksheet(TAB_NAME)

    # Clear existing content and update with new data
    ws.clear()
    ws.update(values=data, range_name='A1')
    logging.info(f"✅ Holdings written to {SHEET_NAME} [{TAB_NAME}]")

def check_portfolio_discrepancy():
    SHEET_NAME = "SARAS Portfolio - Stocks"
    TAB_NAME = "Portfolio"
    CELL = "U1"
    CELL_CHECK = "V1"
    gc = get_gsheet_client()
    sh = gc.open(SHEET_NAME)
    ws = sh.worksheet(TAB_NAME)
    try:
        cell_value = ws.acell(CELL).value
        cell_check_value = ws.acell(CELL_CHECK).value
        if str(cell_value).strip() == "0":
            logging.info("✅ All Good. Portfolio Matched Completely.")
        else:
            if str(cell_check_value).strip() == "0":
                logging.warning(f"❌ Discrepancy Found! {cell_value} Tickers are not in sync & {cell_check_value} Tickers were bought")
            if str(cell_check_value).strip() == str(cell_value).strip():
                logging.warning(f"✅ All Good. {cell_value} Tickers are not in sync & {cell_check_value} Tickers were bought")              
            else:
                logging.warning(f"❌ Discrepancy Found! {cell_value} Tickers are not in sync & {cell_check_value} Tickers were bought")              
    except Exception as e:
        logging.error(f"❌ Error while checking portfolio discrepancy: {e}")

if __name__ == "__main__":
    holdings = fetch_holdings()
    write_to_gsheet(holdings)
    check_portfolio_discrepancy()
