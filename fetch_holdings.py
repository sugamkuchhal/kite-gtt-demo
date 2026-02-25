# fetch_holdings.py

import logging
from kite_session import get_kite

from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import get_gsheet_client, open_spreadsheet

ALGO_NAME = "PORTFOLIO_STOCKS"
TAB_NAME = "ZERODHA_PORTFOLIO"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


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
            h.get("exchange")
        ]
        data.append(row)

    client = get_gsheet_client()
    sh = open_spreadsheet(client, spreadsheet_id=get_sheet_id(ALGO_NAME))
    ws = sh.worksheet(TAB_NAME)
    ws.clear()
    ws.update(values=data, range_name='A1')
    logging.info(f"✅ Successfully wrote {len(holdings)} holdings to Google Sheet: {TAB_NAME}")


def check_cell_status():
    client = get_gsheet_client()
    sh = open_spreadsheet(client, spreadsheet_id=get_sheet_id(ALGO_NAME))
    ws = sh.worksheet(TAB_NAME)
    CELL = "J1"
    CELL_CHECK = "K1"
    try:
        cell_value = ws.acell(CELL).value
        cell_check_value = ws.acell(CELL_CHECK).value
        if cell_value == "0" and cell_check_value == "True":
            logging.info(f"✅ Process complete: {CELL}=0 and {CELL_CHECK}=True")
        else:
            logging.error(f"❌ Process not complete: {CELL}={cell_value}, {CELL_CHECK}={cell_check_value}")
    except Exception as e:
        logging.error(f"❌ Failed to read check cells: {e}")


def main():
    holdings = fetch_holdings()
    write_to_gsheet(holdings)
    check_cell_status()


if __name__ == "__main__":
    main()
