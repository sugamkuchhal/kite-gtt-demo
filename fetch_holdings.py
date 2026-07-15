import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("fetch_holdings")
atexit.register(log_end, _RUN_CTX)
# fetch_holdings.py

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from kite_session import get_kite

import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path, repo_root
from ref_sheets_utils import resolve_sheet_id

sys.path.insert(0, str(Path(__file__).resolve().parent / "db"))
from db import get_conn, init_db, update_meta
from git_utils import commit_file_if_changed

CREDS_PATH = str(get_creds_path())
ref_sheets = "PORTFOLIO"
sheet_id = resolve_sheet_id(ref_sheets)
tab_name_holdings = "ZERODHA_PORTFOLIO"
tab_name_portfolio = "Portfolio"

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
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name_holdings)

    # Clear existing content and update with new data
    ws.clear()
    ws.update(values=data, range_name='A1')
    logging.info(f"✅ Holdings written to {ref_sheets} [{tab_name_holdings}]")

def check_portfolio_discrepancy():
    CELL = "U1"
    CELL_CHECK = "V1"
    gc = get_gsheet_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name_portfolio)
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

def write_holdings_to_db(holdings):
    """Writes holdings snapshot to DB — DELETE + INSERT on every fetch."""
    if not holdings:
        return
    now = datetime.now(timezone.utc).isoformat()
    rows = [(
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
        now,
    ) for h in holdings]
    with get_conn() as conn:
        conn.execute("DELETE FROM holdings")
        conn.executemany("""
            INSERT INTO holdings (
                tradingsymbol, isin, quantity, used_quantity, t1_quantity,
                average_price, last_price, pnl, product, exchange, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        update_meta(conn, "holdings", len(rows))
    logging.info("✅ %d holdings written to DB.", len(rows))


if __name__ == "__main__":
    try:
        holdings = fetch_holdings()
        write_to_gsheet(holdings)
        check_portfolio_discrepancy()
        init_db()
        write_holdings_to_db(holdings)
        commit_file_if_changed(
            filepath="db/trading.db",
            message="chore: update trading.db — holdings [skip ci]",
            repo_root=repo_root(),
        )
        raise SystemExit(0)
    except Exception:
        logging.exception("fetch_holdings script failed.")
        raise SystemExit(1)
