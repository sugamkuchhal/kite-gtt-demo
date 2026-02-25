import logging
import subprocess
from kite_session import get_kite
from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import get_gsheet_client, open_worksheet

# Google Sheet details
ALGO_NAME = "PORTFOLIO_STOCKS"
ZERODHA_GTT_DATA_TAB_NAME = "ZERODHA_GTT_DATA"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def fetch_all_gtts():
    kite = get_kite()
    try:
        gtts = kite.get_gtts()
        if not gtts:
            logging.info("No GTTs found.")
            return
        
        formatted = []
        for g in gtts:
            order = g['orders'][0] if g['orders'] else {}
            condition = g.get("condition", {})
            trigger_values = condition.get("trigger_values", [])
            
            row = {
                "GTT ID": g.get("id"),
                "Symbol": condition.get("tradingsymbol"),
                "Exchange": condition.get("exchange"),
                "Trigger Type": g.get("type"),
                "Trigger Value": trigger_values[0] if trigger_values else None,
                "Order Price": order.get("price"),
                "Order Qty": order.get("quantity"),
                "Order Type": order.get("order_type"),
                "Product": order.get("product"),
                "Transaction Type": order.get("transaction_type"),
                "Status": g.get("status")
            }
            formatted.append(row)
            # logging.info(f"GTT Row: {row}")
        
        client = get_gsheet_client()
        sheet = open_worksheet(client, ZERODHA_GTT_DATA_TAB_NAME, spreadsheet_id=get_sheet_id(ALGO_NAME))
        
        # Prepare headers and rows
        headers = list(formatted[0].keys())
        values = [headers] + [[row.get(h, "") for h in headers] for row in formatted]
        
        # Write to sheet
        sheet.clear()
        sheet.update(values=values, range_name="A1")
        
        logging.info(f"✅ {len(formatted)} GTTs written to sheet: {ZERODHA_GTT_DATA_TAB_NAME}")
        
    except Exception as e:
        logging.error(f"❌ Failed to fetch/write GTTs: {e}")

if __name__ == "__main__":
    fetch_all_gtts()