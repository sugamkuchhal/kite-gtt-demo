import logging
from kite_session import get_kite
from google_sheets_utils import get_gsheet_client
from datetime import datetime


PORTFOLIO_SHEET_ID = "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s"
ORDERS_SHEET = "ZERODHA_ORDERS"
LATEST_ORDERS_TAB = "LATEST_ORDERS"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def fetch_all_orders():
    kite = get_kite()
    try:
        orders = kite.orders()
        if not orders:
            logging.info("No orders found.")
            return

        formatted = []
        for o in orders:
            row = {
                "Order ID": o.get("order_id"),
                "Exchange Order ID": o.get("exchange_order_id"),
                "Instrument Token": o.get("instrument_token"),
                "Trading Symbol": o.get("tradingsymbol"),
                "Transaction Type": o.get("transaction_type"),
                "Order Type": o.get("order_type"),
                "Product": o.get("product"),
                "Quantity": o.get("quantity"),
                "Filled Qty": o.get("filled_quantity"),
                "Price": o.get("price"),
                "Average Price": o.get("average_price"),
                "Status": o.get("status"),
                "Order Timestamp": o.get("order_timestamp"),
            }
            formatted.append(row)
            # logging.info(f"Order Row: {row}")
        
        client = get_gsheet_client()
        sheet = client.open_by_key(PORTFOLIO_SHEET_ID).worksheet(ORDERS_SHEET)
        
        headers = list(formatted[0].keys())

        values = [headers]
        for row in formatted:
            processed_row = []
            for h in headers:
                val = row.get(h, "")
                if isinstance(val, datetime):
                    val = val.strftime("%Y-%m-%d %H:%M:%S")
                processed_row.append(val)
            values.append(processed_row)
        
        sheet.clear()
        sheet.update(values=values, range_name="A1")
        logging.info(f"✅ {len(formatted)} orders written to sheet: {ORDERS_SHEET}")

        # ---- Post Check: LATEST_ORDERS!I1 ----
        latest_orders_sheet = client.open_by_key(PORTFOLIO_SHEET_ID).worksheet(LATEST_ORDERS_TAB)
        check_value = latest_orders_sheet.acell("I1").value

        if check_value == "0":
            logging.info("✅ Post-check passed: LATEST_ORDERS!I1 = 0 → Process completed successfully")
        else:
            logging.error(f"❌ Post-check failed: LATEST_ORDERS!I1 = {check_value} → Process not completed")

    except Exception as e:
        logging.error(f"❌ Failed to fetch/write orders: {e}")

if __name__ == "__main__":
    fetch_all_orders()
