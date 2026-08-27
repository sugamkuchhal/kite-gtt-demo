import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from kite_session import get_kite
from google_sheets_utils import get_gsheet_client, gsheets_retry
from ref_sheets_utils import resolve_sheet_id

sys.path.insert(0, str(Path(__file__).resolve().parent / "db"))
from db import get_conn, init_db, update_meta
from git_utils import commit_file_if_changed
from runtime_paths import repo_root


import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("fetch_all_orders")
atexit.register(log_end, _RUN_CTX)
ref_sheets = "PORTFOLIO"
sheet_id = resolve_sheet_id(ref_sheets)
tab_name_orders = "ZERODHA_ORDERS"
tab_name_latest_orders = "LATEST_ORDERS"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def fetch_all_orders():
    kite = get_kite()
    try:
        orders = kite.orders()
        if not orders:
            logging.info("No orders found.")
            return

        headers = [
            "Order ID", "Exchange Order ID", "Instrument Token",
            "Trading Symbol", "Transaction Type", "Order Type", "Product",
            "Quantity", "Filled Qty", "Price", "Average Price",
            "Status", "Order Timestamp",
        ]

        formatted = []
        for o in orders:
            ts = o.get("order_timestamp")
            if isinstance(ts, datetime):
                ts = ts.strftime("%Y-%m-%d %H:%M:%S")
            row = [
                str(o.get("order_id") or ""),
                str(o.get("exchange_order_id") or ""),
                str(o.get("instrument_token") or ""),
                o.get("tradingsymbol"),
                o.get("transaction_type"),
                o.get("order_type"),
                o.get("product"),
                int(o.get("quantity") or 0),
                int(o.get("filled_quantity") or 0),
                float(o.get("price") or 0.0),
                float(o.get("average_price") or 0.0),
                o.get("status"),
                ts,
            ]
            formatted.append(row)

        client = get_gsheet_client()
        sheet = client.open_by_key(sheet_id).worksheet(tab_name_orders)

        values = [headers] + formatted
        
        gsheets_retry(sheet.clear)
        gsheets_retry(sheet.update, values=values, range_name="A1")
        logging.info(f"✅ {len(formatted)} orders written to sheet: {tab_name_orders}")

        # ---- Post Check: LATEST_ORDERS!I1 ----
        latest_orders_sheet = client.open_by_key(sheet_id).worksheet(tab_name_latest_orders)
        check_value = gsheets_retry(latest_orders_sheet.acell, "I1").value

        if check_value == "0":
            logging.info("✅ Post-check passed: LATEST_ORDERS!I1 = 0 → Process completed successfully")
        else:
            logging.error(f"❌ Post-check failed: LATEST_ORDERS!I1 = {check_value} → Process not completed")

    except Exception as e:
        logging.error(f"❌ Failed to fetch/write orders: {e}")

def write_orders_to_db(orders):
    """Writes today's orders snapshot to DB — DELETE + INSERT on every fetch."""
    if not orders:
        return
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for o in orders:
        ts = o.get("order_timestamp")
        if isinstance(ts, datetime):
            ts = ts.isoformat()
        rows.append((
            str(o.get("order_id") or ""),
            str(o.get("exchange_order_id") or ""),
            str(o.get("instrument_token") or ""),
            o.get("tradingsymbol"),
            o.get("transaction_type"),
            o.get("order_type"),
            o.get("product"),
            o.get("quantity"),
            o.get("filled_quantity"),
            o.get("price"),
            o.get("average_price"),
            o.get("status"),
            str(ts) if ts else None,
            now,
        ))
    with get_conn() as conn:
        conn.execute("DELETE FROM orders")
        conn.executemany("""
            INSERT INTO orders (
                order_id, exchange_order_id, instrument_token, tradingsymbol,
                transaction_type, order_type, product, quantity, filled_qty,
                price, average_price, status, order_timestamp, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        update_meta(conn, "orders", len(rows))
    logging.info("✅ %d orders written to DB.", len(rows))


def run_cli():
    try:
        fetch_all_orders()
        return 0
    except Exception:
        logging.exception("fetch_all_orders failed.")
        return 1

if __name__ == "__main__":
    init_db()
    kite = get_kite()
    orders = kite.orders()
    write_orders_to_db(orders or [])
    commit_file_if_changed(
        filepath="db/trading.db",
        message="chore: update trading.db — orders [skip ci]",
        repo_root=repo_root(),
    )
    raise SystemExit(run_cli())
