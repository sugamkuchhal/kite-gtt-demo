import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from kite_session import get_kite
from google_sheets_utils import get_gsheet_client
from ref_sheets_utils import resolve_sheet_id

sys.path.insert(0, str(Path(__file__).resolve().parent / "db"))
from db import get_conn, init_db, update_meta
from git_utils import commit_file_if_changed
from runtime_paths import repo_root

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("fetch_all_gtts")
atexit.register(log_end, _RUN_CTX)
# Google Sheet details
ref_sheets = "PORTFOLIO"
sheet_id = resolve_sheet_id(ref_sheets)
tab_name = "ZERODHA_GTT_DATA"

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
        sheet = client.open_by_key(sheet_id).worksheet(tab_name)
        
        # Prepare headers and rows
        headers = list(formatted[0].keys())
        values = [headers] + [[row.get(h, "") for h in headers] for row in formatted]
        
        # Write to sheet
        sheet.clear()
        sheet.update(values=values, range_name="A1")
        
        logging.info(f"✅ {len(formatted)} GTTs written to sheet: {tab_name}")
        
    except Exception as e:
        logging.error(f"❌ Failed to fetch/write GTTs: {e}")

def write_gtts_to_db(gtts):
    """Writes GTT snapshot to DB — DELETE + INSERT on every fetch."""
    if not gtts:
        return
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for g in gtts:
        order     = g["orders"][0] if g.get("orders") else {}
        condition = g.get("condition", {})
        triggers  = condition.get("trigger_values", [])
        rows.append((
            g.get("id"),
            condition.get("tradingsymbol"),
            condition.get("exchange"),
            g.get("type"),
            triggers[0] if triggers else None,
            condition.get("last_price"),
            order.get("price"),
            order.get("quantity"),
            order.get("order_type"),
            order.get("product"),
            order.get("transaction_type"),
            g.get("status"),
            now,
        ))
    with get_conn() as conn:
        conn.execute("DELETE FROM gtts")
        conn.executemany("""
            INSERT INTO gtts (
                gtt_id, symbol, exchange, trigger_type, trigger_value,
                last_price, order_price, order_qty, order_type,
                product, transaction_type, status, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        update_meta(conn, "gtts", len(rows))
    logging.info("✅ %d GTTs written to DB.", len(rows))


def run_cli():
    try:
        fetch_all_gtts()
        return 0
    except Exception:
        logging.exception("fetch_all_gtts failed.")
        return 1

if __name__ == "__main__":
    init_db()
    kite = get_kite()
    gtts = kite.get_gtts()
    write_gtts_to_db(gtts or [])
    commit_file_if_changed(
        filepath="db/trading.db",
        message="chore: update trading.db — gtts [skip ci]",
        repo_root=repo_root(),
    )
    raise SystemExit(run_cli())
