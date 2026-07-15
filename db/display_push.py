"""
display_push.py — Push SQLite DB tables to Google Sheets DB_VIEW for human viewing.

Sheet: https://docs.google.com/spreadsheets/d/1HBBT7kvhZ84-vaHE7DNehfkFaua4zeznHYxbg5m1p58

Tabs pushed:
  - HOLDINGS      — all current holdings
  - GTTS          — all active GTTs
  - ORDERS        — today's orders
  - MARKET_DATA   — latest date only, one row per ticker
  - CORP_ACTIONS  — ±7 day window

Usage:
    python3 db/display_push.py
    python3 db/display_push.py --dry-run
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "db"))

from db import get_conn, init_db
from runtime_paths import get_creds_path

DB_VIEW_SHEET_ID = "1HBBT7kvhZ84-vaHE7DNehfkFaua4zeznHYxbg5m1p58"
TABS             = ["HOLDINGS", "GTTS", "ORDERS", "MARKET_DATA", "CORP_ACTIONS"]
EMAIL_WINDOW     = 7
SCOPES           = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── Google Sheets service ─────────────────────────────────────────────────────

def get_service():
    creds = Credentials.from_service_account_file(str(get_creds_path()), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


# ── Tab management ────────────────────────────────────────────────────────────

def ensure_tabs(service):
    """Creates tabs that don't exist yet. Removes default Sheet1 if present."""
    meta     = service.spreadsheets().get(spreadsheetId=DB_VIEW_SHEET_ID).execute()
    existing = {s["properties"]["title"]: s["properties"]["sheetId"]
                for s in meta["sheets"]}
    requests = []

    for name in TABS:
        if name not in existing:
            requests.append({"addSheet": {"properties": {"title": name}}})

    # Remove default Sheet1 if it still exists
    if "Sheet1" in existing:
        requests.append({"deleteSheet": {"sheetId": existing["Sheet1"]}})

    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=DB_VIEW_SHEET_ID,
            body={"requests": requests},
        ).execute()
        log.info(f"Tabs updated.")


# ── Data loaders ──────────────────────────────────────────────────────────────

def load_holdings() -> tuple[list, list]:
    headers = ["tradingsymbol", "isin", "quantity", "used_quantity", "t1_quantity",
               "average_price", "last_price", "pnl", "product", "exchange", "fetched_at"]
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM holdings ORDER BY tradingsymbol").fetchall()
    return headers, [list(r) for r in rows]


def load_gtts() -> tuple[list, list]:
    headers = ["gtt_id", "symbol", "exchange", "trigger_type", "trigger_value",
               "last_price", "order_price", "order_qty", "order_type",
               "product", "transaction_type", "status", "fetched_at"]
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM gtts ORDER BY symbol").fetchall()
    return headers, [list(r) for r in rows]


def load_orders() -> tuple[list, list]:
    headers = ["order_id", "exchange_order_id", "instrument_token", "tradingsymbol",
               "transaction_type", "order_type", "product", "quantity", "filled_qty",
               "price", "average_price", "status", "order_timestamp", "fetched_at"]
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM orders ORDER BY order_timestamp DESC
        """).fetchall()
    return headers, [list(r) for r in rows]


def load_market_data() -> tuple[list, list]:
    headers = ["symbol", "type", "date", "close", "low", "high",
               "volume", "volume_filled", "updated_at"]
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT symbol, type, date, close, low, high, volume, volume_filled, updated_at
            FROM market_data
            WHERE date = (SELECT MAX(date) FROM market_data)
            ORDER BY symbol
        """).fetchall()
    return headers, [list(r) for r in rows]


def load_corporate_actions() -> tuple[list, list]:
    headers = ["symbol", "company", "subject", "ex_date",
               "record_date", "critical", "fetched_at"]
    today       = datetime.now().date()
    window_from = (today - timedelta(days=EMAIL_WINDOW)).strftime("%Y-%m-%d")
    window_to   = (today + timedelta(days=EMAIL_WINDOW)).strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT symbol, company, subject, ex_date, record_date, critical, fetched_at
            FROM corporate_actions
            WHERE ex_date BETWEEN ? AND ?
            ORDER BY ex_date ASC, symbol ASC
        """, (window_from, window_to)).fetchall()
    return headers, [list(r) for r in rows]


# ── Sheet writer ──────────────────────────────────────────────────────────────

def write_tab(service, tab: str, headers: list, rows: list):
    """Clears tab and writes headers + rows."""
    range_name = f"{tab}!A1"
    values     = [headers] + [[str(v) if v is not None else "" for v in r] for r in rows]

    service.spreadsheets().values().clear(
        spreadsheetId=DB_VIEW_SHEET_ID,
        range=range_name,
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=DB_VIEW_SHEET_ID,
        range=range_name,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()

    log.info(f"✅ {tab}: {len(rows)} rows written.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Push SQLite DB to Google Sheets DB_VIEW.")
    parser.add_argument("--dry-run", action="store_true", help="Print data, no Sheets write.")
    args = parser.parse_args()

    init_db()

    data = {
        "HOLDINGS":     load_holdings(),
        "GTTS":         load_gtts(),
        "ORDERS":       load_orders(),
        "MARKET_DATA":  load_market_data(),
        "CORP_ACTIONS": load_corporate_actions(),
    }

    if args.dry_run:
        for tab, (headers, rows) in data.items():
            print(f"\n── {tab} ({len(rows)} rows)")
            print("  " + " | ".join(headers))
            for r in rows[:3]:
                print("  " + " | ".join(str(v) for v in r))
            if len(rows) > 3:
                print(f"  ... {len(rows) - 3} more rows")
        return

    service = get_service()
    ensure_tabs(service)

    for tab, (headers, rows) in data.items():
        write_tab(service, tab, headers, rows)

    log.info(f"\n✅ DB_VIEW push complete.")
    log.info(f"   https://docs.google.com/spreadsheets/d/{DB_VIEW_SHEET_ID}")


if __name__ == "__main__":
    main()
