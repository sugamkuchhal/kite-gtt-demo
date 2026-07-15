"""
display_push.py — Push SQLite DB tables to Google Sheets for human viewing.

Creates DB_VIEW sheet if it doesn't exist, then pushes:
  - HOLDINGS      — all current holdings
  - GTTS          — all active GTTs
  - ORDERS        — today's orders
  - MARKET_DATA   — latest date only, one row per ticker
  - CORP_ACTIONS  — ±7 day window

Usage:
    python3 db/display_push.py
    python3 db/display_push.py --dry-run    # print data, no Sheets write
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "db"))

from db import get_conn, init_db
from runtime_paths import get_creds_path, repo_root
from git_utils import commit_file_if_changed

REF_SHEETS_PATH = _REPO_ROOT / "ref_sheets.json"
DB_VIEW_KEY     = "DB_VIEW"
SCOPES          = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
EMAIL_WINDOW    = 7   # days for corporate actions

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── Google Sheets service ─────────────────────────────────────────────────────

def get_service():
    creds = Credentials.from_service_account_file(str(get_creds_path()), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def get_drive_service():
    creds = Credentials.from_service_account_file(str(get_creds_path()), scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


# ── Sheet creation ────────────────────────────────────────────────────────────

def create_db_view_sheet(service, drive_service) -> str:
    """Creates DB_VIEW spreadsheet, shares with service account. Returns sheet ID."""
    log.info("Creating DB_VIEW spreadsheet...")

    body = {"properties": {"title": "DB_VIEW — algo_trading"}}
    sheet = service.spreadsheets().create(body=body).execute()
    sheet_id = sheet["spreadsheetId"]
    log.info(f"Created: {sheet_id}")

    # Share with the service account email so it can write
    creds_data = json.loads(get_creds_path().read_text())
    sa_email   = creds_data.get("client_email", "")
    if sa_email:
        drive_service.permissions().create(
            fileId=sheet_id,
            body={"type": "user", "role": "writer", "emailAddress": sa_email},
            sendNotificationEmail=False,
        ).execute()
        log.info(f"Shared with service account: {sa_email}")

    return sheet_id


def get_or_create_sheet_id(service, drive_service) -> str:
    """Returns existing DB_VIEW sheet ID from ref_sheets.json, or creates it."""
    ref = json.loads(REF_SHEETS_PATH.read_text(encoding="utf-8"))
    for row in ref.get("rows", []):
        if row.get("ref-sheets") == DB_VIEW_KEY:
            log.info(f"DB_VIEW already exists: {row['sheet-id']}")
            return row["sheet-id"]

    # Create new sheet
    sheet_id = create_db_view_sheet(service, drive_service)

    # Add to ref_sheets.json
    ref["rows"].append({
        "ref-sheets": DB_VIEW_KEY,
        "type":       "BASE",
        "sheet-id":   sheet_id,
        "sheet-name": "DB_VIEW — algo_trading",
    })
    ref["row_count"] = len(ref["rows"])
    ref["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    REF_SHEETS_PATH.write_text(json.dumps(ref, indent=2), encoding="utf-8")
    log.info("ref_sheets.json updated with DB_VIEW entry.")

    commit_file_if_changed(
        filepath="ref_sheets.json",
        message="chore: add DB_VIEW to ref_sheets.json [skip ci]",
        repo_root=repo_root(),
    )
    return sheet_id


# ── Tab management ────────────────────────────────────────────────────────────

def ensure_tabs(service, sheet_id: str, tab_names: list[str]):
    """Creates tabs that don't exist yet."""
    meta     = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    existing = {s["properties"]["title"] for s in meta["sheets"]}
    requests = []
    for name in tab_names:
        if name not in existing:
            requests.append({"addSheet": {"properties": {"title": name}}})
    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests},
        ).execute()
        log.info(f"Created tabs: {[r['addSheet']['properties']['title'] for r in requests]}")


# ── Data loaders from SQLite ──────────────────────────────────────────────────

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
            SELECT * FROM orders
            ORDER BY order_timestamp DESC
        """).fetchall()
    return headers, [list(r) for r in rows]


def load_market_data() -> tuple[list, list]:
    headers = ["symbol", "type", "date", "close", "low", "high", "volume", "volume_filled", "updated_at"]
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT symbol, type, date, close, low, high, volume, volume_filled, updated_at
            FROM market_data
            WHERE date = (SELECT MAX(date) FROM market_data)
            ORDER BY symbol
        """).fetchall()
    return headers, [list(r) for r in rows]


def load_corporate_actions() -> tuple[list, list]:
    headers = ["symbol", "company", "subject", "ex_date", "record_date", "critical", "fetched_at"]
    today      = datetime.now().date()
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

def write_tab(service, sheet_id: str, tab: str, headers: list, rows: list):
    """Clears tab and writes headers + rows."""
    values = [headers] + rows
    service.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=f"{tab}!A1",
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()
    log.info(f"✅ {tab}: {len(rows)} rows written.")


# ── Main ──────────────────────────────────────────────────────────────────────

TABS = ["HOLDINGS", "GTTS", "ORDERS", "MARKET_DATA", "CORP_ACTIONS"]

def main():
    parser = argparse.ArgumentParser(description="Push SQLite DB to Google Sheets DB_VIEW.")
    parser.add_argument("--dry-run", action="store_true", help="Print data, no Sheets write.")
    args = parser.parse_args()

    init_db()

    # Load all data
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

    service       = get_service()
    drive_service = get_drive_service()
    sheet_id      = get_or_create_sheet_id(service, drive_service)

    ensure_tabs(service, sheet_id, TABS)

    for tab, (headers, rows) in data.items():
        write_tab(service, sheet_id, tab, headers, rows)

    log.info(f"\n✅ DB_VIEW push complete.")
    log.info(f"   https://docs.google.com/spreadsheets/d/{sheet_id}")


if __name__ == "__main__":
    main()
