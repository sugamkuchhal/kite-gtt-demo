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
from datetime import datetime, timedelta, date
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "db"))

from db import get_conn, init_db
from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

DB_VIEW_SHEET_ID = resolve_sheet_id("DB_VIEW")
TABS             = ["HOLDINGS", "GTTS", "ORDERS", "MARKET_DATA", "CORP_ACTIONS"]
EMAIL_WINDOW     = 7
SCOPES           = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ── Tab column metadata ───────────────────────────────────────────────────────
# For each tab: which 0-based column indices are REAL (2dp) and which are DATE

TAB_META = {
    "HOLDINGS": {
        # tradingsymbol, isin, quantity, used_quantity, t1_quantity,
        # average_price, last_price, pnl, product, exchange, fetched_at
        "real_cols":  [5, 6, 7],       # average_price, last_price, pnl
        "int_cols":   [2, 3, 4],       # quantity, used_quantity, t1_quantity
        "date_cols":  [10],            # fetched_at
    },
    "GTTS": {
        # gtt_id, symbol, exchange, trigger_type, trigger_value,
        # last_price, order_price, order_qty, order_type, product,
        # transaction_type, status, fetched_at
        "real_cols":  [4, 5, 6],       # trigger_value, last_price, order_price
        "int_cols":   [0, 7],          # gtt_id, order_qty
        "date_cols":  [12],            # fetched_at
    },
    "ORDERS": {
        # order_id, exchange_order_id, instrument_token, tradingsymbol,
        # transaction_type, order_type, product, quantity, filled_qty,
        # price, average_price, status, order_timestamp, fetched_at
        "real_cols":  [9, 10],         # price, average_price
        "int_cols":   [2, 7, 8],       # instrument_token, quantity, filled_qty
        "date_cols":  [12, 13],        # order_timestamp, fetched_at
    },
    "MARKET_DATA": {
        # symbol, type, date, close, low, high, volume, volume_filled, updated_at
        "real_cols":  [3, 4, 5, 6],    # close, low, high, volume
        "int_cols":   [7],             # volume_filled
        "date_cols":  [2, 8],          # date, updated_at
    },
    "CORP_ACTIONS": {
        # symbol, company, subject, ex_date, record_date, critical, fetched_at
        "real_cols":  [],
        "int_cols":   [5],             # critical
        "date_cols":  [3, 4, 6],       # ex_date, record_date, fetched_at
    },
}


# ── Google Sheets service ─────────────────────────────────────────────────────

def get_service():
    creds = Credentials.from_service_account_file(str(get_creds_path()), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


# ── Tab management ────────────────────────────────────────────────────────────

def get_sheet_id_map(service) -> dict[str, int]:
    """Returns {tab_title: sheetId} for all tabs."""
    meta = service.spreadsheets().get(spreadsheetId=DB_VIEW_SHEET_ID).execute()
    return {s["properties"]["title"]: s["properties"]["sheetId"]
            for s in meta["sheets"]}


def ensure_tabs(service):
    """Creates tabs that don't exist yet. Removes default Sheet1 if present."""
    existing = get_sheet_id_map(service)
    requests = []

    for name in TABS:
        if name not in existing:
            requests.append({"addSheet": {"properties": {"title": name}}})

    if "Sheet1" in existing:
        requests.append({"deleteSheet": {"sheetId": existing["Sheet1"]}})

    if requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=DB_VIEW_SHEET_ID,
            body={"requests": requests},
        ).execute()
        log.info("Tabs updated.")


# ── Value cleaning ────────────────────────────────────────────────────────────

_DATE_FORMATS = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                 "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"]

def _parse_date(v: str) -> str | None:
    """Try to parse a date string. Returns DD-MMM-YYYY string or None."""
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(v[:26], fmt[:len(v)])
            return dt.strftime("%d-%b-%Y")
        except (ValueError, TypeError):
            continue
    # Try built-in fromisoformat
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        return dt.strftime("%d-%b-%Y")
    except (ValueError, AttributeError):
        pass
    return None


def _clean(v) -> str | int | float:
    """Clean a single value for Sheets."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return round(v, 2)
    if isinstance(v, (datetime, date)):
        return v.strftime("%d-%b-%Y")
    if isinstance(v, str):
        parsed = _parse_date(v)
        if parsed:
            return parsed
        return v
    return str(v)


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
        rows = conn.execute("SELECT * FROM orders ORDER BY order_timestamp DESC").fetchall()
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

def _col_letter(idx: int) -> str:
    """0-based column index to letter (0=A, 25=Z, 26=AA)."""
    result = ""
    idx += 1
    while idx:
        idx, r = divmod(idx - 1, 26)
        result = chr(65 + r) + result
    return result


def write_tab(service, tab: str, headers: list, rows: list):
    """Clears tab, writes headers + cleaned rows, then formats columns."""
    sheet_id_map = get_sheet_id_map(service)
    sheet_id     = sheet_id_map.get(tab)
    meta         = TAB_META.get(tab, {})
    real_cols    = meta.get("real_cols", [])
    date_cols    = meta.get("date_cols", [])

    # Clean all values
    cleaned = [headers] + [[_clean(v) for v in r] for r in rows]

    # Write data
    service.spreadsheets().values().clear(
        spreadsheetId=DB_VIEW_SHEET_ID,
        range=f"{tab}!A1",
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=DB_VIEW_SHEET_ID,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": cleaned},
    ).execute()

    if not sheet_id or not rows:
        log.info(f"✅ {tab}: {len(rows)} rows written.")
        return

    # Format columns
    num_rows = len(rows) + 1  # include header
    format_requests = []

    for col_idx in real_cols:
        format_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": num_rows,
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1,
                },
                "cell": {"userEnteredFormat": {
                    "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}
                }},
                "fields": "userEnteredFormat.numberFormat",
            }
        })

    for col_idx in date_cols:
        format_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "endRowIndex": num_rows,
                    "startColumnIndex": col_idx,
                    "endColumnIndex": col_idx + 1,
                },
                "cell": {"userEnteredFormat": {
                    "numberFormat": {"type": "DATE", "pattern": "dd-mmm-yyyy"}
                }},
                "fields": "userEnteredFormat.numberFormat",
            }
        })

    # Bold header row
    format_requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1,
            },
            "cell": {"userEnteredFormat": {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.26, "green": 0.26, "blue": 0.44},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)",
        }
    })

    if format_requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=DB_VIEW_SHEET_ID,
            body={"requests": format_requests},
        ).execute()

    log.info(f"✅ {tab}: {len(rows)} rows written and formatted.")


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
                print("  " + " | ".join(str(_clean(v)) for v in r))
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
