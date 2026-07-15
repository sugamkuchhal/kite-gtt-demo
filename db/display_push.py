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


# ── Tab column metadata — by column name ──────────────────────────────────────
# real_cols  → float, display as #,##0.00
# int_cols   → integer, display as number
# date_cols  → date only, display as DD-MMM-YYYY
# ts_cols    → timestamp with time, display as DD-MMM-YYYY HH:MM:SS

TAB_META = {
    "HOLDINGS": {
        "real_cols": ["average_price", "last_price", "pnl"],
        "int_cols":  ["quantity", "used_quantity", "t1_quantity"],
        "date_cols": [],
        "ts_cols":   ["fetched_at"],
    },
    "GTTS": {
        "real_cols": ["trigger_value", "last_price", "order_price"],
        "int_cols":  ["gtt_id", "order_qty"],
        "date_cols": [],
        "ts_cols":   ["fetched_at"],
    },
    "ORDERS": {
        "real_cols": ["price", "average_price"],
        "int_cols":  ["instrument_token", "quantity", "filled_qty"],
        "date_cols": [],
        "ts_cols":   ["order_timestamp", "fetched_at"],
    },
    "MARKET_DATA": {
        "real_cols": ["close", "low", "high", "volume"],
        "int_cols":  ["volume_filled"],
        "date_cols": ["date"],
        "ts_cols":   ["updated_at"],
    },
    "CORP_ACTIONS": {
        "real_cols": [],
        "int_cols":  ["critical"],
        "date_cols": ["ex_date", "record_date"],
        "ts_cols":   ["fetched_at"],
    },
}


# ── Google Sheets service ─────────────────────────────────────────────────────

def get_service():
    creds = Credentials.from_service_account_file(str(get_creds_path()), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


# ── Tab management ────────────────────────────────────────────────────────────

def get_sheet_id_map(service) -> dict[str, int]:
    meta = service.spreadsheets().get(spreadsheetId=DB_VIEW_SHEET_ID).execute()
    return {s["properties"]["title"]: s["properties"]["sheetId"]
            for s in meta["sheets"]}


def ensure_tabs(service):
    existing  = get_sheet_id_map(service)
    requests  = []
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

def _parse_dt(v: str) -> datetime | None:
    """Try to parse a string as datetime. Returns datetime or None."""
    if not v:
        return None
    for fmt in [
        "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",   "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",      "%Y-%m-%d",
        "%d-%b-%Y",
    ]:
        try:
            return datetime.strptime(v[:26], fmt[:len(v[:26])])
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _clean_date(v) -> str:
    """Format as DD-MMM-YYYY."""
    if v is None:
        return ""
    if isinstance(v, (datetime, date)):
        return v.strftime("%d-%b-%Y")
    if isinstance(v, str):
        dt = _parse_dt(v)
        if dt:
            return dt.strftime("%d-%b-%Y")
    return str(v) if v else ""


def _clean_ts(v) -> str:
    """Format as DD-MMM-YYYY HH:MM:SS."""
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%d-%b-%Y %H:%M:%S")
    if isinstance(v, str):
        dt = _parse_dt(v)
        if dt:
            return dt.strftime("%d-%b-%Y %H:%M:%S")
    return str(v) if v else ""


def _clean_value(v) -> str | int | float:
    """Generic clean — preserves int/float, passes strings through."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return round(v, 2)
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

def write_tab(service, tab: str, headers: list, rows: list):
    """Clears tab, writes headers + cleaned rows, then formats columns."""
    sheet_id_map = get_sheet_id_map(service)
    sheet_id     = sheet_id_map.get(tab)
    meta         = TAB_META.get(tab, {})

    real_cols  = meta.get("real_cols", [])
    int_cols   = meta.get("int_cols",  [])
    date_cols  = meta.get("date_cols", [])
    ts_cols    = meta.get("ts_cols",   [])

    # Resolve column names to 0-based indices
    def _idx(col_names: list[str]) -> list[int]:
        return [headers.index(c) for c in col_names if c in headers]

    real_idx  = _idx(real_cols)
    int_idx   = _idx(int_cols)
    date_idx  = _idx(date_cols)
    ts_idx    = _idx(ts_cols)

    # Clean each row — apply correct formatter per column
    def _clean_row(row: list) -> list:
        result = []
        for i, v in enumerate(row):
            if i in date_idx:
                result.append(_clean_date(v))
            elif i in ts_idx:
                result.append(_clean_ts(v))
            else:
                result.append(_clean_value(v))
        return result

    cleaned = [headers] + [_clean_row(r) for r in rows]

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

    num_rows     = len(rows) + 1
    fmt_requests = []

    # Float columns → #,##0.00
    for col_idx in real_idx:
        fmt_requests.append({"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1,
                      "endRowIndex": num_rows,
                      "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
            "cell": {"userEnteredFormat": {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00"}}},
            "fields": "userEnteredFormat.numberFormat",
        }})

    # Date columns → DD-MMM-YYYY
    for col_idx in date_idx:
        fmt_requests.append({"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1,
                      "endRowIndex": num_rows,
                      "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
            "cell": {"userEnteredFormat": {
                "numberFormat": {"type": "DATE", "pattern": "dd-mmm-yyyy"}}},
            "fields": "userEnteredFormat.numberFormat",
        }})

    # Timestamp columns → DD-MMM-YYYY HH:MM:SS
    for col_idx in ts_idx:
        fmt_requests.append({"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1,
                      "endRowIndex": num_rows,
                      "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
            "cell": {"userEnteredFormat": {
                "numberFormat": {"type": "DATE_TIME",
                                 "pattern": "dd-mmm-yyyy hh:mm:ss"}}},
            "fields": "userEnteredFormat.numberFormat",
        }})

    # Bold header row with dark background
    fmt_requests.append({"repeatCell": {
        "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
        "cell": {"userEnteredFormat": {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.26, "green": 0.26, "blue": 0.44},
            "horizontalAlignment": "CENTER",
        }},
        "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment)",
    }})

    if fmt_requests:
        service.spreadsheets().batchUpdate(
            spreadsheetId=DB_VIEW_SHEET_ID,
            body={"requests": fmt_requests},
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
