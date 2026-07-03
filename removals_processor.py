#!/usr/bin/env python3
"""
removals_processor.py

Pure sheet actuator for the removals process — no email/Telegram here;
comms are owned by algo_tickers_mailer.py, which imports run_removals().

Step 1 — REMOVALS tab (ref sheet: TICKER)
  * Column A holds the pending-removals list (data from row 2).
  * Column B is formula-driven; the exact text "CAN REMOVE NOW"
    flags a ticker as removable. Cell B1 is the removable count.
  * Flagged tickers are collected, then column A is compacted upward
    (flagged rows dropped, order preserved) and trailing cells cleared.
  * Column B is never written to (formulas stay intact).

Step 2 — FEED sheet tabs (ref sheet: FEED)
  * Tabs: SGST_FILTERED_TICKERS, SUPER_FILTERED_TICKERS,
    TURTLE_FILTERED_TICKERS — all share the A:C layout
    DATE | TICKER | CATEGORY (data from row 2).
  * Any row whose column B ticker is in the removal list is dropped;
    the A:C block is compacted upward and trailing rows cleared.
  * Tickers not found in a tab are recorded (non-fatal).
  * A failure on one tab is recorded and the remaining tabs still run,
    so the report always reflects the full picture.

Can be run standalone (manual use) or imported by the mailer.
"""

import logging

import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

# ==========================
# Config (constants)
# ==========================
REMOVALS_REF = "TICKER"
REMOVALS_TAB = "REMOVALS"
CAN_REMOVE_TEXT = "CAN REMOVE NOW"

FEED_REF = "FEED"
FEED_TABS = [
    "SGST_FILTERED_TICKERS",
    "SUPER_FILTERED_TICKERS",
    "TURTLE_FILTERED_TICKERS",
]
FEED_NUM_COLS = 3  # A:C -> DATE | TICKER | CATEGORY
FEED_TICKER_COL = 1  # column B (0-indexed)

SERVICE_CREDS = str(get_creds_path())

# ==========================
# Core logic
# ==========================

def get_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_CREDS, scopes=scope)
    return gspread.authorize(creds)


def validate_removals_header(ws):
    """Fail loudly if the REMOVALS tab layout has changed."""
    a1 = (ws.acell("A1").value or "").strip().upper()
    if a1 != "REMOVALS":
        raise RuntimeError(
            f"Header validation failed on '{REMOVALS_TAB}' tab: "
            f"expected 'REMOVALS' in A1, found '{a1}'"
        )
    logging.info("Header validation passed on '%s' tab.", REMOVALS_TAB)


def process_removals_tab(client):
    """
    Returns the list of tickers flagged CAN REMOVE NOW, after compacting
    column A of the REMOVALS tab (flagged rows removed, order preserved).
    """
    sheet_id = resolve_sheet_id(REMOVALS_REF)
    ws = client.open_by_key(sheet_id).worksheet(REMOVALS_TAB)

    validate_removals_header(ws)

    values = ws.get_values("A2:B")  # ragged: rows may have 1 or 2 cells
    old_len = len(values)

    keep, remove = [], []
    for i, row in enumerate(values):
        ticker = (row[0] if len(row) > 0 else "").strip()
        flag = (row[1] if len(row) > 1 else "").strip().upper()
        if not ticker:
            continue
        if flag == CAN_REMOVE_TEXT:
            remove.append(ticker)
            logging.info("Flagged for removal: %s (row %d)", ticker, i + 2)
        else:
            keep.append(ticker)

    if not remove:
        logging.info("No '%s' rows found — nothing to remove.", CAN_REMOVE_TEXT)
        return []

    # Write compacted list back to column A, then clear trailing cells.
    if keep:
        ws.update(
            f"A2:A{len(keep) + 1}",
            [[t] for t in keep],
            value_input_option="RAW",
        )
    if old_len > len(keep):
        ws.batch_clear([f"A{len(keep) + 2}:A{old_len + 1}"])

    logging.info(
        "REMOVALS tab compacted: %d removed, %d remaining.",
        len(remove), len(keep),
    )
    return remove


def purge_feed_tab(client, sheet_id, tab_name, remove_set):
    """
    Drops rows from A2:C whose column B ticker is in remove_set,
    compacts the block upward and clears trailing rows.

    Returns a result dict for reporting.
    """
    ws = client.open_by_key(sheet_id).worksheet(tab_name)
    values = ws.get_values("A2:C")
    old_len = len(values)

    keep_rows = []
    found = set()
    for row in values:
        padded = row + [""] * (FEED_NUM_COLS - len(row))
        ticker = padded[FEED_TICKER_COL].strip()
        if ticker in remove_set:
            found.add(ticker)
            continue
        keep_rows.append(padded[:FEED_NUM_COLS])

    not_found = sorted(remove_set - found)
    for t in not_found:
        logging.info("[%s] not found in tab: %s", tab_name, t)

    purged = old_len - len(keep_rows)
    if purged > 0:
        if keep_rows:
            ws.update(
                f"A2:C{len(keep_rows) + 1}",
                keep_rows,
                value_input_option="RAW",
            )
        ws.batch_clear([f"A{len(keep_rows) + 2}:C{old_len + 1}"])
        logging.info(
            "[%s] purged %d row(s) (%s); %d row(s) remain.",
            tab_name, purged, ", ".join(sorted(found)), len(keep_rows),
        )
    else:
        logging.info("[%s] no matching rows — tab unchanged.", tab_name)

    return {
        "tab": tab_name,
        "purged": purged,
        "purged_tickers": sorted(found),
        "not_found": not_found,
        "remaining": len(keep_rows),
        "error": None,
    }


def run_removals():
    """
    Full removals cycle. Returns a result dict:
      {
        "removed": [tickers pulled off the REMOVALS tab],
        "tabs": [per-tab result dicts (see purge_feed_tab)],
        "error": None | str  (fatal error in step 1, if any)
      }
    A per-tab failure is recorded in that tab's result dict and the
    remaining tabs still run.
    """
    result = {"removed": [], "tabs": [], "error": None}

    client = get_client()

    try:
        removed = process_removals_tab(client)
    except Exception as e:
        logging.exception("REMOVALS tab processing failed: %s", e)
        result["error"] = f"REMOVALS tab processing failed: {e}"
        return result

    result["removed"] = removed
    if not removed:
        return result

    logging.info("Tickers to purge from FEED tabs: %s", ", ".join(removed))
    remove_set = set(removed)

    feed_sheet_id = resolve_sheet_id(FEED_REF)
    for tab in FEED_TABS:
        try:
            result["tabs"].append(
                purge_feed_tab(client, feed_sheet_id, tab, remove_set)
            )
        except Exception as e:
            logging.exception("[%s] purge failed: %s", tab, e)
            result["tabs"].append({
                "tab": tab,
                "purged": 0,
                "purged_tickers": [],
                "not_found": [],
                "remaining": None,
                "error": str(e),
            })

    logging.info("Removals processing complete.")
    return result

# ==========================
# Standalone entry point
# ==========================

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    from script_logger import log_start, log_end
    ctx = log_start("removals_processor")
    try:
        result = run_removals()
    finally:
        log_end(ctx)

    if result["error"]:
        raise SystemExit(1)
    if not result["removed"]:
        logging.info("Nothing to remove — done.")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
