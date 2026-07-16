#!/usr/bin/env python3
"""
remover_delisted.py

Pure sheet actuator for the "Order Tickers w/o Category" healing process.
No email/Telegram here; comms are owned by algo_checklist_mailer.py,
which imports run_delisted().

Detection — DELISTED tab (ref sheet: PORTFOLIO)
  * Cell A1 holds a formula-driven count. When > 0, the healer is triggered.
  * A2 downward contains the delisted tickers.

Action
  * Read A2:A{last_a} (the delisted ticker list).
  * Append those values to col C, starting at C{max(2, last_c + 1)}.
  * Clear A2:A{last_a} — A1 recalculates to 0 automatically.

Can be run standalone (manual use) or imported by the mailer.
"""

import logging

import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

# ==========================
# Config
# ==========================
DELISTED_REF  = "PORTFOLIO"
DELISTED_TAB  = "DELISTED"
SIGNAL_CELL   = "A1"

SERVICE_CREDS = str(get_creds_path())

# ==========================
# Core logic
# ==========================

def get_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_CREDS, scopes=scope)
    return gspread.authorize(creds)


def run_delisted():
    """
    Full delisted cycle. Returns a result dict:
      {
        "moved":       [ticker values moved from col A to col C],
        "paste_start": row number where col C paste began,
        "paste_end":   row number where col C paste ended,
        "error":       None | str
      }
    """
    result = {"moved": [], "paste_start": None, "paste_end": None, "error": None}

    client  = get_client()
    sheet_id = resolve_sheet_id(DELISTED_REF)
    ws      = client.open_by_key(sheet_id).worksheet(DELISTED_TAB)

    # Read full col A and col C to determine extents
    col_a = ws.col_values(1)   # 1-indexed; includes A1
    col_c = ws.col_values(3)   # 1-indexed; includes C1

    # last_a: last row with data in col A (1-based sheet row)
    last_a = len(col_a)

    # last_c: last row with data in col C (1-based); floor at 1 (header always present)
    last_c = max(1, len(col_c))

    # Data to move: A2:A{last_a}
    if last_a < 2:
        logging.info("DELISTED: col A has no data below A1 — nothing to move.")
        return result

    tickers = [row.strip() for row in col_a[1:] if row.strip()]   # col_a[0] is A1 (signal)
    if not tickers:
        logging.info("DELISTED: col A data rows are all blank — nothing to move.")
        return result

    logging.info("DELISTED: %d ticker(s) to move: %s", len(tickers), ", ".join(tickers))

    # Paste destination: C{paste_start}:C{paste_end}
    paste_start = max(2, last_c + 1)
    paste_end   = paste_start + len(tickers) - 1

    try:
        # Write tickers to col C
        ws.update(
            range_name=f"C{paste_start}:C{paste_end}",
            values=[[t] for t in tickers],
            value_input_option="RAW",
        )
        logging.info("DELISTED: pasted %d ticker(s) into C%d:C%d.", len(tickers), paste_start, paste_end)

        # Clear A2:A{last_a}
        ws.batch_clear([f"A2:A{last_a}"])
        logging.info("DELISTED: cleared A2:A%d — A1 will recalculate to 0.", last_a)

        result["moved"]       = tickers
        result["paste_start"] = paste_start
        result["paste_end"]   = paste_end

    except Exception as e:
        logging.exception("DELISTED: sheet update failed: %s", e)
        result["error"] = str(e)

    return result


# ==========================
# Standalone entry point
# ==========================

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    from script_logger import log_start, log_end
    ctx = log_start("remover_delisted")
    try:
        result = run_delisted()
    finally:
        log_end(ctx)

    if result["error"]:
        raise SystemExit(1)
    if not result["moved"]:
        logging.info("Nothing to move — done.")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
