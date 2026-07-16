#!/usr/bin/env python3
"""
populate_gids.py

Reads the Checklist tab of the TICKER sheet and populates column I with
the GID (numeric sheet ID) for each row where:
  - Column F holds a Spreadsheet ID
  - Column G holds a tab name
  - Column I is currently empty

Optimisation: spreadsheets that appear in multiple rows are opened only
once; worksheet metadata is cached per spreadsheet.

A single batch_update() writes all results back to column I at the end.
"""

import logging

import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

# ==========================
# Config
# ==========================
REF_KEY  = "TICKER"
TAB_NAME = "Checklist"

COL_F = 5   # 0-indexed  (Spreadsheet ID)
COL_G = 6   # 0-indexed  (Tab name)
COL_I = 8   # 0-indexed  (GID — to be populated)

DATA_START_ROW = 2   # row 1 is the header; data begins at row 2

SERVICE_CREDS = str(get_creds_path())

# ==========================
# Core logic
# ==========================

def get_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_CREDS, scopes=scope)
    return gspread.authorize(creds)


def fetch_gid(client, spreadsheet_id: str, tab_name: str, cache: dict) -> int | None:
    """
    Return the GID for tab_name inside spreadsheet_id.
    Uses cache to avoid re-opening the same spreadsheet.
    Returns None if the tab is not found.
    """
    if spreadsheet_id not in cache:
        logging.info("Opening spreadsheet: %s", spreadsheet_id)
        wb = client.open_by_key(spreadsheet_id)
        cache[spreadsheet_id] = {ws.title: ws.id for ws in wb.worksheets()}
        logging.info(
            "Cached %d tab(s) for %s", len(cache[spreadsheet_id]), spreadsheet_id
        )

    tab_map = cache[spreadsheet_id]
    gid = tab_map.get(tab_name)
    if gid is None:
        logging.warning(
            "Tab '%s' not found in spreadsheet %s (available: %s)",
            tab_name, spreadsheet_id, ", ".join(tab_map.keys()),
        )
    return gid


def run():
    client = get_client()

    # Open the source sheet
    source_id = resolve_sheet_id(REF_KEY)
    ws = client.open_by_key(source_id).worksheet(TAB_NAME)
    all_rows = ws.get_all_values()

    header     = all_rows[0] if all_rows else []
    data_rows  = all_rows[DATA_START_ROW - 1:]  # 0-indexed slice

    logging.info(
        "Read %d data row(s) from %s!%s (header: %s)",
        len(data_rows), REF_KEY, TAB_NAME, header,
    )

    cache: dict = {}          # spreadsheet_id -> {tab_title: gid}
    updates: list = []        # (sheet_row_number, gid_value)

    for i, row in enumerate(data_rows):
        sheet_row = DATA_START_ROW + i   # 1-based row number on the sheet

        # Pad row if shorter than expected
        padded = row + [""] * max(0, COL_I + 1 - len(row))

        spreadsheet_id = padded[COL_F].strip()
        tab_name       = padded[COL_G].strip()
        current_i      = padded[COL_I].strip()

        if not spreadsheet_id or not tab_name:
            continue   # nothing to do for this row

        if current_i:
            logging.info(
                "Row %d: I already filled ('%s') — skipping.", sheet_row, current_i
            )
            continue

        try:
            gid = fetch_gid(client, spreadsheet_id, tab_name, cache)
        except Exception as e:
            logging.error(
                "Row %d: failed to fetch GID for %s / '%s': %s",
                sheet_row, spreadsheet_id, tab_name, e,
            )
            continue

        if gid is not None:
            logging.info(
                "Row %d: %s / '%s' -> GID %s", sheet_row, spreadsheet_id, tab_name, gid
            )
            updates.append((sheet_row, gid))
        else:
            logging.warning(
                "Row %d: tab '%s' not found in %s — column I left blank.",
                sheet_row, tab_name, spreadsheet_id,
            )

    if not updates:
        logging.info("Nothing to update — all rows already filled or no matches found.")
        return

    # Single batch update for all I-column cells
    cell_updates = [
        gspread.Cell(row=row_num, col=COL_I + 1, value=str(gid))
        for row_num, gid in updates
    ]
    ws.update_cells(cell_updates, value_input_option="RAW")
    logging.info(
        "Batch-wrote GIDs for %d row(s): rows %s",
        len(updates), ", ".join(str(r) for r, _ in updates),
    )


# ==========================
# Entry point
# ==========================

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    from script_logger import log_start, log_end
    ctx = log_start("populate_gids")
    try:
        run()
    finally:
        log_end(ctx)


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted.")
        raise SystemExit(130)
