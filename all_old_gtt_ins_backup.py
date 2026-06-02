#!/usr/bin/env python3
"""Back up ALL_OLD_GTT_INS rows into ALL_OLD_GTT_INS_BACKUP as values."""

import argparse
import logging

import gspread

from google_sheets_utils import get_gsheet_client
from ref_sheets_utils import resolve_sheet_id
from script_logger import log_end, log_start

REF_SHEETS = "PORTFOLIO"
SOURCE_TAB = "ALL_OLD_GTT_INS"
BACKUP_TAB = "ALL_OLD_GTT_INS_BACKUP"
HEADER_ROW = 1
DATA_START_ROW = HEADER_ROW + 1
DEFAULT_BACKUP_ROWS = 1000

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def _column_number_to_letter(col_num):
    """Convert a 1-based column number to a Google Sheets column letter."""
    if col_num < 1:
        raise ValueError("Column number must be greater than zero.")

    letters = ""
    while col_num:
        col_num, remainder = divmod(col_num - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _worksheet_or_create(spreadsheet, title, rows, cols):
    """Return an existing worksheet, or create it when missing."""
    try:
        return spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        logging.info("Worksheet %s not found. Creating backup worksheet.", title)
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def _max_column_count(rows):
    """Return the widest row length, defaulting to one column for empty sheets."""
    return max((len(row) for row in rows), default=1)


def _ensure_worksheet_size(worksheet, rows, cols):
    """Grow a worksheet when needed so batch copy ranges fit."""
    target_rows = max(rows, worksheet.row_count)
    target_cols = max(cols, worksheet.col_count)
    if target_rows != worksheet.row_count or target_cols != worksheet.col_count:
        logging.info(
            "Resizing %s to %s rows x %s columns.",
            worksheet.title,
            target_rows,
            target_cols,
        )
        worksheet.resize(rows=target_rows, cols=target_cols)


def backup_all_old_gtt_ins(ref_sheets=REF_SHEETS, source_tab=SOURCE_TAB, backup_tab=BACKUP_TAB):
    """
    Copy ALL_OLD_GTT_INS data rows to ALL_OLD_GTT_INS_BACKUP as static values.

    The backup sheet's header row is preserved. All backup contents from row 2
    onward are cleared across the worksheet's columns before source rows from row
    2 onward are copied with Google Sheets' native PASTE_FORMAT and PASTE_VALUES
    operations. That keeps formula results as typed values while also carrying
    source number/date formats, so values do not get converted into text values
    prefixed with an apostrophe or displayed with stale backup formatting.
    """
    sheet_id = resolve_sheet_id(ref_sheets)
    client = get_gsheet_client()
    spreadsheet = client.open_by_key(sheet_id)

    source_ws = spreadsheet.worksheet(source_tab)
    source_values = source_ws.get_all_values(value_render_option="UNFORMATTED_VALUE")
    data_row_count = max(len(source_values) - HEADER_ROW, 0)
    source_col_count = _max_column_count(source_values)

    backup_rows = max(DEFAULT_BACKUP_ROWS, len(source_values), DATA_START_ROW)
    backup_ws = _worksheet_or_create(
        spreadsheet,
        backup_tab,
        rows=str(backup_rows),
        cols=str(source_col_count),
    )
    _ensure_worksheet_size(backup_ws, backup_rows, source_col_count)

    clear_col_count = max(backup_ws.col_count, source_col_count, 1)
    clear_end_col = _column_number_to_letter(clear_col_count)
    clear_range = f"A{DATA_START_ROW}:{clear_end_col}{backup_ws.row_count}"

    logging.info("Clearing %s!%s", backup_tab, clear_range)
    backup_ws.batch_clear([clear_range])

    if data_row_count:
        logging.info(
            "Copying %s data rows from %s to %s as native Sheets values.",
            data_row_count,
            source_tab,
            backup_tab,
        )
        source_range = {
            "sheetId": source_ws.id,
            "startRowIndex": DATA_START_ROW - 1,
            "endRowIndex": DATA_START_ROW - 1 + data_row_count,
            "startColumnIndex": 0,
            "endColumnIndex": source_col_count,
        }
        destination_range = {
            "sheetId": backup_ws.id,
            "startRowIndex": DATA_START_ROW - 1,
            "endRowIndex": DATA_START_ROW - 1 + data_row_count,
            "startColumnIndex": 0,
            "endColumnIndex": source_col_count,
        }
        spreadsheet.batch_update(
            {
                "requests": [
                    {
                        "copyPaste": {
                            "source": source_range,
                            "destination": destination_range,
                            "pasteType": "PASTE_FORMAT",
                            "pasteOrientation": "NORMAL",
                        }
                    },
                    {
                        "copyPaste": {
                            "source": source_range,
                            "destination": destination_range,
                            "pasteType": "PASTE_VALUES",
                            "pasteOrientation": "NORMAL",
                        }
                    },
                ]
            }
        )
    else:
        logging.info("No data rows found in %s from row %s onward.", source_tab, DATA_START_ROW)

    return data_row_count


def main():
    parser = argparse.ArgumentParser(
        description="Back up ALL_OLD_GTT_INS data rows to ALL_OLD_GTT_INS_BACKUP as values."
    )
    parser.add_argument("--ref-sheets", default=REF_SHEETS, help="ref_sheets key to resolve spreadsheet ID")
    parser.add_argument("--source-tab", default=SOURCE_TAB, help="source worksheet tab name")
    parser.add_argument("--backup-tab", default=BACKUP_TAB, help="backup worksheet tab name")
    args = parser.parse_args()

    copied = backup_all_old_gtt_ins(
        ref_sheets=args.ref_sheets,
        source_tab=args.source_tab,
        backup_tab=args.backup_tab,
    )
    logging.info("✅ Backup completed. Copied %s rows.", copied)


def run_cli():
    ctx = log_start("ALL_OLD_GTT_INS_BACKUP")
    try:
        main()
        return 0
    except Exception:
        logging.exception("ALL_OLD_GTT_INS_BACKUP failed.")
        return 1
    finally:
        log_end(ctx)


if __name__ == "__main__":
    raise SystemExit(run_cli())
