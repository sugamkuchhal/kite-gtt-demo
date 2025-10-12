#!/usr/bin/env python3
import logging
import argparse
from google_sheets_utils import get_gsheet_client, read_rows_from_sheet

# --- Batch size: single source of truth from config.py ---
try:
    import config
except Exception as e:
    raise SystemExit("Missing required module `config`. Please provide config.py with BATCH_SIZE defined.") from e

if not hasattr(config, "BATCH_SIZE"):
    raise SystemExit("config.BATCH_SIZE is not defined. Set BATCH_SIZE in config.py (no fallback).")

try:
    BATCH_SIZE = int(config.BATCH_SIZE)
    if BATCH_SIZE <= 0:
        raise ValueError("BATCH_SIZE must be a positive integer")
except Exception as e:
    raise SystemExit(f"config.BATCH_SIZE is invalid: {e}")
# --- End batch size setup ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def fetch_existing_gtts_batch(sheet, start_row):
    """
    Reads up to BATCH_SIZE raw rows starting from `start_row` (1-based).
    Returns a tuple: (raw_records, filtered_records)
    - raw_records: list of row dicts as returned by read_rows_from_sheet (may contain empty rows)
    - filtered_records: list of row dicts where at least one cell is non-empty
    """
    effective_batch = BATCH_SIZE

    # Count total rows in sheet (expensive for very large sheets, but retains prior behavior)
    num_rows = effective_batch
    raw_records = read_rows_from_sheet(sheet, start_row=start_row, num_rows=num_rows, as_dict=True)
    if not raw_records:
        return [], []
    records = raw_records

    # Filter out rows that are entirely empty
    filtered_records = [row for row in raw_records if any(str(v).strip() for v in row.values())]
    logging.info(f"Fetched {len(filtered_records)} existing GTT records from row {start_row} (requested {num_rows}, raw_returned {len(raw_records)})")
    return raw_records, filtered_records


def get_tracking_sheet(sheet_id=None, sheet_name=None):
    if sheet_id is None:
        sheet_id = getattr(config, "DATA_MANAGEMENT_SHEET_ID", None)
    if sheet_name is None:
        sheet_name = getattr(config, "DATA_MANAGEMENT_SHEET_NAME", None)

    if not sheet_id or not sheet_name:
        raise ValueError("sheet_id and sheet_name must be provided either as args or via config")

    client = get_gsheet_client()
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

    logging.info(f"Accessed GTT sheet: {sheet_name}")
    return sheet


def _get_from_args_or_config(arg_value, config_obj, attr_name, default=None):
    """
    Helper: prefer arg_value; if falsy, try config_obj.attr_name; else default.
    """
    if arg_value:
        return arg_value
    if config_obj is not None and hasattr(config_obj, attr_name):
        return getattr(config_obj, attr_name)
    return default

if __name__ == "__main__":
    # This script is intentionally non-interactive (no CLI args).
    start_row = 2

    try:
        sheet_id = config.DATA_MANAGEMENT_SHEET_ID
        sheet_name = config.DATA_MANAGEMENT_SHEET_NAME
    except Exception:
        logging.error("DATA_MANAGEMENT_SHEET_ID and DATA_MANAGEMENT_SHEET_NAME must be defined in config.py")
        raise SystemExit(1)

    logging.info(f"Using sheet_id={sheet_id}, sheet_name={sheet_name}, start_row={start_row}, batch_size={BATCH_SIZE}")

    sheet = get_tracking_sheet(sheet_id, sheet_name)
    raw_rows, filtered_rows = fetch_existing_gtts_batch(sheet, start_row)

    logging.info(f"Total raw rows returned: {len(raw_rows)}; filtered (non-empty) rows: {len(filtered_rows)}")
    for row in filtered_rows:
        print(row)
