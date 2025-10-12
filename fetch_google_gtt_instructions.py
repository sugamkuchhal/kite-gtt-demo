#!/usr/bin/env python3
import logging
import argparse

# Use the Google Sheets helpers
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

def fetch_gtt_instructions_batch(sheet, start_row):
    """
    Reads up to BATCH_SIZE raw rows starting from `start_row` (1-based).
    Returns (raw_instructions, filtered_instructions).
    """
    effective_batch = BATCH_SIZE

    raw_instructions = read_rows_from_sheet(sheet, start_row=start_row, num_rows=effective_batch, as_dict=True)
    filtered_instructions = [row for row in raw_instructions if any(str(v).strip() for v in row.values())]

    logging.info(f"Fetched {len(filtered_instructions)} instructions from row {start_row} (requested {effective_batch}, raw_returned {len(raw_instructions)})")
    return raw_instructions, filtered_instructions


def get_instructions_sheet(sheet_id=None, sheet_name=None):
    if sheet_id is None:
        sheet_id = getattr(config, "INSTRUCTION_SHEET_ID", None)
    if sheet_name is None:
        sheet_name = getattr(config, "INSTRUCTION_SHEET_NAME", None)

    if not sheet_id or not sheet_name:
        raise ValueError("Please provide sheet_id and sheet_name (or set them in config)")

    client = get_gsheet_client()
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)

    logging.info(f"Accessed instructions sheet: {sheet_name}")
    return sheet

def _get_from_args_or_config(arg_value, cfg_obj, attr_name, default=None):
    if arg_value is not None:
        return arg_value
    if cfg_obj is not None and hasattr(cfg_obj, attr_name):
        return getattr(cfg_obj, attr_name)
    return default

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch GTT instructions. CLI flags override config values.")
    parser.add_argument("--sheet-id", dest="sheet_id", help="Instruction sheet ID (overrides config.INSTRUCTION_SHEET_ID)", type=str)
    parser.add_argument("--sheet-name", dest="sheet_name", help="Instruction worksheet name (overrides config.INSTRUCTION_SHEET_NAME)", type=str)
    parser.add_argument("--start-row", dest="start_row", help="1-based start row for fetching (default: 2)", type=int, default=2)

    args = parser.parse_args()

    sheet_id = _get_from_args_or_config(args.sheet_id, config, "INSTRUCTION_SHEET_ID")
    sheet_name = _get_from_args_or_config(args.sheet_name, config, "INSTRUCTION_SHEET_NAME")
    start_row = args.start_row if args.start_row and args.start_row > 0 else 2

    missing = []
    if not sheet_id:
        missing.append("sheet-id (or config.INSTRUCTION_SHEET_ID)")
    if not sheet_name:
        missing.append("sheet-name (or config.INSTRUCTION_SHEET_NAME)")
    if missing:
        parser.error("Missing required parameters: " + ", ".join(missing))

    logging.info(f"Using sheet_id={sheet_id}, sheet_name={sheet_name}, start_row={start_row}, batch_size={BATCH_SIZE}")

    sheet = get_instructions_sheet(sheet_id, sheet_name)

    all_instructions = []
    cur_row = start_row
    while True:
        raw_batch, filtered_batch = fetch_gtt_instructions_batch(sheet, cur_row)
        raw_read = len(raw_batch)
        if raw_read == 0:
            break
        all_instructions.extend(filtered_batch)
        cur_row += raw_read
        if len(all_instructions) > 200000:
            logging.warning("Aborting fetch_all after 200k rows as safety limit")
            break

    logging.info(f"Total instructions fetched: {len(all_instructions)}")
    for r in all_instructions:
        print(r)
