#!/usr/bin/env python3
import logging
import argparse

from algo_sheets_lookup import get_sheet_id
# Use the Google Sheets helpers
from google_sheets_utils import get_gsheet_client, open_worksheet, read_rows_from_sheet

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


def get_instructions_sheet(sheet_id=None, tab_name=None, sheet_name=None):
    if sheet_id is None:
        algo_name = getattr(config, "INSTRUCTION_ALGO_NAME", None)
        sheet_id = get_sheet_id(algo_name) if algo_name else None
    # Backward compatibility: allow legacy sheet_name kwarg, but prefer tab_name.
    if tab_name is None and sheet_name is not None:
        tab_name = sheet_name
    if tab_name is None:
        tab_name = getattr(config, "INSTRUCTION_TAB_NAME", None)

    if not sheet_id or not tab_name:
        raise ValueError("Please provide sheet_id and tab_name (or set them in config)")

    client = get_gsheet_client()
    sheet = open_worksheet(client, tab_name, spreadsheet_id=sheet_id)

    logging.info(f"Accessed instructions sheet: {tab_name}")
    return sheet

def _get_from_args_or_config(arg_value, cfg_obj, attr_name, default=None):
    if arg_value is not None:
        return arg_value
    if cfg_obj is not None and hasattr(cfg_obj, attr_name):
        return getattr(cfg_obj, attr_name)
    return default

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch GTT instructions. CLI flags override config values.")
    parser.add_argument("--sheet-id", dest="sheet_id", help="Instruction sheet ID (overrides lookup/config default)", type=str)
    parser.add_argument(
        "--tab-name",
        "--sheet-name",
        dest="tab_name",
        help="Instruction TAB_NAME (overrides config.INSTRUCTION_TAB_NAME)",
        type=str,
    )
    parser.add_argument("--start-row", dest="start_row", help="1-based start row for fetching (default: 2)", type=int, default=2)

    args = parser.parse_args()

    sheet_id = args.sheet_id if args.sheet_id is not None else get_sheet_id(getattr(config, "INSTRUCTION_ALGO_NAME", None))
    tab_name = _get_from_args_or_config(args.tab_name, config, "INSTRUCTION_TAB_NAME")
    start_row = args.start_row if args.start_row and args.start_row > 0 else 2

    missing = []
    if not sheet_id:
        missing.append("sheet-id (or config.INSTRUCTION_ALGO_NAME lookup)")
    if not tab_name:
        missing.append("tab-name (or config.INSTRUCTION_TAB_NAME)")
    if missing:
        parser.error("Missing required parameters: " + ", ".join(missing))

    logging.info(f"Using sheet_id={sheet_id}, tab_name={tab_name}, start_row={start_row}, batch_size={BATCH_SIZE}")

    sheet = get_instructions_sheet(sheet_id, tab_name)

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
