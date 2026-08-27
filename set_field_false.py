import logging

from google_sheets_utils import get_gsheet_client, gsheets_retry
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("set_field_false")
atexit.register(log_end, _RUN_CTX)
REF_SHEETS = "PORTFOLIO"
SHEET_NAME = "ALL_OLD_GTTs"
CELL = "R1"

def main():
    gc = get_gsheet_client()
    spreadsheet_id = resolve_sheet_id(REF_SHEETS)
    ws = gc.open_by_key(spreadsheet_id).worksheet(SHEET_NAME)
    gsheets_retry(ws.update, [["FALSE"]], f"{CELL}")

    print(f"Updated {CELL} in {SHEET_NAME} to FALSE")

def run_cli():
    try:
        main()
        return 0
    except Exception:
        logging.exception("set_field_false failed.")
        return 1

if __name__ == "__main__":
    raise SystemExit(run_cli())
