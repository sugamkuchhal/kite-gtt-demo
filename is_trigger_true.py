#!/usr/bin/env python3
import logging

from google_sheets_utils import get_gsheet_client, gsheets_retry
from ref_sheets_utils import resolve_sheet_id

REF_SHEETS = "PORTFOLIO"
RANGE = "ALL_OLD_GTTs!R1"

def is_trigger_true():
    try:
        gc = get_gsheet_client()
        sheet_id = resolve_sheet_id(REF_SHEETS)
        ws = gc.open_by_key(sheet_id).worksheet("ALL_OLD_GTTs")
        value = gsheets_retry(ws.acell, "R1").value

        return str(value).strip().lower() == "true"
    except Exception:
        return False

if __name__ == "__main__":
    try:
        print(is_trigger_true())
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
    except Exception:
        logging.exception("is_trigger_true failed.")
        raise SystemExit(1)
