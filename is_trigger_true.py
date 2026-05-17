#!/usr/bin/env python3
import logging

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

REF_SHEETS = "PORTFOLIO"
RANGE = "ALL_OLD_GTTs!R1"

def is_trigger_true():
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file(str(get_creds_path()), scopes=scopes)
        gc = gspread.authorize(creds)
        sheet_id = resolve_sheet_id(REF_SHEETS)
        sheet = gc.open_by_key(sheet_id)
        result = sheet.values_get(RANGE, params={"valueRenderOption": "FORMATTED_VALUE"})
        value = result.get("values", [[""]])[0][0]

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
