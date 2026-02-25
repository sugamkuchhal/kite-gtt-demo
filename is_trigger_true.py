#!/usr/bin/env python3

from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import DEFAULT_READONLY_SCOPES, get_gsheet_client, open_worksheet

ALGO_NAME = "GTT_MASTER"
TAB_NAME = "ALL_OLD_GTTs"
CELL = "R1"

def is_trigger_true():
    try:
        client = get_gsheet_client(scopes=DEFAULT_READONLY_SCOPES)
        ws = open_worksheet(client, TAB_NAME, spreadsheet_id=get_sheet_id(ALGO_NAME))
        value = ws.acell(CELL).value
        return str(value).strip().lower() == "true"
    except Exception:
        return False

if __name__ == "__main__":
    print(is_trigger_true())
