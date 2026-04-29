import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("append_new_orders")
atexit.register(log_end, _RUN_CTX)
CREDS_PATH = str(get_creds_path())
ref_sheets = "PORTFOLIO"
tab_name_src = "LATEST_ORDERS"
tab_name_dest = "NEW_ORDERS"
SRC_RANGE = "A:H"  # covers columns A to H

def main():
    creds = Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    gc = gspread.authorize(creds)

    sheet_id = resolve_sheet_id(ref_sheets)

    # Open source and destination worksheet (same file)
    sh = gc.open_by_key(sheet_id)
    ws_src = sh.worksheet(tab_name_src)
    ws_dest = sh.worksheet(tab_name_dest)

    # Fetch all values from source (A:H)
    src_data = ws_src.get(SRC_RANGE)
    if not src_data or len(src_data) <= 1:
        print("No data to copy from source (or only header present).")
        return

    # Exclude header row (row 0)
    data_rows = src_data[1:]

    # Append all non-header rows to the destination in one call.
    ws_dest.append_rows(data_rows, value_input_option="USER_ENTERED")
    print(f"✅ Appended {len(data_rows)} rows from {tab_name_src} to {tab_name_dest}.")

if __name__ == "__main__":
    main()
