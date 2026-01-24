import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path

CREDS_PATH = str(get_creds_path())
SHEET_NAME = "SARAS Portfolio - Stocks"
SRC_TAB = "LATEST_ORDERS"
DEST_TAB = "NEW_ORDERS"
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

    # Open source and destination worksheet (same file)
    sh = gc.open(SHEET_NAME)
    ws_src = sh.worksheet(SRC_TAB)
    ws_dest = sh.worksheet(DEST_TAB)

    # Fetch all values from source (A:H)
    src_data = ws_src.get(SRC_RANGE)
    if not src_data or len(src_data) <= 1:
        print("No data to copy from source (or only header present).")
        return

    # Exclude header row (row 0)
    data_rows = src_data[1:]

    # Append all non-header rows to the destination in one call.
    ws_dest.append_rows(data_rows, value_input_option="USER_ENTERED")
    print(f"✅ Appended {len(data_rows)} rows from {SRC_TAB} to {DEST_TAB}.")

if __name__ == "__main__":
    main()
