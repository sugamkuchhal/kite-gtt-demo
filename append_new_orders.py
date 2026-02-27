from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import get_gsheet_client, open_spreadsheet
ALGO_NAME = "PORTFOLIO_STOCKS"
SRC_TAB = "LATEST_ORDERS"
DEST_TAB = "NEW_ORDERS"
SRC_RANGE = "A:H"  # covers columns A to H

def main():
    client = get_gsheet_client()

    # Open source and destination worksheet (same file)
    sh = open_spreadsheet(client, spreadsheet_id=get_sheet_id(ALGO_NAME))
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
