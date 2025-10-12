import gspread
from google.oauth2.service_account import Credentials

CREDS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"
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

    # Find first empty row in column A in destination
    dest_colA = ws_dest.col_values(1)
    first_empty_row = len(dest_colA) + 1  # 1-based index
    for idx, val in enumerate(dest_colA, 1):
        if not val.strip():
            first_empty_row = idx
            break

    # Prepare the range to update in destination
    num_rows = len(data_rows)
    cell_list = ws_dest.range(
        f"A{first_empty_row}:H{first_empty_row + num_rows - 1}"
    )

    # Flatten data_rows to fit into cell_list (row by row, col by col)
    flat_values = [cell for row in data_rows for cell in row]
    for cell, value in zip(cell_list, flat_values):
        cell.value = value

    # Update the destination sheet
    ws_dest.update_cells(cell_list, value_input_option="USER_ENTERED")
    print(f"✅ Copied {num_rows} rows from {SRC_TAB} to {DEST_TAB} starting at row {first_empty_row}.")

if __name__ == "__main__":
    main()
