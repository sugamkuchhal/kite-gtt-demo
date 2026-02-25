from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import get_gsheet_client, open_worksheet

ALGO_NAME = "GTT_MASTER"
TAB_NAME = "ALL_OLD_GTTs"
CELL = "R1"


def main():
    client = get_gsheet_client()
    ws = open_worksheet(client, TAB_NAME, spreadsheet_id=get_sheet_id(ALGO_NAME))
    ws.update(range_name=CELL, values=[["FALSE"]], value_input_option="USER_ENTERED")
    print(f"Updated {CELL} in {TAB_NAME} to FALSE")


if __name__ == "__main__":
    main()
