import argparse
import time

from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import get_gsheet_client, open_spreadsheet


def load_sheet(algo_name):
    client = get_gsheet_client()
    return open_spreadsheet(client, spreadsheet_id=get_sheet_id(algo_name))

def central_buy_update(action_sheet, special_target_sheet, filter_col_letter="O", dest_col_letter="I", uncheck=False):
    special_target_sheet.batch_clear([f"{dest_col_letter}2:{dest_col_letter}"])
    action_data = action_sheet.get_all_values()
    if len(action_data) < 2:
        print("⚠️ No data in Action_List.")
        return

    data = action_data[1:]  # skip header
    col_idx = ord(filter_col_letter.upper()) - ord('A')

    if uncheck:
        # Copy all rows with non-empty column A
        filtered_rows = [[row[0]] for row in data if row and row[0].strip()]
    else:
        # Copy rows where column O contains "buy" (case-insensitive) and A is non-empty
        filtered_rows = [
            [row[0]]
            for row in data
            if len(row) > col_idx and "buy" in str(row[col_idx]).lower() and row[0].strip()
        ]

    if filtered_rows:
        special_target_sheet.update(
            f"{dest_col_letter}2:{dest_col_letter}{len(filtered_rows)+1}",
            filtered_rows,
            value_input_option='USER_ENTERED'
        )
        print(f"Copied {len(filtered_rows)} rows to {special_target_sheet.title}.{dest_col_letter}")
    else:
        print("⚠️ No eligible rows found.")

def mkt_kwk_ops_sort_email(
    main_sheet_file,
    action_sheet_name,
    special_target_sheet_file,
    special_target_sheet_name,
    uncheck=False
):
    # Open main and special target sheets (different files)
    main_sheet = load_sheet(main_sheet_file)
    special_target_sheet_book = load_sheet(special_target_sheet_file)

    action_sheet = main_sheet.worksheet(action_sheet_name)
    special_target_sheet = special_target_sheet_book.worksheet(special_target_sheet_name)

    # --- TOUCH A CELL IN EACH WORKSHEET TO FORCE RECALC ---
    for ws, name in [(action_sheet, "Action_List"), (special_target_sheet, "Special_Target")]:
        try:
            val = ws.acell("A1").value
            ws.update_acell("A1", val)
            print(f"TOUCH: Triggered formula recalc for {name} Sheet.")
        except Exception as e:
            print(f"TOUCH: Could not touch A1 in {name} Sheet: {e}")

    print("WAIT: Sleeping 10 seconds for Sheets to refresh/recalculate.")
    time.sleep(10)

    # Step 3: Central BUY update (cross-sheet)
    central_buy_update(
        action_sheet,
        special_target_sheet,
        filter_col_letter="O",
        dest_col_letter="I",
        uncheck=uncheck
    )
    time.sleep(1)

    print("✅ All operations complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Central BUY Update Script (cross-sheet)")
    parser.add_argument("--algo-name", required=True, help="Main Google Sheet file name")
    parser.add_argument("--action-sheet", required=True, help="Action_List sheet/tab name (in main file)")
    parser.add_argument("--special-target-algo-name", required=True, help="Special target Google Sheet file name")
    parser.add_argument("--special-target-sheet", required=True, help="Special target sheet/tab name (in special file)")
    parser.add_argument("--uncheck", action="store_true", help="If set, disables column O filtering and copies all non-empty A")
    args = parser.parse_args()

    mkt_kwk_ops_sort_email(
        main_sheet_file=args.algo_name,
        action_sheet_name=args.action_sheet,
        special_target_sheet_file=args.special_target_algo_name,
        special_target_sheet_name=args.special_target_sheet,
        uncheck=args.uncheck
    )
