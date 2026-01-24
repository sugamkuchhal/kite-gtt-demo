import gspread
import argparse
import time
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path

CREDS_PATH = str(get_creds_path())

def load_sheet(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scope)
    client = gspread.authorize(creds)
    return client.open(sheet_name)

def copy_columns(sheet, src_col_start, src_col_end, dst_col_start, dst_col_end, nrows):
    src_range = f"{src_col_start}1:{src_col_end}{nrows}"
    dst_range = f"{dst_col_start}1:{dst_col_end}{nrows}"
    values = sheet.get_values(src_range)
    if values:
        sheet.update(range_name = dst_range, values = values, value_input_option='USER_ENTERED')
        print(f"Copied {src_range} → {dst_range} ({len(values)} rows)")

def central_buy_update(action_sheet, special_target_sheet, filter_col_letter="O", dest_col_letter="J", uncheck=False):
    # Clear the destination column in the special target sheet
    special_target_sheet.batch_clear([f"{dest_col_letter}2:{dest_col_letter}"])
    action_data = action_sheet.get_all_values()
    if len(action_data) < 2:
        print("⚠️ No data in Action_List.")
        return

    data = action_data[1:]  # skip header
    col_idx = ord(filter_col_letter.upper()) - ord('A')
    filtered_rows = []
    if uncheck:
        # Take all rows with non-empty column A (symbol/code), skip blanks
        filtered_rows = [[row[0]] for row in data if row and row[0].strip()]
    else:
        # Take all rows where column O contains "buy" (case-insensitive)
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
        print(f"Copied {len(filtered_rows)} BUY rows to {special_target_sheet.title}.{dest_col_letter}")
    else:
        print("⚠️ No rows with BUY found.")

def mkt_kwk_ops_sort_email(
    main_sheet_file,
    kwk_sheet_name,
    action_sheet_name,
    special_target_sheet_file,
    special_target_sheet_name,
    uncheck=False
):
    # Open main and special target sheets (different files)
    main_sheet = load_sheet(main_sheet_file)
    special_target_sheet_book = load_sheet(special_target_sheet_file)

    kwk_sheet = main_sheet.worksheet(kwk_sheet_name)
    action_sheet = main_sheet.worksheet(action_sheet_name)
    special_target_sheet = special_target_sheet_book.worksheet(special_target_sheet_name)

    # --- TOUCH A CELL IN EACH WORKSHEET TO FORCE RECALC ---
    for ws, name in [(kwk_sheet, "KWK"), (action_sheet, "Action_List"), (special_target_sheet, "Special_Target")]:
        try:
            val = ws.acell("A1").value
            ws.update_acell("A1", val)
            print(f"TOUCH: Triggered formula recalc for {name} Sheet.")
        except Exception as e:
            print(f"TOUCH: Could not touch A1 in {name} Sheet: {e}")

    print("WAIT: Sleeping 10 seconds for Sheets to refresh/recalculate.")
    time.sleep(10)

    nrows = len(kwk_sheet.get_all_values())
    # Step 1: S:X → AH:AM
    copy_columns(kwk_sheet, "S", "X", "AH", "AM", nrows)
    time.sleep(1)

    # Step 2: D:I → S:X
    copy_columns(kwk_sheet, "D", "I", "S", "X", nrows)
    time.sleep(1)

    # Step 3: Central BUY update (cross-sheet)
    central_buy_update(action_sheet, special_target_sheet, filter_col_letter="O", dest_col_letter="J", uncheck=uncheck)
    time.sleep(1)

    print("✅ All operations complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KWK Ops Sort Email Script (cross-sheet)")
    parser.add_argument("--sheet-name", required=True, help="Main Google Sheet file name")
    parser.add_argument("--kwk-sheet", required=True, help="KWK sheet/tab name (in main file)")
    parser.add_argument("--action-sheet", required=True, help="Action_List sheet/tab name (in main file)")
    parser.add_argument("--special-target-sheet-file", required=True, help="Special target Google Sheet file name")
    parser.add_argument("--special-target-sheet", required=True, help="Special target sheet/tab name (in special file)")
    parser.add_argument("--uncheck", action="store_true", help="Skip BUY filter, copy all non-empty A")
    args = parser.parse_args()

    mkt_kwk_ops_sort_email(
        main_sheet_file=args.sheet_name,
        kwk_sheet_name=args.kwk_sheet,
        action_sheet_name=args.action_sheet,
        special_target_sheet_file=args.special_target_sheet_file,
        special_target_sheet_name=args.special_target_sheet,
        uncheck=args.uncheck
    )
