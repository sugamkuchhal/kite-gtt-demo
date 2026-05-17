import gspread
from google.oauth2.service_account import Credentials
import argparse

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

CREDS_PATH = str(get_creds_path())

def load_sheet(ref_sheets):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scope)
    client = gspread.authorize(creds)
    sheet_id = resolve_sheet_id(ref_sheets)
    return client.open_by_key(sheet_id)

def prepare_feed_list(ref_sheets, source_tab, dest_tab):
    print(f"")
    print(f"⚙️  Preparing feed list from '{ref_sheets}'")
    print(f"")
    print(f"⚙️  Preparing feed list from '{source_tab}' ➡️ '{dest_tab}'")

    sheet = load_sheet(ref_sheets)
    source_ws = sheet.worksheet(source_tab)
    dest_ws = sheet.worksheet(dest_tab)

    # 👉 TOUCH CELL to force sheet refresh/recalc
    try:
        val = source_ws.acell("A1").value
        source_ws.update_acell("A1", val)
        print("🔄 Touched A1 to trigger formula recalc.")
    except Exception as e:
        print(f"⚠️  Could not touch A1: {e}")

    # 👉 WAIT 10 seconds before starting further processing
    import time
    print("⏳ Waiting 10 seconds for recalculation/refresh...")
    time.sleep(10)

    # STEP 1: Copy rows where Column D starts with "Copy" → append A, B, C to dest
    source_data = source_ws.get_all_values()[2:]  # Skip 2 header rows
    copy_rows = [row[:3] for row in source_data if len(row) > 3 and row[3].startswith("Copy")]

    if copy_rows:
        dest_ws.append_rows(copy_rows, value_input_option='USER_ENTERED')
        print(f"🧹 Step 1: Appended {len(copy_rows)} 'Copy' rows to destination.")
    else:
        print("⚠️  Step 1: No 'Copy' rows found.")

    # STEP 2: Sort destination by Column B (ticker) first, then Column A (timestamp)
    dest_ws.sort((2, 'asc'), (1, 'asc'))  # 2 = Column B, 1 = Column A
    print("🔀 Step 2: Sorted destination by Ticker (B), then Timestamp (A).")

    # STEP 3: Deduplicate based on Column B (ticker)
    dest_data = dest_ws.get_all_values()[1:]  # Skip 1 header row
    seen = set()
    deduped_rows = []
    for row in dest_data:
        if len(row) < 2:
            continue
        ticker = row[1]
        if ticker not in seen:
            seen.add(ticker)
            deduped_rows.append(row[:3])  # Only A, B, C

    # Overwrite destination sheet from row 2 (only A-C columns)
    if deduped_rows:
        dest_ws.batch_clear([f"A2:C{len(dest_data)+1}"])
        dest_ws.update(range_name="A2", values=deduped_rows, value_input_option='USER_ENTERED')
        print(f"🗑️  Step 3: Removed duplicates by Ticker. Remaining rows: {len(deduped_rows)}")
    else:
        dest_ws.batch_clear([f"A2:C{len(dest_data)+1}"])
        print(f"🗑️  Step 3: Destination emptied after deduplication.")

    # STEP 4: Remove rows whose tickers match "Remove" in source sheet
    remove_tickers = [row[1] for row in source_data if len(row) > 3 and row[3].startswith("Remove")]
    if remove_tickers:
        # Read current rows again
        current_data = dest_ws.get_all_values()[1:]
        filtered_rows = [row[:3] for row in current_data if row[1] not in remove_tickers]
        dest_ws.batch_clear([f"A2:C{len(current_data)+1}"])
        if filtered_rows:
            dest_ws.update(range_name="A2", values=filtered_rows, value_input_option='USER_ENTERED')
        print(f"🗑️  Step 4: Removed {len(current_data) - len(filtered_rows)} rows matching 'Remove' tickers.")
    else:
        print("⚠️  Step 4: No 'Remove' tickers found.")

    # STEP 5: Final sort by Column C (Source), then Column B (Ticker)
    dest_ws.sort((3, 'asc'), (2, 'asc'))  # 3 = Column C, 2 = Column B
    print("🔀 Step 5: Final sort by Source (C), then Ticker (B).")

    print("✅ Feed list preparation complete.")
    print("")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare Feed List from Google Sheet tabs.")
    parser.add_argument("--ref-sheets", required=True, help="Resolver key from ref_sheets.json")
    parser.add_argument("--source-sheet", required=True, help="Source tab name")
    parser.add_argument("--dest-sheet", required=True, help="Destination tab name")
    args = parser.parse_args()

    prepare_feed_list(
        ref_sheets=args.ref_sheets,
        source_tab=args.source_sheet,
        dest_tab=args.dest_sheet
    )

    # ------------------ POST-CHECKS: specific cells & simple prints ------------------
    def _check_cell_and_log(spreadsheet, tab_name, cell_addr, friendly_name=None):
        """
        Read spreadsheet.worksheet(tab_name).acell(cell_addr).value and print:
         - ✅ message if value == "0"
         - ❌ message otherwise (including errors)
        """
        if friendly_name is None:
            friendly_name = f"{tab_name}!{cell_addr}"
    
        try:
            try:
                ws = spreadsheet.worksheet(tab_name)
            except Exception as e:
                print(f"❌ Could not open worksheet '{tab_name}' to check {friendly_name}: {e}")
                return
    
            try:
                val = ws.acell(cell_addr).value
            except Exception as e:
                print(f"❌ Could not read cell {friendly_name}: {e}")
                return
    
            # Normalize and compare to string "0"
            val_norm = (str(val).strip() if val is not None else "")
            if val_norm == "0":
                print(f"✅ Post-check passed: {friendly_name} = 0 → Process completed successfully")
            else:
                print(f"❌ Post-check failed: {friendly_name} = {val_norm or '<EMPTY/None>'} → Process not completed")
    
        except Exception as e:
            print(f"❌ Unexpected error while checking {friendly_name}: {e}")
    
    # Resolve spreadsheet explicitly from the CLI sheet name (Option A)
    try:
        spreadsheet = load_sheet(args.ref_sheets)
    except Exception as e:
        spreadsheet = None
        print(f"❌ Could not open spreadsheet for ref-sheets '{args.ref_sheets}' for post-checks: {e}")
    
    if spreadsheet is None:
        print("❌ Could not resolve Spreadsheet object for post-checks. Skipping post-checks.")
    else:
        # The checks to run (each checks J1 in the target feed-list worksheet)
        _check_cell_and_log(spreadsheet, "US_FEED_LIST", "J1", "US_FEED_LIST!J1")

    import time
    time.sleep(60)
