import gspread
from google.oauth2.service_account import Credentials
from tqdm import tqdm
from datetime import datetime
import time

# --- CONFIGURATION ---
CREDS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"
SRC_SHEET = "Algo Master Data Bank"
SRC_TAB = "BANK_FINAL"
DEST_SHEET = "Algo Master Data Calculator"
DEST_TAB = "BANK_FINAL"
COL_RANGE = "A:F"
BATCH_SIZE = 500
MAX_RETRIES = 3

def main():

    start_time = datetime.now()
    print(f"")
    print(f"[DATE BANK TELEPORTER PROCESS START] {start_time.strftime('%Y-%m-%d %H:%M:%S')} - Starting process_and_update")

    # --- Authenticate Google Sheets ---
    creds = Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    gc = gspread.authorize(creds)

    # --- Open worksheets ---
    try:
        sh_src = gc.open(SRC_SHEET)
        ws_src = sh_src.worksheet(SRC_TAB)
    except Exception as e:
        print(f"❌ Error opening source: {e}")
        return

    try:
        sh_dest = gc.open(DEST_SHEET)
        ws_dest = sh_dest.worksheet(DEST_TAB)
    except Exception as e:
        print(f"❌ Error opening destination: {e}")
        return

    def safe_get(ws, col_range, max_retries=3):
        for attempt in range(1, max_retries+1):
            try:
                return ws.get(col_range)
            except Exception as e:
                print(f"⚠️  Failed to read ({col_range}) on attempt {attempt}: {e}")
                time.sleep(2 * attempt)
                if attempt == max_retries:
                    raise
    
    # --- Read source data (skip header) ---
    src_data = safe_get(ws_src, COL_RANGE)
    if not src_data or len(src_data) <= 1:
        print("❌ No data to copy (or only header present) in source.")
        return
    src_data = src_data[1:]  # Skip header
    
    # --- Read destination data (skip header) ---
    dest_data = safe_get(ws_dest, COL_RANGE)
    dest_data = dest_data[1:] if dest_data and len(dest_data) > 1 else []

    # --- Build set of existing (A,B) keys in dest ---
    dest_keys = set()
    for row in dest_data:
        a = row[0].strip() if len(row) > 0 and row[0] else ""
        b = row[1].strip() if len(row) > 1 and row[1] else ""
        if a and b:
            dest_keys.add((a, b))

    # --- Identify incremental rows in source (not present in dest) ---
    new_rows = []
    for row in src_data:
        a = row[0].strip() if len(row) > 0 and row[0] else ""
        b = row[1].strip() if len(row) > 1 and row[1] else ""
        if a and b and (a, b) not in dest_keys:
            # Pad to 6 columns
            row_padded = row[:6] + [""] * (6 - len(row[:6]))
            new_rows.append(row_padded)

    if not new_rows:
        print("✅ No new incremental rows to append. Destination up to date.")
        return

    print(f"🔍 Found {len(new_rows)} new incremental rows to append.")

    # --- Ensure enough rows in destination sheet (expand if necessary) ---
    dest_row_count = len(dest_data) + 1  # +1 for header row
    total_needed_rows = dest_row_count + len(new_rows)
    current_sheet_rows = ws_dest.row_count
    if total_needed_rows > current_sheet_rows:
        ws_dest.add_rows(total_needed_rows - current_sheet_rows)

    # --- Calculate starting append row (after last dest data row + header) ---
    append_start_row = dest_row_count + 1  # 1-based indexing, skip header

    # --- Write new rows in batches with progress bar ---
    print("⬇️  Appending rows in batches...")
    for i in tqdm(range(0, len(new_rows), BATCH_SIZE), desc="Copying", ncols=80):
        batch = new_rows[i:i+BATCH_SIZE]
        start_row = append_start_row + i
        end_row = start_row + len(batch) - 1
        rng = f"A{start_row}:F{end_row}"
        retries = 0
        while retries < MAX_RETRIES:
            try:
                ws_dest.update(
                    batch,
                    range_name=rng,
                    value_input_option='USER_ENTERED'
                )
                break  # Batch success
            except Exception as e:
                retries += 1
                print(f"⚠️  Batch {i//BATCH_SIZE+1} ({start_row}-{end_row}) failed (attempt {retries}): {e}")
                time.sleep(2 * retries)
                if retries >= MAX_RETRIES:
                    print(f"❌ Batch {i//BATCH_SIZE+1} ({start_row}-{end_row}) failed permanently. Rows not copied.")

    print("✅ Copy complete. All new rows appended.")

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    print(f"[DATE BANK TELEPORTER PROCESS END]✅  {end_time.strftime('%Y-%m-%d %H:%M:%S')} - Finished process_and_update (duration: {elapsed:.2f}s)")
    print(f"")

if __name__ == "__main__":
    main()
