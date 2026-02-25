import gspread
import argparse
import time

from algo_sheets_lookup import get_sheet_id
from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import DEFAULT_RW_SCOPES, get_gsheet_client, open_spreadsheet

# --- tiny retry helper (exponential backoff) for 429s on READ ops only ---
def _retry_read(fn, *args, max_tries=5, **kwargs):
    """
    Run a gspread READ call with exponential backoff on 429s.
    Does not affect write operations or logic.
    """
    delay = 1.0
    for attempt in range(1, max_tries + 1):
        try:
            return fn(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            if "429" in str(e):
                if attempt == max_tries:
                    raise
                print(f"⚠️  Hit 429 on {getattr(fn, '__name__', 'read')}, retrying in {delay:.1f}s (attempt {attempt}/{max_tries})")
                time.sleep(delay)
                delay *= 2
            else:
                raise

def load_sheet(algo_name):
    scope = list(DEFAULT_RW_SCOPES) + ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    client = get_gsheet_client(scopes=scope)
    return open_spreadsheet(client, spreadsheet_id=get_sheet_id(algo_name))

def prepare_feed_list(algo_name, source_tab, dest_tab):
    print("")
    print(f"⚙️  Preparing feed list from ALGO '{algo_name}'")
    print("")
    print(f"⚙️  Preparing feed list from '{source_tab}' ➡️ '{dest_tab}'")

    sheet = load_sheet(algo_name)
    source_ws = sheet.worksheet(source_tab)
    dest_ws = sheet.worksheet(dest_tab)

    # 👉 TOUCH CELL to force sheet refresh/recalc
    try:
        val = _retry_read(source_ws.acell, "A1").value
        source_ws.update_acell("A1", val)
        print("🔄 Touched A1 to trigger formula recalc.")
    except Exception as e:
        print(f"⚠️  Could not touch A1: {e}")

    # 👉 WAIT 10 seconds before starting further processing
    print("⏳ Waiting 10 seconds for recalculation/refresh...")
    time.sleep(10)

    # STEP 1: Copy rows where Column D starts with "Copy" → append A, B, C to dest
    # Read only A:D starting at row 3 (skip 2 header rows)
    source_data = _retry_read(source_ws.get, "A3:D", value_render_option="UNFORMATTED_VALUE") or []
    copy_rows = [row[:3] for row in source_data if len(row) > 3 and str(row[3]).startswith("Copy")]

    if copy_rows:
        dest_ws.append_rows(copy_rows, value_input_option='USER_ENTERED')
        print(f"🧹 Step 1: Appended {len(copy_rows)} 'Copy' rows to destination.")
    else:
        print("⚠️  Step 1: No 'Copy' rows found.")

    # STEP 2: Sort destination by Column B (ticker) first, then Column A (timestamp)
    dest_ws.sort((2, 'asc'), (1, 'asc'))  # 2 = Column B, 1 = Column A
    print("🔀 Step 2: Sorted destination by Ticker (B), then Timestamp (A).")

    # STEP 3: Deduplicate based on Column B (ticker)
    dest_data = _retry_read(dest_ws.get, "A2:C", value_render_option="UNFORMATTED_VALUE") or []
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
    dest_ws.batch_clear([f"A2:C{len(dest_data)+1}"])
    if deduped_rows:
        dest_ws.update(range_name="A2", values=deduped_rows, value_input_option='USER_ENTERED')
        print(f"🗑️  Step 3: Removed duplicates by Ticker. Remaining rows: {len(deduped_rows)}")
    else:
        print(f"🗑️  Step 3: Destination emptied after deduplication.")

    # STEP 4: Remove rows whose tickers match "Remove" in source sheet
    remove_tickers = [row[1] for row in source_data if len(row) > 3 and str(row[3]).startswith("Remove")]
    if remove_tickers:
        # Filter the already written (post-dedupe) rows we have in memory
        before = len(deduped_rows)
        filtered_rows = [row[:3] for row in deduped_rows if len(row) > 1 and row[1] not in remove_tickers]
        removed = before - len(filtered_rows)
        # Clear based on previous on-sheet length (dest_data) to avoid leftovers
        dest_ws.batch_clear([f"A2:C{len(dest_data)+1}"])
        if filtered_rows:
            dest_ws.update(range_name="A2", values=filtered_rows, value_input_option='USER_ENTERED')
        print(f"🗑️  Step 4: Removed {removed} rows matching 'Remove' tickers.")
    else:
        print("⚠️  Step 4: No 'Remove' tickers found.")

    # STEP 5: Final sort by Column C (Source), then Column B (Ticker)
    dest_ws.sort((3, 'asc'), (2, 'asc'))  # 3 = Column C, 2 = Column B
    print("🔀 Step 5: Final sort by Source (C), then Ticker (B).")

    print("✅ Feed list preparation complete.")
    time.sleep(60)
    print("")

# ------------------ POST-CHECKS: specific cells & simple prints ------------------
def _post_checks_batch(spreadsheet):
    """
    Batch read the six J1 cells across different worksheets in ONE API call,
    then log the same pass/fail messages as before.
    """
    ranges = [
        "SGST_FEED_LIST!J1",
        "VS_SGST_FEED_LIST!J1",
        "SUPER_FEED_LIST!J1",
        "VS_SUPER_FEED_LIST!J1",
        "TURTLE_FEED_LIST!J1",
        "VS_TURTLE_FEED_LIST!J1",
    ]
    try:
        # Correct portable call – no keyword arguments
        resp = _retry_read(spreadsheet.values_batch_get, ranges)
        # resp is a dict with 'valueRanges' aligned with our ranges
        for rng, vr in zip(ranges, resp.get("valueRanges", [])):
            values = vr.get("values", [])
            val = ""
            if values and values[0]:
                cell = values[0][0]
                val = str(cell).strip() if cell is not None else ""
            if val == "0":
                print(f"✅ Post-check passed: {rng} = 0 → Process completed successfully")
            else:
                print(f"❌ Post-check failed: {rng} = {val or '<EMPTY/None>'} → Process not completed")
    except Exception as e:
        print(f"❌ Batch post-checks failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare Feed List from Google Sheet tabs.")
    parser.add_argument("--algo-name", required=True, help="Google Sheet file name")
    parser.add_argument("--source-sheet", required=True, help="Source tab name")
    parser.add_argument("--dest-sheet", required=True, help="Destination tab name")
    args = parser.parse_args()

    prepare_feed_list(
        algo_name=args.algo_name,
        source_tab=args.source_sheet,
        dest_tab=args.dest_sheet
    )

    # Resolve spreadsheet explicitly from the CLI sheet name
    try:
        spreadsheet = load_sheet(args.algo_name)
    except Exception as e:
        spreadsheet = None
        print(f"❌ Could not open spreadsheet ALGO '{args.algo_name}' for post-checks: {e}")

    if spreadsheet is None:
        print("❌ Could not resolve Spreadsheet object for post-checks. Skipping post-checks.")
    else:
        _post_checks_batch(spreadsheet)
