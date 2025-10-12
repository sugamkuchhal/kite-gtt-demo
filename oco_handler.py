# oco_handler.py

import argparse
import time
import logging
from kite_session import get_kite  # Assumes you have this utility in your project
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURABLE ---
CREDS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"  # Adjust if needed
BATCH_SIZE = 10
SLEEP_BETWEEN_BATCHES = 2  # seconds

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("oco_handler")

def get_gsheet_client():
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scopes)
    return gspread.authorize(creds)

def fetch_gtt_ids(ws):
    """
    Read col F from row 2 downwards, stop at first blank.
    Returns: List of (row_num, gtt_id_str)
    """
    col_values = ws.col_values(6)  # F=6, 0-based=5
    result = []
    for idx, val in enumerate(col_values[1:], start=2):  # skip header, start at row 2
        if not val.strip():
            break
        result.append((idx, val.strip()))
    return result

def safe_api_call(func, *args, max_retries=3, base_delay=1, **kwargs):
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Rate limit hit, retrying in {delay:.2f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
            raise e
    return None

def main():
    parser = argparse.ArgumentParser(description="Delete GTT IDs from Kite, based on sheet tab OCO_GTT_DATA.")
    parser.add_argument('--sheet-name', required=True, help='Google Sheet filename')
    parser.add_argument('--tab-name', required=True, help='Tab name (worksheet) to process')
    args = parser.parse_args()

    logger.info(f"Opening Google Sheet: {args.sheet_name} [{args.tab_name}]")
    gc = get_gsheet_client()
    sh = gc.open(args.sheet_name)
    ws = sh.worksheet(args.tab_name)

    kite = get_kite()

    gtt_rows = fetch_gtt_ids(ws)
    total = len(gtt_rows)
    logger.info(f"Found {total} GTT IDs to process (col F, up to first blank)")

    # --- Clear column G statuses before starting ---
    MAX_ROWS = ws.row_count  # This gives you the absolute number of rows in the sheet, used or unused
    if MAX_ROWS > 1:
        clear_range = f'G2:G{MAX_ROWS}'
        logger.info(f"Clearing status column G: {clear_range}")
        ws.update(clear_range, [[""]] * (MAX_ROWS - 1))
    
    statuses = []
    rows = []

    for batch_start in range(0, total, BATCH_SIZE):
        batch = gtt_rows[batch_start: batch_start+BATCH_SIZE]
        batch_status = []
        logger.info(f"Processing batch {batch_start//BATCH_SIZE + 1}: rows {batch[0][0]}–{batch[-1][0]}")

        for row_num, gtt_id_str in batch:
            status_cell = ""
            try:
                gtt_id = int(float(gtt_id_str))
                logger.debug(f"Attempting to delete GTT ID: {gtt_id} (row {row_num})")
                safe_api_call(kite.delete_gtt, gtt_id)
                status_cell = "✅ deleted"
                logger.info(f"Deleted GTT ID {gtt_id} (row {row_num})")
            except Exception as e:
                err_msg = f"❌ error: {str(e)}"
                status_cell = err_msg
                logger.error(f"Error deleting GTT ID {gtt_id_str} (row {row_num}): {err_msg}")
            batch_status.append([status_cell])
            rows.append(row_num)

        # Batch update column G (7th col) with results for this batch
        cell_range = f'G{batch[0][0]}:G{batch[-1][0]}'
        ws.update(cell_range, batch_status)
        logger.info(f"Updated status in sheet: {cell_range}")

        time.sleep(SLEEP_BETWEEN_BATCHES)

    logger.info("All done.")

if __name__ == "__main__":
    main()
