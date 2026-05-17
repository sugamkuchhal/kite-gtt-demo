from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials
import logging

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("prepare_feed_data_val")
atexit.register(log_end, _RUN_CTX)
CREDS_PATH = str(get_creds_path())

def get_ws(ref_sheets, tab_name):
    creds = Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    sheet_id = resolve_sheet_id(ref_sheets)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    return sh, ws

def check_gt_threshold(sheet_title, ws, cell, threshold=0.995):
    value = ws.acell(cell).value
    try:
        # Handle empty/whitespace as 0.0
        if value is None or str(value).strip().lower() in ("", "na", "n/a", "null", "none"):
            val_float = 0.0
            print(f"❌ [{ws.title}:{cell}] Value is empty or blank, treating as 0.0000 -> {sheet_title}")
        else:
            val_float = float(value)
    except Exception as e:
        print(f"❌ [{ws.title}:{cell}] FAIL: Non-numeric value '{value}'. Error: {e} -> {sheet_title}")
        return
    print(f"[{ws.title}:{cell}] Value: {val_float:.4f}", end=' ')
    if val_float > threshold:
        print(f"-- (> {threshold}) ✅ PASS: -> {sheet_title}")
    else:
        if val_float == 0.0:
            print(f"-- ❌ FAIL: Value is zero -> {sheet_title}")
        else:
            print(f"-- ❌ FAIL: Value not greater than {threshold} -> {sheet_title}")

def main():
    # ==== Threshold Check Example Usage: ====
    sh1_src, ws1_src = get_ws("FEED", "SGST_OPEN_LIST")
    check_gt_threshold(sh1_src.title, ws1_src, "G1")  

    sh2_src, ws2_src = get_ws("FEED", "SUPER_OPEN_LIST")
    check_gt_threshold(sh2_src.title, ws2_src, "G1") 

    sh3_src, ws3_src = get_ws("FEED", "TURTLE_OPEN_LIST")
    check_gt_threshold(sh3_src.title, ws3_src, "G1") 

if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
    except Exception:
        logging.exception("prepare_feed_data_val failed.")
        raise SystemExit(1)
