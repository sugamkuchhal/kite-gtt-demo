import logging
import time
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials

import atexit
from script_logger import log_start, log_end
from ref_sheets_utils import resolve_sheet_id
from runtime_paths import get_creds_path

_RUN_CTX = log_start("prepare_us_feed_date_ext")
atexit.register(log_end, _RUN_CTX)

CREDS_PATH = str(get_creds_path())
LOOP_INTERVAL = 70  # seconds between iterations


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


def init_date(sheet_title, ws_src, src_cell, ws_dest, dest_cell):
    value = ws_src.acell(src_cell).value
    try:
        cell_date = datetime.strptime(value, "%d-%b-%Y").date()
    except Exception as e:
        print(f"{sheet_title} -> ❌ Could not parse '{value}' as a date: {e}")
        return None
    if cell_date <= date.today():
        ws_dest.update_acell(dest_cell, value)
        print(f"{sheet_title} -> ✅ Copied value '{value}' from {ws_src.title}:{src_cell} to {ws_dest.title}:{dest_cell}")
        return True
    else:
        print(f"{sheet_title} -> 🚫 Not copying: date {cell_date} is after today.")
        return None


def run_until_done(ref_sheets, tab_name):
    while True:
        sh, ws = get_ws(ref_sheets, tab_name)
        result = init_date(sh.title, ws, "B1", ws, "A2")
        if result is None:
            break
        print(f"{tab_name} -> ⏳ Next iteration in {LOOP_INTERVAL}s...")
        time.sleep(LOOP_INTERVAL)


def main():
    run_until_done("US_SGST", "US_OPEN_LIST")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
    except Exception:
        logging.exception("prepare_us_feed_date_ext failed.")
        raise SystemExit(1)
