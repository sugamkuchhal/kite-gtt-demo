from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id
from script_logger import log_start, log_end

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
    ref_sheets_kwk = "KWK"
    tab_name_kwk = "Friday_Identifier"
    sh4_src, ws4_src = get_ws(ref_sheets_kwk, tab_name_kwk)
    check_gt_threshold(sh4_src.title, ws4_src, "F1")

    ref_sheets_portfolio = "PORTFOLIO"
    tab_name_portfolio = "CREDIT_CANDIDATES"
    sh5_src, ws5_src = get_ws(ref_sheets_portfolio, tab_name_portfolio)
    check_gt_threshold(sh5_src.title, ws5_src, "K1")

    ref_sheets_rtp = "RTP"
    tab_name_rtp = "DATE_Identifier"
    sh6_src, ws6_src = get_ws(ref_sheets_rtp, tab_name_rtp)
    check_gt_threshold(sh6_src.title, ws6_src, "F1")

    ref_sheets_hundred = "HUNDRED"
    tab_name_hundred = "OPEN_LIST"
    sh7_src, ws7_src = get_ws(ref_sheets_hundred, tab_name_hundred)
    check_gt_threshold(sh7_src.title, ws7_src, "F1")

    ref_sheets_consolidated = "CONSOLIDATED"
    tab_name_consolidated = "OPEN_LIST"
    sh8_src, ws8_src = get_ws(ref_sheets_consolidated, tab_name_consolidated)
    check_gt_threshold(sh8_src.title, ws8_src, "E1")


if __name__ == "__main__":
    _ctx = log_start("data_val")
    try:
        main()
    finally:
        log_end(_ctx)
