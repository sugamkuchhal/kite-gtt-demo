from datetime import datetime, date
from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import get_gsheet_client, open_spreadsheet

def get_ws(algo_name, tab_name):
    gc = get_gsheet_client()
    sh = open_spreadsheet(gc, spreadsheet_id=get_sheet_id(algo_name))
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

# ==== Threshold Check Example Usage: ====

sh1_src, ws1_src = get_ws("ALGO_MASTER_FEED_SHEET", "SGST_OPEN_LIST")
check_gt_threshold(sh1_src.title, ws1_src, "G1")  

sh2_src, ws2_src = get_ws("ALGO_MASTER_FEED_SHEET", "SUPER_OPEN_LIST")
check_gt_threshold(sh2_src.title, ws2_src, "G1") 

sh3_src, ws3_src = get_ws("ALGO_MASTER_FEED_SHEET", "TURTLE_OPEN_LIST")
check_gt_threshold(sh3_src.title, ws3_src, "G1") 
