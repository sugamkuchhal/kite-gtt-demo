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

sh4_src, ws4_src = get_ws("KWK_DEEP_BEAR_REVERSAL", "Friday_Identifier")
check_gt_threshold(sh4_src.title, ws4_src, "F1") 

sh5_src, ws5_src = get_ws("PORTFOLIO_STOCKS", "CREDIT_CANDIDATES")
check_gt_threshold(sh5_src.title, ws5_src, "K1") 

sh6_src, ws6_src = get_ws("RTP_REVERSE_TRIGGER_POINT_SALVAGING", "DATE_Identifier")
check_gt_threshold(sh6_src.title, ws6_src, "F1") 

sh7_src, ws7_src = get_ws("DMB_100_DMA_STOCK_SCREENER_WITH_BOH", "OPEN_LIST")
check_gt_threshold(sh7_src.title, ws7_src, "F1") 

sh8_src, ws8_src = get_ws("DMB_CONSOLIDATED_BREAKOUT_WITH_BOH", "OPEN_LIST")
check_gt_threshold(sh8_src.title, ws8_src, "E1") 

