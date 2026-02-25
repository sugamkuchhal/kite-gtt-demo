from datetime import datetime, date
from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import get_gsheet_client, open_spreadsheet

def get_ws(algo_name, tab_name):
    gc = get_gsheet_client()
    sh = open_spreadsheet(gc, spreadsheet_id=get_sheet_id(algo_name))
    ws = sh.worksheet(tab_name)
    return sh, ws

def init_date(sheet_title, ws_src, src_cell, ws_dest, dest_cell):
    value = ws_src.acell(src_cell).value
    try:
        cell_date = datetime.strptime(value, "%d-%b-%Y").date()
    except Exception as e:
        print(f"{sheet_title} -> ❌ Could not parse '{value}' as a date: {e}")
        return
    if cell_date <= date.today():
        ws_dest.update_acell(dest_cell, value)
        print(f"{sheet_title} -> ✅ Copied value '{value}' from {ws_src.title}:{src_cell} to {ws_dest.title}:{dest_cell}")
    else:
        print(f"{sheet_title} -> 🚫 Not copying: date {cell_date} is after today.")

sh1_src, ws1_src = get_ws("US_DGB_SGST_REVERSAL_VALIDATION_WITH_BOH", "US_OPEN_LIST")
sh1_des, ws1_des = get_ws("US_DGB_SGST_REVERSAL_VALIDATION_WITH_BOH", "US_OPEN_LIST")
init_date(sh1_src.title, ws1_src, "B1", ws1_des, "A2")
