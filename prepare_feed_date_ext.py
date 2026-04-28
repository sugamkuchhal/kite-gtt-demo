from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

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

sh1_src, ws1_src = get_ws("FEED", "SGST_OPEN_LIST")
sh1_des, ws1_des = get_ws("FEED", "SGST_OPEN_LIST")
init_date(sh1_src.title, ws1_src, "B1", ws1_des, "A2")

sh2_src, ws2_src = get_ws("FEED", "SUPER_OPEN_LIST")
sh2_des, ws2_des = get_ws("FEED", "SUPER_OPEN_LIST")
init_date(sh2_src.title, ws2_src, "B1", ws2_des, "A2")

sh3_src, ws3_src = get_ws("FEED", "TURTLE_OPEN_LIST")
sh3_des, ws3_des = get_ws("FEED", "TURTLE_OPEN_LIST")
init_date(sh3_src.title, ws3_src, "B1", ws3_des, "A2")
