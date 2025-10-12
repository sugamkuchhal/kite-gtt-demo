from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials

CREDS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"

def get_ws(sheet_name, tab_name):
    creds = Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open(sheet_name)
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

sh1_src, ws1_src = get_ws("US D G B - SGST (Reversal Validation) With BOH", "US_OPEN_LIST")
sh1_des, ws1_des = get_ws("US D G B - SGST (Reversal Validation) With BOH", "US_OPEN_LIST")
init_date(sh1_src.title, ws1_src, "B1", ws1_des, "A2")
