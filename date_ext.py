from datetime import datetime, date
import gspread

from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

CREDS_PATH = str(get_creds_path())

def get_client():
    creds = Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

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

ref_sheets_kwk = "KWK"
tab_name_kwk = "Friday_Identifier"
sh4_src, ws4_src = get_ws(ref_sheets_kwk, tab_name_kwk)
sh4_des, ws4_des = get_ws(ref_sheets_kwk, tab_name_kwk)
try:
    before = ws4_des.acell("A2").value

    # call init_date normally (prints will show up)
    init_date(sh4_src.title, ws4_src, "B1", ws4_des, "A2")

    after = ws4_des.acell("A2").value

    changed = (after != before)  # raw text comparison, same as before
    gc = get_client()
    ref_sheets_flag = "PORTFOLIO"
    sheet_id_flag = resolve_sheet_id(ref_sheets_flag)
    tab_name_flag = "ALL_OLD_GTTs"
    flag_sh = gc.open_by_key(sheet_id_flag)
    flag_ws = flag_sh.worksheet(tab_name_flag)

    # Write boolean TRUE/FALSE to R1 (Google Sheets boolean, not string)
    flag_ws.update(range_name="R1", values=[[changed]])

except:

    # On any exception, write boolean FALSE (same behavior as old "0")
    try:
        gc = get_client()
        ref_sheets_flag = "PORTFOLIO"
        sheet_id_flag = resolve_sheet_id(ref_sheets_flag)
        tab_name_flag = "ALL_OLD_GTTs"
        flag_sh = gc.open_by_key(sheet_id_flag)
        flag_ws = flag_sh.worksheet(tab_name_flag)
        flag_ws.update(range_name="R1", values=[[False]])
    except:

        # If even the flag write fails, there's nothing further we can do.
        pass

ref_sheets_portfolio = "PORTFOLIO"
tab_name_portfolio = "CREDIT_CANDIDATES"
sh5_src, ws5_src = get_ws(ref_sheets_portfolio, tab_name_portfolio)
sh5_des, ws5_des = get_ws(ref_sheets_portfolio, tab_name_portfolio)
init_date(sh5_src.title, ws5_src, "K24", ws5_des, "K23")

ref_sheets_rtp = "RTP"
tab_name_rtp = "DATE_Identifier"
sh6_src, ws6_src = get_ws(ref_sheets_rtp, tab_name_rtp)
sh6_des, ws6_des = get_ws(ref_sheets_rtp, tab_name_rtp)
init_date(sh6_src.title, ws6_src, "B1", ws6_des, "A2")

ref_sheets_hundred = "HUNDRED"
tab_name_hundred = "OPEN_LIST"
sh7_src, ws7_src = get_ws(ref_sheets_hundred, tab_name_hundred)
sh7_des, ws7_des = get_ws(ref_sheets_hundred, tab_name_hundred)
init_date(sh7_src.title, ws7_src, "B1", ws7_des, "A2")

ref_sheets_consolidated = "CONSOLIDATED"
tab_name_consolidated = "OPEN_LIST"
sh8_src, ws8_src = get_ws(ref_sheets_consolidated, tab_name_consolidated)
sh8_des, ws8_des = get_ws(ref_sheets_consolidated, tab_name_consolidated)
init_date(sh8_src.title, ws8_src, "B1", ws8_des, "A2")
