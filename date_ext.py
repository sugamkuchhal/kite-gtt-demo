from datetime import datetime, date
import gspread

from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path

CREDS_PATH = str(get_creds_path())

def get_client():
    creds = Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

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

sh4_src, ws4_src = get_ws("SARAS W M B - KWK (Deep Bear Reversal)", "Friday_Identifier")
sh4_des, ws4_des = get_ws("SARAS W M B - KWK (Deep Bear Reversal)", "Friday_Identifier")
try:
    before = ws4_des.acell("A2").value

    # call init_date normally (prints will show up)
    init_date(sh4_src.title, ws4_src, "B1", ws4_des, "A2")

    after = ws4_des.acell("A2").value

    changed = (after != before)  # raw text comparison, same as before
    gc = get_client()
    flag_sh = gc.open_by_key("14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s")
    flag_ws = flag_sh.worksheet("ALL_OLD_GTTs")

    # Write boolean TRUE/FALSE to R1 (Google Sheets boolean, not string)
    flag_ws.update(range_name="R1", values=[[changed]])

except:

    # On any exception, write boolean FALSE (same behavior as old "0")
    try:
        gc = get_client()
        flag_sh = gc.open_by_key("14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s")
        flag_ws = flag_sh.worksheet("ALL_OLD_GTTs")
        flag_ws.update(range_name="R1", values=[[False]])
    except:

        # If even the flag write fails, there's nothing further we can do.
        pass

sh5_src, ws5_src = get_ws("SARAS Portfolio - Stocks", "CREDIT_CANDIDATES")
sh5_des, ws5_des = get_ws("SARAS Portfolio - Stocks", "CREDIT_CANDIDATES")
init_date(sh5_src.title, ws5_src, "K24", ws5_des, "K23")

sh6_src, ws6_src = get_ws("SARAS D G C - RTP (Reverse Trigger Point Salvaging)", "DATE_Identifier")
sh6_des, ws6_des = get_ws("SARAS D G C - RTP (Reverse Trigger Point Salvaging)", "DATE_Identifier")
init_date(sh6_src.title, ws6_src, "B1", ws6_des, "A2")

sh7_src, ws7_src = get_ws("SARAS D M B - 100 DMA Stock Screener with BOH", "OPEN_LIST")
sh7_des, ws7_des = get_ws("SARAS D M B - 100 DMA Stock Screener with BOH", "OPEN_LIST")
init_date(sh7_src.title, ws7_src, "B1", ws7_des, "A2")

sh8_src, ws8_src = get_ws("SARAS D M B - Consolidated BreakOut with BOH", "OPEN_LIST")
sh8_des, ws8_des = get_ws("SARAS D M B - Consolidated BreakOut with BOH", "OPEN_LIST")
init_date(sh8_src.title, ws8_src, "B1", ws8_des, "A2")

