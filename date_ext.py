from datetime import datetime, date

from algo_sheets_lookup import get_sheet_id
from google_sheets_utils import get_gsheet_client, open_spreadsheet


def get_client():
    return get_gsheet_client()


def get_ws(algo_name, tab_name):
    gc = get_client()
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


sh4_src, ws4_src = get_ws("KWK_DEEP_BEAR_REVERSAL", "Friday_Identifier")
sh4_des, ws4_des = get_ws("KWK_DEEP_BEAR_REVERSAL", "Friday_Identifier")
try:
    before = ws4_des.acell("A2").value

    # call init_date normally (prints will show up)
    init_date(sh4_src.title, ws4_src, "B1", ws4_des, "A2")

    after = ws4_des.acell("A2").value

    changed = (before != after)

    # Update R1 boolean in separate sheet
    gc = get_client()
    flag_sh = open_spreadsheet(gc, spreadsheet_id=get_sheet_id("GTT_MASTER"))
    flag_ws = flag_sh.worksheet("ALL_OLD_GTTs")

    # Write boolean TRUE/FALSE to R1 (Google Sheets boolean, not string)
    flag_ws.update(range_name="R1", values=[[changed]])

    print(f"✅ Updated ALL_OLD_GTTs!R1 to {changed} (A2 before='{before}', after='{after}')")

except Exception as e:
    print(f"❌ Error during date init/check: {e}")
    try:
        gc = get_client()
        flag_sh = open_spreadsheet(gc, spreadsheet_id=get_sheet_id("GTT_MASTER"))
        flag_ws = flag_sh.worksheet("ALL_OLD_GTTs")
        flag_ws.update(range_name="R1", values=[[False]])
        print("⚠️ Fallback: set ALL_OLD_GTTs!R1 to False")
    except Exception as e2:
        print(f"❌ Could not write fallback flag: {e2}")
