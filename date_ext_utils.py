from datetime import datetime, date
from ref_sheets_utils import resolve_sheet_id
from google_sheets_utils import get_gsheet_client, gsheets_retry


def get_ws(ref_sheets, tab_name):
    gc = get_gsheet_client()
    sheet_id = resolve_sheet_id(ref_sheets)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    return sh, ws


def init_date(sheet_title, ws_src, src_cell, ws_dest, dest_cell):
    """
    Reads a date from ws_src:src_cell and copies it to ws_dest:dest_cell
    if the date is <= today.

    Returns:
        True   — date was copied and the destination value changed
        False  — date was copied but destination value was already the same
        None   — date was not copied (future date or parse error)
    """
    value = gsheets_retry(ws_src.acell, src_cell).value
    try:
        cell_date = datetime.strptime(value, "%d-%b-%Y").date()
    except Exception as e:
        print(f"{sheet_title} -> ❌ Could not parse '{value}' as a date: {e}")
        return None

    if cell_date > date.today():
        print(f"{sheet_title} -> 🚫 Not copying: date {cell_date} is after today.")
        return None

    before = gsheets_retry(ws_dest.acell, dest_cell).value
    gsheets_retry(ws_dest.update_acell, dest_cell, value)
    changed = (value != before)

    if changed:
        print(f"{sheet_title} -> ✅ Date changed: '{before}' → '{value}' ({ws_src.title}:{src_cell} → {ws_dest.title}:{dest_cell})")
    else:
        print(f"{sheet_title} -> ✅ Date unchanged: '{value}' already in {ws_dest.title}:{dest_cell}")

    return changed
