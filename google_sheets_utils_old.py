# google_sheets_utils.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from runtime_paths import get_creds_path
def get_gsheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(get_creds_path()), scope)
    return gspread.authorize(creds)

def read_sheet(sheet_id, sheet_name):
    """
    Reads all records (rows) from the given sheet.
    Returns (list of dicts, sheet object).
    """
    client = get_gsheet_client()
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    records = sheet.get_all_records()
    return records, sheet

def read_rows_from_sheet(sheet, start_row, num_rows, as_dict=False):
    """
    Reads `num_rows` rows starting at `start_row` (1-based index) from the sheet.
    If as_dict=True, returns list of dicts keyed by header row (row 1).
    Otherwise, returns list of lists (values).
    """
    header = sheet.row_values(1)
    if not header:
        raise ValueError("Header row (row 1) is empty.")

    end_row = start_row + num_rows - 1
    max_col_letter = _col_num_to_letter(len(header))
    range_str = f"A{start_row}:{max_col_letter}{end_row}"
    rows = sheet.get(range_str, value_render_option='UNFORMATTED_VALUE')


    # Pad shorter rows to header length
    padded_rows = [row + ['']*(len(header) - len(row)) for row in rows]

    if as_dict:
        return [dict(zip(header, row)) for row in padded_rows]
    else:
        return padded_rows

def write_rows(sheet, rows, start_row_index):
    """
    Writes the given rows (list of lists) into the sheet starting at `start_row_index`.
    """
    for i, row in enumerate(rows):
        for col_index, value in enumerate(row):
            sheet.update_cell(start_row_index + i, col_index + 1, value)

def clear_column(sheet, col_name):
    """
    Clears contents of the column named `col_name` from row 2 downwards.
    """
    header = sheet.row_values(1)
    if col_name not in header:
        raise ValueError(f"Column '{col_name}' not found in header.")
    col_index = header.index(col_name) + 1
    col_letter = _col_num_to_letter(col_index)
    col_range = f"{col_letter}2:{col_letter}"
    sheet.batch_clear([col_range])

def _col_num_to_letter(col_num):
    """
    Converts a 1-based column number to its corresponding Excel-style column letter(s).
    e.g., 1 -> A, 27 -> AA
    """
    letters = ''
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters
