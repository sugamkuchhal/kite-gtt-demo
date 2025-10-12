# status_tracker.py
from google_sheets_utils import write_rows, read_rows_from_sheet

def update_status_in_sheet(sheet, start_row, rows_with_status):
    """
    Updates the STATUS column in the sheet for each row in `rows_with_status`.
    `rows_with_status` is a list of dicts with at least keys 'row_number' and 'STATUS'.
    Assumes header is in row 1.
    """
    header = sheet.row_values(1)
    if "STATUS" not in header:
        raise ValueError("STATUS column not found in sheet header.")
    status_col_index = header.index("STATUS") + 1

    for item in rows_with_status:
        row_num = item.get("row_number")
        status = item.get("STATUS", "")
        if row_num is None:
            continue
        # Update the STATUS cell
        sheet.update_cell(row_num, status_col_index, status)
