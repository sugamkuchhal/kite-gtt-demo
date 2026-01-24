# google_sheets_utils.py

import os
import time
import random
import collections
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError
from functools import lru_cache

from runtime_paths import get_creds_path

# ---- Simple token-bucket to keep us under RPM caps (reads/writes) ----
_MAX_RPM = int(os.getenv("GSHEETS_MAX_RPM", "55"))  # conservative default
_CALL_TIMES = collections.deque()  # timestamps of recent calls (any GET/UPDATE)

def _throttle():
    now = time.time()
    # drop timestamps older than 60s
    while _CALL_TIMES and now - _CALL_TIMES[0] > 60.0:
        _CALL_TIMES.popleft()
    if len(_CALL_TIMES) >= _MAX_RPM:
        sleep_for = 60.0 - (now - _CALL_TIMES[0]) + 0.01
        if sleep_for > 0:
            time.sleep(sleep_for)
    _CALL_TIMES.append(time.time())

# ---- Robust wrapper for transient errors (429/5xx), with jitter ----
def _is_retriable(e: Exception) -> bool:
    if not isinstance(e, APIError):
        return False
    try:
        code = int(getattr(e, "response", None) and e.response.status_code or 0)
    except Exception:
        code = 0
    return code in (429, 500, 502, 503, 504)

def _call_with_retries(fn, *args, **kwargs):
    attempts = int(os.getenv("GSHEETS_MAX_RETRIES", "6"))
    base = float(os.getenv("GSHEETS_BACKOFF_BASE", "0.6"))
    for i in range(attempts):
        try:
            _throttle()
            return fn(*args, **kwargs)
        except Exception as e:
            if i == attempts - 1 or not _is_retriable(e):
                raise
            # exponential backoff with jitter
            sleep_s = (base * (2 ** i)) + random.uniform(0, 0.2)
            time.sleep(sleep_s)

# ---- Auth unchanged ----
def get_gsheet_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(str(get_creds_path()), scope)
    return gspread.authorize(creds)

# ---- Header cache (per worksheet id) ----
# gspread Worksheet exposes .id for the grid's sheetId in recent versions.
@lru_cache(maxsize=256)
def _cached_header_for_sheet(sheet_id: int, fetcher):
    # fetcher is a zero-arg lambda that will call sheet.row_values(1)
    return _call_with_retries(fetcher)

def _get_header_row(sheet):
    # use cache if possible; fall back to direct call if id missing
    sid = getattr(sheet, "id", None)
    if sid is None:
        return _call_with_retries(sheet.row_values, 1)
    return _cached_header_for_sheet(sid, lambda: sheet.row_values(1))

# ---- Public API (unchanged signatures) ----
def read_sheet(sheet_id, sheet_name):
    """
    Reads all records (rows) from the given sheet.
    Returns (list of dicts, sheet object).
    """
    client = get_gsheet_client()
    # open/worksheet are light; the heavy part is the values call
    sheet = _call_with_retries(client.open_by_key, sheet_id).worksheet(sheet_name)
    records = _call_with_retries(sheet.get_all_records)
    return records, sheet

def read_rows_from_sheet(sheet, start_row, num_rows, as_dict=False):
    """
    Reads `num_rows` rows starting at `start_row` (1-based index) from the sheet.
    If as_dict=True, returns list of dicts keyed by header row (row 1).
    Otherwise, returns list of lists (values).
    """
    header = _get_header_row(sheet)
    if not header:
        raise ValueError("Header row (row 1) is empty.")

    end_row = start_row + num_rows - 1
    max_col_letter = _col_num_to_letter(len(header))
    range_str = f"A{start_row}:{max_col_letter}{end_row}"

    rows = _call_with_retries(
        sheet.get, range_str, value_render_option="UNFORMATTED_VALUE"
    )

    # Pad shorter rows to header length
    padded_rows = [row + [""] * (len(header) - len(row)) for row in rows]

    if as_dict:
        return [dict(zip(header, row)) for row in padded_rows]
    else:
        return padded_rows

def write_rows(sheet, rows, start_row_index):
    """
    Writes the given rows (list of lists) into the sheet starting at `start_row_index`.
    (Same effect as many update_cell calls, but done in one batch to avoid rate limits.)
    """
    if not rows:
        return
    num_cols = max((len(r) for r in rows), default=0)
    if num_cols == 0:
        return
    start_col_letter = "A"
    end_col_letter = _col_num_to_letter(num_cols)
    end_row_index = start_row_index + len(rows) - 1
    rng = f"{start_col_letter}{start_row_index}:{end_col_letter}{end_row_index}"

    body = {"range": rng, "majorDimension": "ROWS", "values": rows}
    _call_with_retries(sheet.update, rng, rows)

def clear_column(sheet, col_name):
    """
    Clears contents of the column named `col_name` from row 2 downwards.
    """
    header = _get_header_row(sheet)
    if col_name not in header:
        raise ValueError(f"Column '{col_name}' not found in header.")
    col_index = header.index(col_name) + 1
    col_letter = _col_num_to_letter(col_index)
    col_range = f"{col_letter}2:{col_letter}"
    _call_with_retries(sheet.batch_clear, [col_range])

def _col_num_to_letter(col_num):
    """
    Converts a 1-based column number to its corresponding Excel-style column letter(s).
    e.g., 1 -> A, 27 -> AA
    """
    letters = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters
