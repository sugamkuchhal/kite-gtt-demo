#!/usr/bin/env python3
"""
date_bank_teleporter_incremental.py

NOW uses source Column H as the incremental flag:
- Copy rows from source -> destination (A:F) ONLY if source[H] != "DONE" (case-insensitive, trimmed).
- After successful append, mark the corresponding source rows' Column H as "DONE".

Still:
- Last row detection in destination is based strictly on Column A (paged scan, no A:A full read).
- Exponential backoff with full jitter for all API calls.
- Batched writes for appends and formula fills.
- Copies H:I formulas in destination for the newly appended rows using last existing data row as template.
"""

import time
import random
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ======== CONFIG ========
CREDS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"

SRC_SHEET = "Algo Master Data Bank"
SRC_TAB   = "BANK_FINAL"

DEST_SHEET = "Algo Master Data Calculator"
DEST_TAB   = "BANK_FINAL"

SRC_RANGE_AH = "A:H"   # read A:H from source (A:F data + H status)
HEADER_ROW = 1         # single header row
BATCH_SIZE = 500
MAX_APPEND_PER_CALL = 2000
MAX_RETRIES = 7

# Paged read chunk sizes
DEST_A_PAGE_SIZE    = 10000  # for last-nonempty scan in Column A
# ========================


# ---------- Utilities ----------
def log(msg=""):
    print(msg, flush=True)

def backoff(attempt, base=1.5, cap=60.0):
    """Full-jitter exponential backoff (attempt is 1-based)."""
    exp = min(cap, base * (2 ** (attempt - 1)))
    time.sleep(random.uniform(0, exp))

def authorize(creds_path: str):
    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def gsheet_get(ws, rng, value_render_option=None, retries=MAX_RETRIES):
    """GET wrapper with retries/backoff."""
    last = None
    for attempt in range(1, retries + 1):
        try:
            if value_render_option:
                return ws.get(rng, value_render_option=value_render_option) or []
            return ws.get(rng) or []
        except Exception as e:
            last = e
            log(f"⚠️  GET {rng} attempt {attempt} failed: {e}")
            backoff(attempt)
    raise last

def gsheet_update(ws, rng, values, value_input_option="USER_ENTERED", retries=MAX_RETRIES):
    """UPDATE wrapper with retries/backoff."""
    last = None
    for attempt in range(1, retries + 1):
        try:
            ws.update(rng, values, value_input_option=value_input_option)
            return
        except Exception as e:
            last = e
            log(f"⚠️  UPDATE {rng} attempt {attempt} failed: {e}")
            backoff(attempt)
    raise last

def ensure_rows(ws, required_rows: int):
    """Ensure the destination sheet has at least required_rows rows."""
    current = ws.row_count
    if required_rows > current:
        to_add = required_rows - current
        log(f"🔧 Adding {to_add} rows to destination (had {current}, need {required_rows})")
        ws.add_rows(to_add)

def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]

def normalize_key(a, b):
    # kept for compatibility; no longer used for dedupe
    a = "" if a is None else str(a).strip()
    b = "" if b is None else str(b).strip()
    return (a, b)

def is_done_flag(val: str) -> bool:
    return (str(val or "").strip().lower() == "done")


# ---------- Domain helpers ----------
def read_source_pending_rows(ws_src):
    """
    Read A:H from source, skip header.
    Select rows where Column H is NOT 'DONE' (case-insensitive, trimmed).
    Returns:
      pending_rows: list of [A..F] rows ready to append (len=6 each, padded)
      pending_row_indices: list of absolute row indices in the source sheet to mark DONE (H column) after append
    """
    t0 = time.time()
    data = gsheet_get(ws_src, SRC_RANGE_AH)
    if not data or len(data) <= 1:
        log("❌ No source data rows to process.")
        return [], []

    body = data[1:]  # skip header
    pending_rows = []
    pending_row_indices = []

    # absolute row index in the sheet (1-based); header is row 1
    abs_row = HEADER_ROW + 1

    for r in body:
        # Ensure we can safely index A..H
        r = list(r) + [""] * max(0, 8 - len(r))
        status = r[7]  # Column H
        if not is_done_flag(status):
            # collect A:F, padding to 6
            af = r[:6] + [""] * max(0, 6 - len(r[:6]))
            pending_rows.append(af[:6])
            pending_row_indices.append(abs_row)
        abs_row += 1

    t1 = time.time()
    log(f"📥 Read source {len(body)} data rows from {SRC_SHEET}::{SRC_TAB} in {t1 - t0:.2f}s")
    log(f"🧮 Found {len(pending_rows)} pending row(s) to copy based on Column H flag.")
    return pending_rows, pending_row_indices

def paged_find_last_nonempty_in_col_A(ws_dest, page_size=DEST_A_PAGE_SIZE, max_pages=200):
    """
    Find the absolute row index (1-based) of the last non-empty cell in Column A,
    considering only data rows (i.e., starting from row 2 to skip header).

    Returns:
        last_nonempty_abs_row (int)
        last_data_row_count (int)  # data rows count = max(0, last_nonempty_abs_row - HEADER_ROW)
    """
    # Start after header
    start = HEADER_ROW + 1
    pages = 0
    last_nonempty_abs_row = HEADER_ROW  # if no data, stays as header

    while pages < max_pages:
        end = start + page_size - 1
        rng = f"A{start}:A{end}"
        try:
            block = gsheet_get(ws_dest, rng)
        except Exception:
            backoff(min(pages + 1, 3))
            block = gsheet_get(ws_dest, rng)

        if not block:
            break

        # Scan this page bottom-up to find the last non-empty
        for idx in range(len(block) - 1, -1, -1):
            cell = block[idx][0] if block[idx] else ""
            if str(cell).strip() != "":
                # Absolute row index = start + idx
                last_nonempty_abs_row = start + idx
                break  # found last non-empty within this page

        # If the page wasn't "full", we're at the end
        if len(block) < page_size:
            break

        start = end + 1
        pages += 1

    last_data_row_count = max(0, last_nonempty_abs_row - HEADER_ROW)
    return last_nonempty_abs_row, last_data_row_count

def append_rows(ws_dest, start_row, rows_to_append):
    """Append rows_to_append (list of A:F rows) to destination starting at start_row."""
    if not rows_to_append:
        return 0, 0

    # Ensure grid fits (pre-extend so writes never fail)
    end_row = start_row + len(rows_to_append) - 1
    ensure_rows(ws_dest, end_row)

    appended_total = 0
    append_calls = 0
    row_ptr = start_row

    for big in chunked(rows_to_append, MAX_APPEND_PER_CALL):
        for small in chunked(big, BATCH_SIZE):
            write_end = row_ptr + len(small) - 1
            rng = f"A{row_ptr}:F{write_end}"
            gsheet_update(ws_dest, rng, small, value_input_option="USER_ENTERED")
            appended_total += len(small)
            append_calls += 1
            row_ptr = write_end + 1

    return appended_total, append_calls

def copy_formulas_HI(ws_dest, template_abs_row, newly_added_count):
    """
    Drag formulas in H:I from the *template row* (absolute row index)
    down to the newly added rows.

    If there is no template row (e.g., no data yet), skip gracefully.
    """
    if newly_added_count <= 0:
        return 0

    if template_abs_row <= HEADER_ROW:
        log("ℹ️  Destination had no prior data rows; skipping H:I formula fill (no template to copy).")
        return 0

    # Read formulas from H and I at the template row (value_render_option="FORMULA" to get the formula text)
    h_i_range = f"H{template_abs_row}:I{template_abs_row}"
    vals = gsheet_get(ws_dest, h_i_range, value_render_option="FORMULA")
    if not vals or not vals[0] or len(vals[0]) < 2:
        log(f"⚠️  Could not read formulas from {h_i_range}; skipping H:I fill.")
        return 0

    h_formula = vals[0][0] if vals[0][0].startswith("=") else None
    i_formula = vals[0][1] if vals[0][1].startswith("=") else None
    if not h_formula and not i_formula:
        log(f"ℹ️  No formulas detected in {h_i_range}; skipping H:I fill.")
        return 0

    # New rows start just after the template row
    start_row = template_abs_row + 1
    end_row = start_row + newly_added_count - 1

    # Build a 2D list of formulas for H:I for each new row
    rows = []
    for _ in range(newly_added_count):
        rows.append([
            h_formula if h_formula else "",
            i_formula if i_formula else "",
        ])

    rng = f"H{start_row}:I{end_row}"
    gsheet_update(ws_dest, rng, rows, value_input_option="USER_ENTERED")
    return newly_added_count

def _group_contiguous(indices):
    """
    Given a sorted list of integers, return list of (start, end) contiguous runs.
    Example: [5,6,7,  10,11,  15] -> [(5,7), (10,11), (15,15)]
    """
    if not indices:
        return []
    runs = []
    start = prev = indices[0]
    for x in indices[1:]:
        if x == prev + 1:
            prev = x
        else:
            runs.append((start, prev))
            start = prev = x
    runs.append((start, prev))
    return runs

def mark_source_done(ws_src, abs_row_indices, batch_size=BATCH_SIZE):
    """
    Mark Column H = "DONE" for the given absolute source row indices.
    Writes in contiguous runs to minimize API calls.
    """
    if not abs_row_indices:
        return 0

    sorted_idx = sorted(abs_row_indices)
    runs = _group_contiguous(sorted_idx)

    total_marked = 0
    # Optionally batch runs if there are too many
    for big in chunked(runs, max(1, batch_size // 10)):
        for (start, end) in big:
            rng = f"H{start}:H{end}"
            count = end - start + 1
            values = [["DONE"]] * count
            gsheet_update(ws_src, rng, values, value_input_option="USER_ENTERED")
            total_marked += count

    return total_marked


# ---------- Main ----------
def main():
    log("")
    start_time = datetime.now()
    log(f"[DATE BANK TELEPORTER PROCESS START] {start_time.strftime('%Y-%m-%d %H:%M:%S')} - Starting incremental copy")

    # Auth
    try:
        gc = authorize(CREDS_PATH)
    except Exception as e:
        log(f"❌ Auth error: {e}")
        return

    # Open sheets
    try:
        ws_src = gc.open(SRC_SHEET).worksheet(SRC_TAB)
    except Exception as e:
        log(f"❌ Open source error ({SRC_SHEET}::{SRC_TAB}): {e}")
        return

    try:
        ws_dest = gc.open(DEST_SHEET).worksheet(DEST_TAB)
    except Exception as e:
        log(f"❌ Open destination error ({DEST_SHEET}::{DEST_TAB}): {e}")
        return

    # Source pending rows (A:F) + their absolute row indices to mark DONE
    rows_to_copy, row_indices_to_mark = read_source_pending_rows(ws_src)
    if not rows_to_copy:
        log("✅ Nothing new to append (all source rows already marked DONE).")
        end_time = datetime.now()
        log(f"[DATE BANK TELEPORTER PROCESS END]✅  {end_time.strftime('%Y-%m-%d %H:%M:%S')} - No changes.")
        return

    # Last non-empty in destination Column A (absolute row index) + last data row count
    t0 = time.time()
    last_nonempty_A_abs, last_data_row_count = paged_find_last_nonempty_in_col_A(ws_dest)
    t1 = time.time()
    log(f"🔎 Last non-empty in Column A at row {last_nonempty_A_abs} (data rows: {last_data_row_count}) — scanned in {t1 - t0:.2f}s")

    # Start writing at next row after the last non-empty A
    start_row = last_nonempty_A_abs + 1
    log(f"🧭 Appending {len(rows_to_copy)} new row(s) to destination A:F starting at row {start_row}")

    # Append A:F
    appended_total, append_calls = append_rows(ws_dest, start_row, rows_to_copy)
    log(f"✅ Append complete. {appended_total} rows written via {append_calls} update call(s).")

    # Drag formulas in destination H:I using template = last non-empty A (the original last data row)
    filled = copy_formulas_HI(ws_dest, template_abs_row=last_nonempty_A_abs, newly_added_count=appended_total)
    if filled:
        log(f"🧪 Formulas filled in destination H:I for {filled} new rows.")

    # Only after successful append & (optional) formula fill, mark source H="DONE"
    if appended_total > 0:
        marked = mark_source_done(ws_src, row_indices_to_mark)
        log(f"📝 Marked {marked} source row(s) as DONE in Column H.")

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    log(f"[DATE BANK TELEPORTER PROCESS END]✅  {end_time.strftime('%Y-%m-%d %H:%M:%S')} - Finished (duration: {elapsed:.2f}s)")
    log("")


if __name__ == "__main__":
    main()
