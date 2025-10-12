#!/usr/bin/env python3
"""
Optimized date_bank_teleporter.py
Preserves original functionality but reduces API calls and improves retry/backoff.
"""

import gspread
from google.oauth2.service_account import Credentials
from tqdm import tqdm
from datetime import datetime
import time
import random
import sys

# --- CONFIGURATION ---
CREDS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"
SRC_SHEET = "Algo Master Data Bank"
SRC_TAB = "BANK_FINAL"
DEST_SHEET = "Algo Master Data Calculator"
DEST_TAB = "BANK_FINAL"
COL_RANGE = "A:F"     # columns pulled from source for full row values
DEST_KEY_RANGE = "A:B"  # only pull A:B from destination for dedupe (faster)
BATCH_SIZE = 500      # tuneable: number of rows per append batch
MAX_RETRIES = 3
# max rows to append in a single append_rows call: keep reasonably large (Sheets supports large payloads)
MAX_APPEND_PER_CALL = 2000

def log(msg=""):
    print(msg, flush=True)

def exponential_backoff_sleep(attempt, base=1.0, cap=30.0):
    """Exponential backoff with jitter."""
    # attempt is 1-based
    exp = min(cap, base * (2 ** (attempt - 1)))
    jitter = random.uniform(0, exp * 0.1)
    time.sleep(exp + jitter)

def authorize(creds_path):
    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    return gspread.authorize(creds)

def safe_get(ws, range_name, max_retries=MAX_RETRIES):
    """Robust getter with retries and backoff. Returns list-of-rows or []"""
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            vals = ws.get(range_name)
            return vals or []
        except Exception as e:
            last_exc = e
            log(f"⚠️  Failed to read ({range_name}) on attempt {attempt}: {e}")
            exponential_backoff_sleep(attempt)
    # if we get here, it failed permanently
    raise last_exc

def ensure_sheet_has_rows(ws, required_rows):
    """
    Ensure worksheet has at least required_rows rows (1-based count).
    Returns (added_start_row, added_end_row) if rows were added, else (None, None).
    """
    current_sheet_rows = ws.row_count
    if required_rows > current_sheet_rows:
        to_add = required_rows - current_sheet_rows
        added_start = current_sheet_rows + 1
        added_end = required_rows
        log(f"🔧 Adding {to_add} rows to destination sheet (was {current_sheet_rows}, need {required_rows})")
        ws.add_rows(to_add)
        return added_start, added_end
    return None, None

def chunked_iter(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def main():
    start_time = datetime.now()
    log("")
    log(f"[DATE BANK TELEPORTER PROCESS START] {start_time.strftime('%Y-%m-%d %H:%M:%S')} - Starting process_and_update")

    # --- Authenticate Google Sheets ---
    try:
        gc = authorize(CREDS_PATH)
    except Exception as e:
        log(f"❌ Error authorizing with creds ({CREDS_PATH}): {e}")
        return

    # --- Open worksheets ---
    try:
        sh_src = gc.open(SRC_SHEET)
        ws_src = sh_src.worksheet(SRC_TAB)
    except Exception as e:
        log(f"❌ Error opening source ({SRC_SHEET}::{SRC_TAB}): {e}")
        return

    try:
        sh_dest = gc.open(DEST_SHEET)
        ws_dest = sh_dest.worksheet(DEST_TAB)
    except Exception as e:
        log(f"❌ Error opening destination ({DEST_SHEET}::{DEST_TAB}): {e}")
        return

    # --- Read source data (A:F), skip header ---
    t0 = time.time()
    try:
        src_vals = safe_get(ws_src, COL_RANGE)
    except Exception as e:
        log(f"❌ Failed to read source range {COL_RANGE}: {e}")
        return
    if not src_vals or len(src_vals) <= 1:
        log("❌ No data to copy (or only header present) in source.")
        return
    src_rows = src_vals[1:]  # skip header
    t1 = time.time()
    log(f"📥 Read source {len(src_rows)} data rows from {SRC_SHEET}::{SRC_TAB} in {t1 - t0:.2f}s")

    # --- Read destination keys (A:B) only to reduce payload ---
    t0 = time.time()
    try:
        dest_vals = safe_get(ws_dest, DEST_KEY_RANGE)
    except Exception as e:
        log(f"❌ Failed to read destination range {DEST_KEY_RANGE}: {e}")
        return
    dest_rows = dest_vals[1:] if dest_vals and len(dest_vals) > 1 else []
    t1 = time.time()
    log(f"📥 Read destination {len(dest_rows)} key rows from {DEST_SHEET}::{DEST_TAB} in {t1 - t0:.2f}s")

    # --- Build set of existing (A,B) keys in dest ---
    dest_keys = set()
    for row in dest_rows:
        a = row[0].strip() if len(row) > 0 and row[0] else ""
        b = row[1].strip() if len(row) > 1 and row[1] else ""
        if a and b:
            dest_keys.add((a, b))

    # --- Identify incremental rows in source (not present in dest) ---
    new_rows = []
    for row in src_rows:
        a = row[0].strip() if len(row) > 0 and row[0] else ""
        b = row[1].strip() if len(row) > 1 and row[1] else ""
        if a and b and (a, b) not in dest_keys:
            # Pad to 6 columns (explicit)
            row_padded = row[:6] + [""] * (6 - len(row[:6]))
            new_rows.append(row_padded)

    if not new_rows:
        log("✅ No new incremental rows to append. Destination up to date.")
        return

    log(f"🔍 Found {len(new_rows)} new incremental rows to append.")



    # --- Ensure enough rows in destination sheet (expand if necessary) ---
    dest_row_count = len(dest_rows) + 1  # +1 for header row
    total_needed_rows = dest_row_count + len(new_rows)
    added_start, added_end = (None, None)
    try:
        added_start, added_end = ensure_sheet_has_rows(ws_dest, total_needed_rows)
    except Exception as e:
        log(f"⚠️ Failed while ensuring enough rows in destination: {e}")
        # Not fatal

    # --- If we just added rows, pre-populate H & I formulas for those new rows ---
    if added_start is not None and added_end is not None and added_end >= added_start:
        # Build the H/I formula grid for the new rows only (row-specific formulas)
        num_new = added_end - added_start + 1
        log(f"🧩 Pre-filling formulas in H:I for newly added rows {added_start}..{added_end}")
    
        # H = =IF(A{r}="","",VLOOKUP(A{r},WEEK!A:C,2,FALSE))
        # I = =IF(A{r}="","",VLOOKUP(A{r},WEEK!A:C,3,FALSE))
        formula_block = [
            [
                f'=IF(A{r}="","",VLOOKUP(A{r},WEEK!A:C,2,FALSE))',
                f'=IF(A{r}="","",VLOOKUP(A{r},WEEK!A:C,3,FALSE))'
            ]
            for r in range(added_start, added_end + 1)
        ]
    
        # One bulk update for H:I over the added rows
        target_range = f"H{added_start}:I{added_end}"
        try:
            ws_dest.update(target_range, formula_block, value_input_option="USER_ENTERED")
            log(f"✅ Formulas filled in {target_range}")
        except Exception as e:
            log(f"⚠️ Failed to fill formulas in {target_range}: {e}")
    
    # ...then proceed to write A:F deterministically as already implemented

    # --- Append new rows deterministically into A:F based on last non-empty row in A–F ---
    log("⬇️  Appending rows in batches (using explicit A:F range).")
    appended_total = 0
    append_call_count = 0
    
    # Find the last used row by scanning only columns A–F
    last_rows = []
    for col in range(1, 7):  # A=1, F=6
        vals = ws_dest.col_values(col)
        if vals:
            last_rows.append(len(vals))
    last_used = max(last_rows) if last_rows else 1  # assume header only
    start_row = last_used + 1
    
    log(f"🧭 Last used row (A–F) = {last_used}, writing new rows starting at {start_row}")
    
    row_ptr = start_row
    for big_chunk in chunked_iter(new_rows, MAX_APPEND_PER_CALL):
        for small_chunk in chunked_iter(big_chunk, BATCH_SIZE):
            end_row = row_ptr + len(small_chunk) - 1
            rng = f"A{row_ptr}:F{end_row}"
            success = False
            last_exc = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    ws_dest.update(rng, small_chunk, value_input_option='USER_ENTERED')
                    appended_total += len(small_chunk)
                    append_call_count += 1
                    success = True
                    break
                except Exception as e:
                    last_exc = e
                    log(f"⚠️  Update attempt {attempt} failed for {rng}: {e}")
                    exponential_backoff_sleep(attempt)
            if not success:
                log(f"❌ Permanent failure writing range {rng}: {last_exc}")
            row_ptr = end_row + 1
    
    log(f"✅ Append complete. {appended_total} rows written via {append_call_count} update call(s).")

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    log(f"[DATE BANK TELEPORTER PROCESS END]✅  {end_time.strftime('%Y-%m-%d %H:%M:%S')} - Finished process_and_update (duration: {elapsed:.2f}s)")
    log("")

if __name__ == "__main__":
    main()
