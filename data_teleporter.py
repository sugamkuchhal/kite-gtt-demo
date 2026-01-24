#!/usr/bin/env python3
"""
Standalone data_teleporter runner (replicates BANK_NEW changes to destination BANK_FINAL).
Run: python3 data_teleporter.py
"""

import time
import random
import sys
from datetime import datetime
from typing import List, Tuple, Dict, Any
import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path

# ---------------- CONFIG ----------------
CREDS_PATH = str(get_creds_path())

# Source spreadsheet URL (ETL destination that contains BANK_INC and BANK_NEW)
SOURCE_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1TX4Q8YG0-d2_L1YOhvb9OYDgklvHj3eFK76JN7Pdavg"

# Destination spreadsheet name (as in existing data_teleporter config)
DEST_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1IZJYejcWZN72f_3Fm1L2IHgbjVxOthTfqwnsnynCcXk"

# Sheet/tab names (as we discussed)
INC_SHEET = "BANK_INC"
NEW_SHEET = "BANK_NEW"
FINAL_SHEET = "BANK_FINAL"

# Tuning
DEFAULT_PAGE_SIZE = 10000
DEFAULT_BATCH_UPDATE_SIZE = 500
DEFAULT_APPEND_CHUNK = 500
ROW_BUFFER = 100
BATCH_SLEEP = 0.15
SAMPLE_VERIFICATION_COUNT = 100
MAX_RETRIES = 5

# ----------------- helpers -----------------
def log(msg=""):
    print(msg, flush=True)

def backoff_sleep(attempt, base=1.0, cap=30.0):
    exp = min(cap, base * (2 ** (attempt - 1)))
    time.sleep(random.uniform(0, exp))

def authorize(creds_path: str = CREDS_PATH):
    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def gsheet_get(ws, rng, value_render_option=None, retries=MAX_RETRIES):
    last = None
    for attempt in range(1, retries + 1):
        try:
            if value_render_option:
                return ws.get(rng, value_render_option=value_render_option) or []
            return ws.get(rng) or []
        except Exception as e:
            last = e
            log(f"⚠️ GET {rng} attempt {attempt} failed: {e}")
            backoff_sleep(attempt)
    raise last

def gsheet_update(ws, rng, values, value_input_option="USER_ENTERED", retries=MAX_RETRIES):
    last = None
    for attempt in range(1, retries + 1):
        try:
            ws.update(rng, values, value_input_option=value_input_option)
            return
        except Exception as e:
            last = e
            log(f"⚠️ UPDATE {rng} attempt {attempt} failed: {e}")
            backoff_sleep(attempt)
    raise last

def ensure_rows(ws, required_rows: int):
    current = ws.row_count
    if required_rows > current:
        to_add = required_rows - current
        log(f"🔧 Adding {to_add} rows to destination (had {current}, need {required_rows})")
        ws.add_rows(to_add)
        time.sleep(0.2)

def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]

def normalize_key(a, b):
    a = "" if a is None else str(a).strip()
    b = "" if b is None else str(b).strip()
    return (a, b)

def _group_contiguous(sorted_indices: List[int]) -> List[Tuple[int,int]]:
    if not sorted_indices:
        return []
    groups = []
    start = prev = sorted_indices[0]
    for idx in sorted_indices[1:]:
        if idx == prev + 1:
            prev = idx
        else:
            groups.append((start, prev))
            start = prev = idx
    groups.append((start, prev))
    return groups

def rows_equal(a: List[Any], b: List[Any]) -> bool:
    a = a or []
    b = b or []
    maxlen = max(len(a), len(b))
    for i in range(maxlen):
        av = "" if i >= len(a) or a[i] is None else str(a[i]).strip()
        bv = "" if i >= len(b) or b[i] is None else str(b[i]).strip()
        if av != bv:
            return False
    return True

# ----------------- domain functions -----------------
def read_inc_keys(client: gspread.client.Client, source_spreadsheet_url: str, inc_sheet_name: str,
                  col_range: str = "A:B") -> List[Tuple[str,str]]:
    ss = client.open_by_url(source_spreadsheet_url)
    ws = ss.worksheet(inc_sheet_name)
    vals = ws.get(col_range) or []
    if not vals:
        return []
    first_row = vals[0]
    header_like = any((str(x).strip().upper() in ("DATE","SYMBOL") for x in first_row))
    start_idx = 1 if header_like else 0
    keys = []
    for row in vals[start_idx:]:
        a = row[0] if len(row) > 0 else ""
        b = row[1] if len(row) > 1 else ""
        k = normalize_key(a,b)
        if k[0] or k[1]:
            keys.append(k)
    return keys

def find_keys_in_sheet_paged(client: gspread.client.Client, spreadsheet_url: str, sheet_name: str,
                             keys_set: List[Tuple[str,str]], page_size: int = DEFAULT_PAGE_SIZE) -> Dict[Tuple[str,str], List[int]]:
    ss = client.open_by_url(spreadsheet_url)
    ws = ss.worksheet(sheet_name)
    total_rows = ws.row_count
    keys_needed = set(keys_set)
    found: Dict[Tuple[str,str], List[int]] = {}
    start = 1
    while start <= total_rows and keys_needed:
        end = min(total_rows, start + page_size - 1)
        rng = f"A{start}:B{end}"
        try:
            page_vals = ws.get(rng) or []
        except Exception as e:
            log(f"[WARN] page read {rng} failed: {e}")
            page_vals = []
        for idx, row in enumerate(page_vals, start=start):
            date_val = row[0] if len(row) > 0 else ""
            symbol_val = row[1] if len(row) > 1 else ""
            k = normalize_key(date_val, symbol_val)
            if k in keys_needed:
                found.setdefault(k, []).append(idx)
                keys_needed.discard(k)
        # stop after current page if done
        if not keys_needed:
            break
        start = end + 1
    return found

def read_rows_by_indices(client: gspread.client.Client, spreadsheet_url: str, sheet_name: str,
                         row_indices: List[int], last_col: str = "G") -> Dict[int, List[Any]]:
    if not row_indices:
        return {}
    ss = client.open_by_url(spreadsheet_url)
    ws = ss.worksheet(sheet_name)
    idxs_sorted = sorted(set(row_indices))
    groups = _group_contiguous(idxs_sorted)
    res = {}
    for start, end in groups:
        rng = f"A{start}:{last_col}{end}"
        block = ws.get(rng) or []
        for offset, row in enumerate(block):
            res[start + offset] = row
    return res

def delete_rows_descending(client: gspread.client.Client, spreadsheet_url: str, sheet_name: str, row_indices: List[int]):
    if not row_indices:
        return
    ss = client.open_by_url(spreadsheet_url)
    ws = ss.worksheet(sheet_name)
    unique_desc = sorted(set(row_indices), reverse=True)
    for r in unique_desc:
        try:
            ws.delete_rows(r)
            time.sleep(0.05)
        except Exception as e:
            log(f"[WARN] delete row {r} failed: {e}")

def batch_overwrite_rows(client: gspread.client.Client, spreadsheet_url: str, sheet_name: str,
                         updates: List[Tuple[int, List[Any]]], batch_size: int = DEFAULT_BATCH_UPDATE_SIZE, last_col: str = "G"):
    if not updates:
        return
    ss = client.open_by_url(spreadsheet_url)
    ws = ss.worksheet(sheet_name)
    updates_sorted = sorted(updates, key=lambda x: x[0])
    groups = []
    current = []
    prev = None
    for idx, vals in updates_sorted:
        if prev is None or idx == prev + 1:
            current.append((idx, vals))
        else:
            groups.append(current)
            current = [(idx, vals)]
        prev = idx
    if current:
        groups.append(current)
    for grp in groups:
        indices = [i for i,_ in grp]
        rows_block = [v for _,v in grp]
        total = len(rows_block)
        s_idx = 0
        while s_idx < total:
            e_idx = min(total, s_idx + batch_size)
            chunk_rows = rows_block[s_idx:e_idx]
            chunk_start = indices[s_idx]
            chunk_end = indices[s_idx + len(chunk_rows) - 1]
            rng = f"A{chunk_start}:{last_col}{chunk_end}"
            try:
                ws.update(rng, chunk_rows, value_input_option="USER_ENTERED")
            except Exception as e:
                log(f"[ERROR] overwrite {rng} failed: {e}")
                raise
            s_idx = e_idx
            time.sleep(BATCH_SLEEP)

def batch_append_rows(client: gspread.client.Client, spreadsheet_url: str, sheet_name: str,
                      rows_to_append: List[List[Any]], append_chunk: int = DEFAULT_APPEND_CHUNK):
    if not rows_to_append:
        return
    ss = client.open_by_url(spreadsheet_url)
    ws = ss.worksheet(sheet_name)
    i = 0
    total = len(rows_to_append)
    while i < total:
        chunk = rows_to_append[i:i+append_chunk]
        try:
            ws.append_rows(chunk, value_input_option="USER_ENTERED")
        except Exception as e:
            log(f"[ERROR] append chunk failed: {e}")
            raise
        i += append_chunk
        time.sleep(BATCH_SLEEP)

def copy_formulas_hi_where_blank(client: gspread.client.Client, spreadsheet_url: str, sheet_name: str,
                                 target_ranges: List[Tuple[int,int]]):
    if not target_ranges:
        return
    ss = client.open_by_url(spreadsheet_url)
    ws = ss.worksheet(sheet_name)
    try:
        template = ws.get("H2:I2", value_render_option="FORMULA") or [["",""]]
        template_row = template[0] if template and template[0] else ["",""]
    except Exception as e:
        log(f"[WARN] read template H2:I2 failed: {e}")
        return
    for start, end in target_ranges:
        rng = f"H{start}:I{end}"
        existing = ws.get(rng, value_render_option="FORMULA") or []
        rows_to_write = []
        write_any = False
        for offset in range(end - start + 1):
            existing_row = existing[offset] if offset < len(existing) else []
            h = existing_row[0] if len(existing_row) > 0 else ""
            i = existing_row[1] if len(existing_row) > 1 else ""
            new_h = h
            new_i = i
            if not str(h).strip():
                new_h = template_row[0] if len(template_row) > 0 else ""
            if not str(i).strip():
                new_i = template_row[1] if len(template_row) > 1 else ""
            rows_to_write.append([new_h, new_i])
            if new_h != h or new_i != i:
                write_any = True
        if write_any:
            try:
                ws.update(rng, rows_to_write, value_input_option="USER_ENTERED")
            except Exception as e:
                log(f"[ERROR] write formulas {rng} failed: {e}")
                raise
        time.sleep(0.02)

# ----------------- orchestrator -----------------
def replicate_bank_new_to_dest(creds_path: str,
                               source_spreadsheet_url: str,
                               dest_spreadsheet_url: str,
                               inc_sheet_name: str = INC_SHEET,
                               new_sheet_name: str = NEW_SHEET,
                               final_sheet_name: str = FINAL_SHEET,
                               page_size: int = DEFAULT_PAGE_SIZE,
                               batch_update_size: int = DEFAULT_BATCH_UPDATE_SIZE,
                               append_chunk: int = DEFAULT_APPEND_CHUNK):
    client = authorize(creds_path)
    # 1) read inc keys
    inc_keys = read_inc_keys(client, source_spreadsheet_url, inc_sheet_name, col_range="A:B")
    if not inc_keys:
        log("[INFO] no keys in BANK_INC; nothing to do.")
        return
    log(f"[INFO] {len(inc_keys)} keys read from {inc_sheet_name}")

    # 2) locate keys in source NEW
    log("[INFO] locating keys in source BANK_NEW...")
    src_found = find_keys_in_sheet_paged(client, source_spreadsheet_url, new_sheet_name, inc_keys, page_size=page_size)
    missing = [k for k in inc_keys if k not in src_found]
    if missing:
        log(f"[WARN] {len(missing)} keys from BANK_INC not found in BANK_NEW; skipping those.")
    # pick canonical source index per key
    key_to_src_index = {}
    src_indices = []
    for k, idxs in src_found.items():
        canonical = min(idxs)
        key_to_src_index[k] = canonical
        src_indices.append(canonical)
    process_keys = [k for k in inc_keys if k in key_to_src_index]
    if not process_keys:
        log("[INFO] no payload rows found; exiting.")
        return

    # 3) read source full rows
    log(f"[INFO] reading {len(src_indices)} source rows from BANK_NEW ...")
    src_rows_map = read_rows_by_indices(client, source_spreadsheet_url, new_sheet_name, src_indices, last_col="G")
    payload_map = {}
    for k in process_keys:
        idx = key_to_src_index[k]
        vals = src_rows_map.get(idx)
        if not vals:
            log(f"[WARN] expected source row {idx} for key {k} missing; skipping")
            continue
        payload_map[k] = vals

    if not payload_map:
        log("[INFO] nothing to apply after reading source; exiting.")
        return

    # 4) locate keys in destination
    log("[INFO] locating keys in destination BANK_FINAL ...")
    dest_found = find_keys_in_sheet_paged(client, dest_spreadsheet_url, final_sheet_name, list(payload_map.keys()), page_size=page_size)

    to_overwrite = []
    to_append = []
    duplicates_to_delete = []
    overwrite_ranges_for_formula = []
    append_ranges_for_formula = []

    # Build action plan
    for key, src_row in payload_map.items():
        dest_idxs = dest_found.get(key, [])
        if dest_idxs:
            dest_idxs_sorted = sorted(set(dest_idxs))
            canonical = dest_idxs_sorted[0]
            extras = dest_idxs_sorted[1:]
            if extras:
                duplicates_to_delete.extend(extras)
            to_overwrite.append((canonical, src_row))
            overwrite_ranges_for_formula.append((canonical, canonical))
        else:
            to_append.append(src_row)

    log(f"[PLAN] overwrites: {len(to_overwrite)}, appends: {len(to_append)}, duplicates_to_delete: {len(duplicates_to_delete)}")

    # 5) delete duplicates descending
    if duplicates_to_delete:
        log("[ACTION] deleting duplicate extra rows (descending indices)...")
        delete_rows_descending(client, dest_spreadsheet_url, final_sheet_name, duplicates_to_delete)
        # re-locate canonical indices after deletes
        log("[INFO] re-locating canonical indices after deletes...")
        dest_found_after = find_keys_in_sheet_paged(client, dest_spreadsheet_url, final_sheet_name, list(payload_map.keys()), page_size=page_size)
        new_overwrite = []
        overwrite_ranges_for_formula = []
        for key, src_row in payload_map.items():
            dest_idxs = dest_found_after.get(key, [])
            if dest_idxs:
                canonical = min(dest_idxs)
                new_overwrite.append((canonical, src_row))
                overwrite_ranges_for_formula.append((canonical, canonical))
        to_overwrite = new_overwrite
        log(f"[INFO] confirmed {len(to_overwrite)} overwrite targets after cleanup.")

    # 6) ensure rows for appends
    ss_dest = client.open_by_url(dest_spreadsheet_url)
    ws_dest = ss_dest.worksheet(final_sheet_name)
    dest_row_count = ws_dest.row_count
    projected = dest_row_count + len(to_append) + ROW_BUFFER
    ensure_rows(ws_dest, projected)

    # 7) perform overwrites
    if to_overwrite:
        log(f"[ACTION] performing {len(to_overwrite)} overwrites...")
        batch_overwrite_rows(client, dest_spreadsheet_url, final_sheet_name, to_overwrite, batch_size=batch_update_size, last_col="G")

    # 8) perform appends (and capture appended ranges for formula copy)
    if to_append:
        log(f"[ACTION] appending {len(to_append)} rows...")
        # Do chunked append; after each chunk attempt to determine appended range
        ssd = client.open_by_url(dest_spreadsheet_url)
        wsd = ssd.worksheet(final_sheet_name)
        i = 0
        while i < len(to_append):
            chunk = to_append[i:i+append_chunk]

            # --- Retry append with backoff ---
            last_err = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    wsd.append_rows(chunk, value_input_option="USER_ENTERED")
                    break
                except Exception as e:
                    last_err = e
                    log(f"[WARN] append_rows attempt {attempt} failed: {e}")
                    backoff_sleep(attempt)
            if last_err and attempt == MAX_RETRIES:
                raise last_err

            # --- Determine where rows were appended (with retry for tail read) ---
            total_rows_now = wsd.row_count
            tail_start = max(1, total_rows_now - (len(chunk) + 200))

            tail = None
            last_err = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    tail = wsd.get(f"A{tail_start}:B{total_rows_now}") or []
                    break
                except Exception as e:
                    last_err = e
                    log(f"[WARN] tail get attempt {attempt} failed: {e}")
                    backoff_sleep(attempt)
            if tail is None and last_err:
                raise last_err

            # --- Map appended rows back to their positions ---
            tail_map = {}
            for offset, r in enumerate(tail):
                rownum = tail_start + offset
                d = r[0] if len(r) > 0 else ""
                s = r[1] if len(r) > 1 else ""
                tail_map[normalize_key(d, s)] = rownum

            mapped_rows = []
            for r in chunk:
                k = normalize_key(
                    r[0] if len(r) > 0 else "",
                    r[1] if len(r) > 1 else ""
                )
                if k in tail_map:
                    mapped_rows.append(tail_map[k])

            if mapped_rows:
                append_start = min(mapped_rows)
                append_end = max(mapped_rows)
                append_ranges_for_formula.append((append_start, append_end))

            i += append_chunk
            time.sleep(BATCH_SLEEP)

    # 9) merge ranges and copy formulas H:I where blank
    combined = overwrite_ranges_for_formula + append_ranges_for_formula
    if combined:
        combined_sorted = sorted(combined, key=lambda x: x[0])
        merged = []
        cs, ce = combined_sorted[0]
        for s,e in combined_sorted[1:]:
            if s <= ce + 1:
                ce = max(ce, e)
            else:
                merged.append((cs, ce))
                cs, ce = s, e
        merged.append((cs, ce))
        log(f"[ACTION] re-applying H:I formulas into {len(merged)} ranges (only where blank)...")
        copy_formulas_hi_where_blank(client, dest_spreadsheet_url, final_sheet_name, merged)

    # 10) sample verification
    sample_keys = list(payload_map.keys())
    import random as _rnd
    if sample_keys:
        sample = _rnd.sample(sample_keys, min(SAMPLE_VERIFICATION_COUNT, len(sample_keys)))
    else:
        sample = []
    mismatches = 0
    for k in sample:
        dest_map_now = find_keys_in_sheet_paged(client, dest_spreadsheet_url, final_sheet_name, [k], page_size=page_size)
        dest_idxs_now = dest_map_now.get(k, [])
        if not dest_idxs_now:
            log(f"[ERROR] verification: key {k} missing in destination after write.")
            mismatches += 1
            continue
        canonical = min(dest_idxs_now)
        ss = client.open_by_url(dest_spreadsheet_url)
        ws = ss.worksheet(final_sheet_name)
        rng = f"A{canonical}:G{canonical}"
        dest_row = (ws.get(rng) or [[]])[0] if (ws.get(rng) or [[]]) else []
        src_row = payload_map.get(k)
        if not rows_equal(dest_row, src_row):
            log(f"[ERROR] verification mismatch for {k} at dest row {canonical}")
            mismatches += 1

    log("[DONE] replication complete.")
    log(f"  overwrites: {len(to_overwrite)}, appends: {len(to_append)}, duplicates_deleted: {len(duplicates_to_delete)}, verification_mismatches: {mismatches}")
    if missing:
        log(f"  missing_keys_count (in INC but not in NEW): {len(missing)}")

# ----------------- main entry -----------------
if __name__ == "__main__":
    start = datetime.now()
    log(f"[REPLICATE RUN START] {start.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        replicate_bank_new_to_dest(
            creds_path=CREDS_PATH,
            source_spreadsheet_url=SOURCE_SPREADSHEET_URL,
            dest_spreadsheet_url=DEST_SPREADSHEET_URL,
            inc_sheet_name=INC_SHEET,
            new_sheet_name=NEW_SHEET,
            final_sheet_name=FINAL_SHEET,
            page_size=DEFAULT_PAGE_SIZE,
            batch_update_size=DEFAULT_BATCH_UPDATE_SIZE,
            append_chunk=DEFAULT_APPEND_CHUNK
        )
    except Exception as e:
        log(f"[ERROR] replicate run failed: {e}")
        sys.exit(2)
    end = datetime.now()
    log(f"[REPLICATE RUN END] {end.strftime('%Y-%m-%d %H:%M:%S')} (duration: {(end-start).total_seconds():.2f}s)")
