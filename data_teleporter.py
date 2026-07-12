#!/usr/bin/env python3
"""
data_teleporter.py  (v2)

Replicates BANK_NEW rows (keyed by BANK_INC) into CALCULATOR!BANK_FINAL.

What changed from v1:
- BANK_FINAL columns A:B are read ONCE into an in-memory lookup dict.
  No more per-key paged scans: the whole classify step is a dict lookup.
- Verification uses the same in-memory snapshot; zero extra API calls.
- Formula carry-forward (H:I) is done in one batched write per range,
  with a loud warning if the template row is blank.
- --dry-run flag: prints the action plan without touching any sheet.
- API calls: ~4 (read INC, read NEW rows, read FINAL index + template,
  write batch) vs the previous O(keys) reads.

Run: python3 data_teleporter.py [--dry-run]
"""
import argparse
import random
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("data_teleporter")
atexit.register(log_end, _RUN_CTX)

# ── CONFIG ───────────────────────────────────────────────────────────────────
CREDS_PATH      = str(get_creds_path())
ref_sheets_src  = "BANK"
ref_sheets_dest = "CALCULATOR"
TAB_INC         = "BANK_INC"
TAB_NEW         = "BANK_NEW"
TAB_FINAL       = "BANK_FINAL"
LAST_COL        = "G"          # A:G — DATE SYMBOL CLOSE LOW HIGH VOLUME TYPE
FORMULA_COLS    = "H:I"        # carry-forward template from row 2
TEMPLATE_ROW    = 2

BATCH_SIZE      = 500          # rows per Sheets update call
APPEND_CHUNK    = 500
ROW_BUFFER      = 100          # extra rows to pre-add before append
MAX_RETRIES     = 5
BATCH_SLEEP     = 0.15

# ── AUTH ─────────────────────────────────────────────────────────────────────
def _authorize() -> gspread.Client:
    creds = Credentials.from_service_account_file(
        CREDS_PATH,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

# ── RETRY HELPERS ─────────────────────────────────────────────────────────────
def _backoff(attempt: int, base: float = 1.0, cap: float = 30.0) -> None:
    time.sleep(random.uniform(0, min(cap, base * (2 ** (attempt - 1)))))

def _get(ws: gspread.Worksheet, rng: str, **kw) -> List[List[Any]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return ws.get(rng, **kw) or []
        except Exception as e:
            print(f"  ⚠ GET {rng} attempt {attempt}: {e}")
            if attempt == MAX_RETRIES:
                raise
            _backoff(attempt)
    return []

def _update(ws: gspread.Worksheet, rng: str, values: List[List[Any]]) -> None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ws.update(range_name=rng, values=values, value_input_option="USER_ENTERED")
            return
        except Exception as e:
            print(f"  ⚠ UPDATE {rng} attempt {attempt}: {e}")
            if attempt == MAX_RETRIES:
                raise
            _backoff(attempt)

def _append(ws: gspread.Worksheet, rows: List[List[Any]]) -> None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            return
        except Exception as e:
            print(f"  ⚠ APPEND attempt {attempt}: {e}")
            if attempt == MAX_RETRIES:
                raise
            _backoff(attempt)

# ── KEY HELPERS ───────────────────────────────────────────────────────────────
def _key(row: List[Any], a: int = 0, b: int = 1) -> Tuple[str, str]:
    return (
        "" if len(row) <= a or row[a] is None else str(row[a]).strip(),
        "" if len(row) <= b or row[b] is None else str(row[b]).strip(),
    )

def _rows_equal(a: List[Any], b: List[Any]) -> bool:
    n = max(len(a), len(b))
    for i in range(n):
        av = "" if i >= len(a) or a[i] is None else str(a[i]).strip()
        bv = "" if i >= len(b) or b[i] is None else str(b[i]).strip()
        if av != bv:
            return False
    return True

# ── CHUNKED CONTIGUOUS WRITE ──────────────────────────────────────────────────
def _write_contiguous_batches(
    ws: gspread.Worksheet,
    updates: List[Tuple[int, List[Any]]],
) -> None:
    """Write (row_index, row_data) pairs; groups contiguous rows into single calls."""
    if not updates:
        return
    updates = sorted(updates, key=lambda x: x[0])
    # build contiguous groups
    groups: List[List[Tuple[int, List[Any]]]] = []
    grp = [updates[0]]
    for item in updates[1:]:
        if item[0] == grp[-1][0] + 1:
            grp.append(item)
        else:
            groups.append(grp)
            grp = [item]
    groups.append(grp)

    for grp in groups:
        for start in range(0, len(grp), BATCH_SIZE):
            chunk = grp[start: start + BATCH_SIZE]
            r_start = chunk[0][0]
            r_end   = chunk[-1][0]
            rng     = f"A{r_start}:{LAST_COL}{r_end}"
            _update(ws, rng, [row for _, row in chunk])
            time.sleep(BATCH_SLEEP)

# ── MAIN LOGIC ────────────────────────────────────────────────────────────────
def run(dry_run: bool = False) -> None:
    client = _authorize()

    src_id  = resolve_sheet_id(ref_sheets_src)
    dest_id = resolve_sheet_id(ref_sheets_dest)

    src_ss  = client.open_by_key(src_id)
    dest_ss = client.open_by_key(dest_id)

    ws_inc   = src_ss.worksheet(TAB_INC)
    ws_new   = src_ss.worksheet(TAB_NEW)
    ws_final = dest_ss.worksheet(TAB_FINAL)

    # ── STEP 1: read INC keys ─────────────────────────────────────────────
    print(f"[1/5] Reading keys from {TAB_INC}...")
    inc_raw = _get(ws_inc, "A:B")
    # skip header if present
    start_idx = 1 if inc_raw and any(
        str(c).strip().upper() in ("DATE", "SYMBOL") for c in inc_raw[0]
    ) else 0
    inc_keys: List[Tuple[str, str]] = []
    for row in inc_raw[start_idx:]:
        k = _key(row)
        if k[0] or k[1]:
            inc_keys.append(k)

    if not inc_keys:
        print(f"  No keys in {TAB_INC}; nothing to do.")
        return
    print(f"  {len(inc_keys)} keys.")

    # ── STEP 2: read NEW — build lookup ───────────────────────────────────
    print(f"[2/5] Reading {TAB_NEW}...")
    new_raw = _get(ws_new, f"A:{ LAST_COL}")
    new_lookup: Dict[Tuple[str, str], List[Any]] = {}
    for row in new_raw:
        k = _key(row)
        if k[0] or k[1]:
            new_lookup.setdefault(k, row)   # first occurrence wins

    missing_in_new = [k for k in inc_keys if k not in new_lookup]
    if missing_in_new:
        print(f"  ⚠ {len(missing_in_new)} key(s) from {TAB_INC} not found in {TAB_NEW}; skipping.")
    payload: Dict[Tuple[str, str], List[Any]] = {
        k: new_lookup[k] for k in inc_keys if k in new_lookup
    }
    if not payload:
        print("  Nothing to apply after reading source.")
        return
    print(f"  {len(payload)} payload rows.")

    # ── STEP 3: read FINAL index ONCE ────────────────────────────────────
    print(f"[3/5] Building index of {TAB_FINAL}...")
    final_raw = _get(ws_final, "A:B")
    # {(date, symbol): [row_numbers]}  (1-indexed, row 1 = header)
    final_index: Dict[Tuple[str, str], List[int]] = {}
    for row_num, row in enumerate(final_raw, start=1):
        k = _key(row)
        if k[0] or k[1]:
            final_index.setdefault(k, []).append(row_num)
    print(f"  {len(final_index)} distinct keys in {TAB_FINAL}.")

    # ── classify ──────────────────────────────────────────────────────────
    to_overwrite:  List[Tuple[int, List[Any]]] = []
    to_append:     List[List[Any]]             = []
    to_delete:     List[int]                   = []   # duplicate extra rows

    for k, src_row in payload.items():
        dest_rows = final_index.get(k, [])
        if dest_rows:
            canonical = min(dest_rows)
            extras    = sorted(dest_rows)[1:]
            to_delete.extend(extras)
            to_overwrite.append((canonical, src_row))
        else:
            to_append.append(src_row)

    print(f"\n  PLAN → overwrites: {len(to_overwrite)} | "
          f"appends: {len(to_append)} | "
          f"duplicate rows to delete: {len(to_delete)}")

    if dry_run:
        print("\n  DRY RUN — no writes performed.")
        return

    # ── STEP 4: execute ───────────────────────────────────────────────────
    print(f"[4/5] Executing...")

    # 4a delete duplicates (descending so row numbers stay valid)
    if to_delete:
        print(f"  Deleting {len(to_delete)} duplicate row(s)...")
        for r in sorted(set(to_delete), reverse=True):
            try:
                ws_final.delete_rows(r)
                time.sleep(0.05)
            except Exception as e:
                print(f"  ⚠ delete row {r}: {e}")
        # rebuild index after deletes (one fresh read)
        final_raw = _get(ws_final, "A:B")
        final_index = {}
        for row_num, row in enumerate(final_raw, start=1):
            k = _key(row)
            if k[0] or k[1]:
                final_index.setdefault(k, []).append(row_num)
        # re-resolve canonical indices
        to_overwrite = []
        for k, src_row in payload.items():
            dest_rows = final_index.get(k, [])
            if dest_rows:
                to_overwrite.append((min(dest_rows), src_row))

    # 4b overwrites
    if to_overwrite:
        print(f"  Overwriting {len(to_overwrite)} row(s)...")
        _write_contiguous_batches(ws_final, to_overwrite)

    # 4c appends
    append_start_rows: List[int] = []
    if to_append:
        print(f"  Appending {len(to_append)} row(s)...")
        # pre-grow sheet
        needed = ws_final.row_count + len(to_append) + ROW_BUFFER
        if needed > ws_final.row_count:
            ws_final.add_rows(needed - ws_final.row_count)
            time.sleep(0.2)
        for i in range(0, len(to_append), APPEND_CHUNK):
            chunk = to_append[i: i + APPEND_CHUNK]
            before_count = len(_get(ws_final, "A:A"))
            _append(ws_final, chunk)
            append_start_rows.append(before_count + 1)   # first appended row
            time.sleep(BATCH_SLEEP)

    # 4d formula carry-forward H:I
    # Build ranges that need the formula: overwritten rows + appended rows
    formula_rows: List[int] = [r for r, _ in to_overwrite]
    for i, chunk_start in enumerate(append_start_rows):
        chunk_len = min(APPEND_CHUNK, len(to_append) - i * APPEND_CHUNK)
        formula_rows.extend(range(chunk_start, chunk_start + chunk_len))

    if formula_rows:
        try:
            tmpl = _get(ws_final, f"H{TEMPLATE_ROW}:I{TEMPLATE_ROW}",
                        value_render_option="FORMULA")
            tmpl_row = tmpl[0] if tmpl and tmpl[0] else []
            if not any(str(c).strip() for c in tmpl_row):
                print(f"  ⚠ Template row {TEMPLATE_ROW} in H:I is blank — "
                      f"formula carry-forward skipped.")
            else:
                # build contiguous ranges
                formula_rows_sorted = sorted(set(formula_rows))
                ranges: List[Tuple[int, int]] = []
                s = e = formula_rows_sorted[0]
                for r in formula_rows_sorted[1:]:
                    if r == e + 1:
                        e = r
                    else:
                        ranges.append((s, e))
                        s = e = r
                ranges.append((s, e))

                print(f"  Writing H:I formulas into {len(ranges)} range(s)...")
                for rs, re_ in ranges:
                    # only overwrite blank cells
                    existing = _get(ws_final, f"H{rs}:I{re_}",
                                    value_render_option="FORMULA")
                    out_rows = []
                    changed = False
                    for offset in range(re_ - rs + 1):
                        ex = existing[offset] if offset < len(existing) else []
                        h = ex[0] if len(ex) > 0 else ""
                        ii = ex[1] if len(ex) > 1 else ""
                        nh = h if str(h).strip() else (tmpl_row[0] if tmpl_row else "")
                        ni = ii if str(ii).strip() else (tmpl_row[1] if len(tmpl_row) > 1 else "")
                        out_rows.append([nh, ni])
                        if nh != h or ni != ii:
                            changed = True
                    if changed:
                        _update(ws_final, f"H{rs}:I{re_}", out_rows)
                    time.sleep(0.02)
        except Exception as e:
            print(f"  ⚠ Formula carry-forward failed (non-fatal): {e}")

    # ── STEP 5: verify ────────────────────────────────────────────────────
    print(f"[5/5] Verifying...")
    # Re-read the final index snapshot for verification — one read
    final_verify = _get(ws_final, f"A:{ LAST_COL}")
    verify_index: Dict[Tuple[str, str], List[Any]] = {}
    for row in final_verify:
        k = _key(row)
        if k[0] or k[1]:
            verify_index[k] = row

    mismatches = 0
    missing_after = 0
    for k, src_row in payload.items():
        dest_row = verify_index.get(k)
        if dest_row is None:
            print(f"  ✗ key {k} missing after write")
            missing_after += 1
        elif not _rows_equal(src_row, dest_row):
            print(f"  ✗ mismatch for {k}")
            mismatches += 1

    print(f"\n[DONE] overwrites={len(to_overwrite)} appends={len(to_append)} "
          f"dupes_deleted={len(to_delete)} "
          f"verify_missing={missing_after} verify_mismatches={mismatches}")


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Replicate BANK_NEW -> CALCULATOR!BANK_FINAL")
    p.add_argument("--dry-run", action="store_true",
                   help="Print action plan; do not write anything")
    args = p.parse_args()

    start = datetime.now()
    print(f"[START] {start.strftime('%Y-%m-%d %H:%M:%S')}"
          + (" [DRY RUN]" if args.dry_run else ""))
    try:
        run(dry_run=args.dry_run)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(2)
    end = datetime.now()
    print(f"[END] {end.strftime('%Y-%m-%d %H:%M:%S')} "
          f"({(end - start).total_seconds():.1f}s)")
