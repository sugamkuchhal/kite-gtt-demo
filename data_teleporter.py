#!/usr/bin/env python3
"""
data_teleporter.py  (v2)

Two modes, one script:

  --mode inc   (incremental) — reads keys from BANK!BANK_INC, fetches
               matching rows from BANK!BANK_NEW, upserts into
               CALCULATOR!BANK_FINAL. Only the delta is touched.

  --mode full  — reads ALL rows from BANK!BANK_FINAL (A:F), clears
               A2:F in CALCULATOR!BANK_FINAL (keeps header + H:I),
               then writes all rows in batches.

Both modes copy columns A:F only (DATE SYMBOL CLOSE LOW HIGH VOLUME).
--dry-run works with either mode: prints the plan without any writes.

Run:
  python3 data_teleporter.py --mode inc  [--dry-run]
  python3 data_teleporter.py --mode full [--dry-run]
"""
import argparse
import random
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("data_teleporter")
atexit.register(log_end, _RUN_CTX)

# ── CONFIG ────────────────────────────────────────────────────────────────────
CREDS_PATH      = str(get_creds_path())
ref_sheets_src  = "BANK"
ref_sheets_dest = "CALCULATOR"
TAB_INC         = "BANK_INC"
TAB_NEW         = "BANK_NEW"
TAB_FINAL_SRC   = "BANK_FINAL"   # source for full mode
TAB_FINAL_DEST  = "BANK_FINAL"   # destination for both modes
LAST_COL        = "F"            # A:F — DATE SYMBOL CLOSE LOW HIGH VOLUME
FORMULA_COLS    = "H:I"          # carry-forward template from row 2
TEMPLATE_ROW    = 2

BATCH_SIZE      = 2000         # 240k rows / 2000 = ~120 calls; well under 60/min at 1.5s sleep
APPEND_CHUNK    = 2000
ROW_BUFFER      = 100
MAX_RETRIES     = 5
BATCH_SLEEP     = 1.5          # ~40 write calls/min; Sheets quota is 60/min

# ── AUTH ──────────────────────────────────────────────────────────────────────
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
def _backoff(attempt: int, base: float = 2.0, cap: float = 60.0) -> None:
    t = min(cap, base * (2 ** (attempt - 1)))
    print(f"  ↻ backing off {t:.0f}s before retry...")
    time.sleep(t)

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

# ── ROW HELPERS ───────────────────────────────────────────────────────────────
def _key(row: List[Any]) -> Tuple[str, str]:
    return (
        "" if not row or row[0] is None else str(row[0]).strip(),
        "" if len(row) < 2 or row[1] is None else str(row[1]).strip(),
    )

def _rows_equal(a: List[Any], b: List[Any]) -> bool:
    n = max(len(a), len(b))
    for i in range(n):
        av = "" if i >= len(a) or a[i] is None else str(a[i]).strip()
        bv = "" if i >= len(b) or b[i] is None else str(b[i]).strip()
        if av != bv:
            return False
    return True

def _trim_to_f(row: List[Any]) -> List[Any]:
    """Return only the first 6 columns (A:F), padding with '' if shorter."""
    r = list(row[:6])
    while len(r) < 6:
        r.append("")
    return r

# ── BATCH WRITE (contiguous grouping) ────────────────────────────────────────
def _write_batches(ws: gspread.Worksheet,
                   updates: List[Tuple[int, List[Any]]]) -> None:
    if not updates:
        return
    updates = sorted(updates, key=lambda x: x[0])
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
        for s in range(0, len(grp), BATCH_SIZE):
            chunk = grp[s: s + BATCH_SIZE]
            r0, r1 = chunk[0][0], chunk[-1][0]
            _update(ws, f"A{r0}:{LAST_COL}{r1}", [row for _, row in chunk])
            time.sleep(BATCH_SLEEP)

# ── FORMULA CARRY-FORWARD ─────────────────────────────────────────────────────
def _carry_formulas(ws: gspread.Worksheet, row_nums: List[int]) -> None:
    """Copy H:I template into any blank H:I cells in the given rows."""
    if not row_nums:
        return
    try:
        tmpl = _get(ws, f"H{TEMPLATE_ROW}:I{TEMPLATE_ROW}",
                    value_render_option="FORMULA")
        tmpl_row = tmpl[0] if tmpl and tmpl[0] else []
        if not any(str(c).strip() for c in tmpl_row):
            print(f"  ⚠ Template H{TEMPLATE_ROW}:I{TEMPLATE_ROW} is blank "
                  f"— formula carry-forward skipped.")
            return
    except Exception as e:
        print(f"  ⚠ Could not read formula template: {e}")
        return

    # build contiguous ranges
    rows_sorted = sorted(set(row_nums))
    ranges: List[Tuple[int, int]] = []
    s = e = rows_sorted[0]
    for r in rows_sorted[1:]:
        if r == e + 1:
            e = r
        else:
            ranges.append((s, e))
            s = e = r
    ranges.append((s, e))

    for rs, re_ in ranges:
        existing = _get(ws, f"H{rs}:I{re_}", value_render_option="FORMULA")
        out, changed = [], False
        for offset in range(re_ - rs + 1):
            ex = existing[offset] if offset < len(existing) else []
            h  = ex[0] if ex else ""
            i  = ex[1] if len(ex) > 1 else ""
            nh = h if str(h).strip() else (tmpl_row[0] if tmpl_row else "")
            ni = i if str(i).strip() else (tmpl_row[1] if len(tmpl_row) > 1 else "")
            out.append([nh, ni])
            if nh != h or ni != i:
                changed = True
        if changed:
            _update(ws, f"H{rs}:I{re_}", out)
        time.sleep(0.02)

# ── INCREMENTAL MODE ──────────────────────────────────────────────────────────
def run_inc(ws_inc, ws_new, ws_final, dry_run: bool) -> None:
    # 1. read INC keys
    print(f"[1/5] Reading keys from {TAB_INC}...")
    inc_raw = _get(ws_inc, "A:B")
    start = 1 if inc_raw and any(
        str(c).strip().upper() in ("DATE", "SYMBOL") for c in inc_raw[0]
    ) else 0
    inc_keys = []
    for row in inc_raw[start:]:
        k = _key(row)
        if k[0] or k[1]:
            inc_keys.append(k)
    if not inc_keys:
        print(f"  No keys in {TAB_INC}; nothing to do.")
        return
    print(f"  {len(inc_keys)} keys.")

    # 2. read NEW → lookup
    print(f"[2/5] Reading {TAB_NEW}...")
    new_raw = _get(ws_new, f"A:{LAST_COL}")
    new_lookup: Dict[Tuple[str, str], List[Any]] = {}
    for row in new_raw:
        k = _key(row)
        if k[0] or k[1]:
            new_lookup.setdefault(k, _trim_to_f(row))
    missing = [k for k in inc_keys if k not in new_lookup]
    if missing:
        print(f"  ⚠ {len(missing)} key(s) from {TAB_INC} not in {TAB_NEW}; skipping.")
    payload = {k: new_lookup[k] for k in inc_keys if k in new_lookup}
    if not payload:
        print("  Nothing to apply.")
        return
    print(f"  {len(payload)} payload rows.")

    # 3. read FINAL index once
    print(f"[3/5] Building index of {TAB_FINAL_DEST}...")
    final_raw = _get(ws_final, "A:B")
    final_index: Dict[Tuple[str, str], List[int]] = {}
    for row_num, row in enumerate(final_raw, start=1):
        k = _key(row)
        if k[0] or k[1]:
            final_index.setdefault(k, []).append(row_num)
    print(f"  {len(final_index)} distinct keys in {TAB_FINAL_DEST}.")

    # classify
    to_overwrite: List[Tuple[int, List[Any]]] = []
    to_append:    List[List[Any]]             = []
    to_delete:    List[int]                   = []
    for k, src_row in payload.items():
        dest_rows = final_index.get(k, [])
        if dest_rows:
            canonical = min(dest_rows)
            to_delete.extend(sorted(dest_rows)[1:])
            to_overwrite.append((canonical, src_row))
        else:
            to_append.append(src_row)

    print(f"\n  PLAN → overwrites: {len(to_overwrite)} | "
          f"appends: {len(to_append)} | "
          f"dupes to delete: {len(to_delete)}")
    if dry_run:
        print("  DRY RUN — no writes.")
        return

    # 4. execute
    print("[4/5] Executing...")
    if to_delete:
        print(f"  Deleting {len(to_delete)} duplicate row(s)...")
        for r in sorted(set(to_delete), reverse=True):
            try:
                ws_final.delete_rows(r)
                time.sleep(0.05)
            except Exception as e:
                print(f"  ⚠ delete row {r}: {e}")
        # rebuild index after deletes
        final_raw = _get(ws_final, "A:B")
        final_index = {}
        for row_num, row in enumerate(final_raw, start=1):
            k = _key(row)
            if k[0] or k[1]:
                final_index.setdefault(k, []).append(row_num)
        to_overwrite = [
            (min(final_index[k]), src_row)
            for k, src_row in payload.items()
            if k in final_index
        ]

    if to_overwrite:
        print(f"  Overwriting {len(to_overwrite)} row(s)...")
        _write_batches(ws_final, to_overwrite)

    append_formula_rows: List[int] = []
    if to_append:
        print(f"  Appending {len(to_append)} row(s)...")
        needed = ws_final.row_count + len(to_append) + ROW_BUFFER
        if needed > ws_final.row_count:
            ws_final.add_rows(needed - ws_final.row_count)
            time.sleep(0.2)
        for i in range(0, len(to_append), APPEND_CHUNK):
            chunk = to_append[i: i + APPEND_CHUNK]
            before = len(_get(ws_final, "A:A"))
            _append(ws_final, chunk)
            append_formula_rows.extend(range(before + 1, before + len(chunk) + 1))
            time.sleep(BATCH_SLEEP)

    all_formula_rows = [r for r, _ in to_overwrite] + append_formula_rows
    if all_formula_rows:
        print(f"  Carrying H:I formulas...")
        _carry_formulas(ws_final, all_formula_rows)

    # 5. verify
    print("[5/5] Verifying...")
    verify_raw = _get(ws_final, f"A:{LAST_COL}")
    verify_index = {}
    for row in verify_raw:
        k = _key(row)
        if k[0] or k[1]:
            verify_index[k] = _trim_to_f(row)
    mismatches = sum(
        1 for k, src in payload.items()
        if k not in verify_index or not _rows_equal(src, verify_index[k])
    )
    print(f"\n[DONE] overwrites={len(to_overwrite)} appends={len(to_append)} "
          f"dupes_deleted={len(to_delete)} mismatches={mismatches}")

# ── FULL MODE ─────────────────────────────────────────────────────────────────
def run_full(ws_src_final, ws_final, dry_run: bool) -> None:
    # 1. read entire source BANK_FINAL A:F (skip header row 1)
    print(f"[1/3] Reading all rows from source {TAB_FINAL_SRC} (A:{LAST_COL})...")
    src_raw = _get(ws_src_final, f"A:{LAST_COL}")
    # skip header if present
    start = 1 if src_raw and any(
        str(c).strip().upper() in ("DATE", "SYMBOL") for c in src_raw[0]
    ) else 0
    rows = [_trim_to_f(row) for row in src_raw[start:]
            if any(str(c).strip() for c in row)]
    print(f"  {len(rows)} data rows to copy.")

    if dry_run:
        print(f"\n  PLAN → clear A2:{LAST_COL} in destination, "
              f"write {len(rows)} rows.")
        print("  DRY RUN — no writes.")
        return

    # 2. clear destination A2:F (keeps header + H:I untouched)
    print(f"[2/3] Clearing A2:{LAST_COL} in destination {TAB_FINAL_DEST}...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            ws_final.batch_clear([f"A2:{LAST_COL}"])
            break
        except Exception as e:
            print(f"  ⚠ clear attempt {attempt}: {e}")
            if attempt == MAX_RETRIES:
                raise
            _backoff(attempt)
    time.sleep(0.3)

    # 3. write all rows in batches from row 2
    print(f"[3/3] Writing {len(rows)} rows...")
    needed = len(rows) + ROW_BUFFER + 1   # +1 for header
    if needed > ws_final.row_count:
        ws_final.add_rows(needed - ws_final.row_count)
        time.sleep(0.2)

    formula_rows: List[int] = []
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i: i + BATCH_SIZE]
        r_start = i + 2           # row 1 is the header
        r_end   = r_start + len(chunk) - 1
        _update(ws_final, f"A{r_start}:{LAST_COL}{r_end}", chunk)
        formula_rows.extend(range(r_start, r_end + 1))
        time.sleep(BATCH_SLEEP)

    if formula_rows:
        print("  Carrying H:I formulas...")
        _carry_formulas(ws_final, formula_rows)

    print(f"\n[DONE] {len(rows)} rows written to {TAB_FINAL_DEST}.")

# ── ENTRY POINT ───────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Teleport BANK data to CALCULATOR")
    p.add_argument("--mode", required=True, choices=["inc", "full"],
                   help="inc = incremental (BANK_INC/NEW -> CALC BANK_FINAL); "
                        "full = full copy (BANK BANK_FINAL -> CALC BANK_FINAL)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print plan; do not write anything")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    start = datetime.now()
    print(f"[START] {start.strftime('%Y-%m-%d %H:%M:%S')} "
          f"mode={args.mode}" + (" [DRY RUN]" if args.dry_run else ""))
    try:
        client   = _authorize()
        src_id   = resolve_sheet_id(ref_sheets_src)
        dest_id  = resolve_sheet_id(ref_sheets_dest)
        src_ss   = client.open_by_key(src_id)
        dest_ss  = client.open_by_key(dest_id)
        ws_final = dest_ss.worksheet(TAB_FINAL_DEST)

        if args.mode == "inc":
            run_inc(
                ws_inc=src_ss.worksheet(TAB_INC),
                ws_new=src_ss.worksheet(TAB_NEW),
                ws_final=ws_final,
                dry_run=args.dry_run,
            )
        else:
            run_full(
                ws_src_final=src_ss.worksheet(TAB_FINAL_SRC),
                ws_final=ws_final,
                dry_run=args.dry_run,
            )
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(2)
    end = datetime.now()
    print(f"[END] {end.strftime('%Y-%m-%d %H:%M:%S')} "
          f"({(end - start).total_seconds():.1f}s)")
