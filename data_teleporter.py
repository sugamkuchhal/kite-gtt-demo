#!/usr/bin/env python3
"""
data_teleporter.py  (v3)

Two modes:

  --mode inc   Incremental: keys from BANK!BANK_INC, rows from BANK!BANK_NEW,
               upsert into CALCULATOR!BANK_FINAL.

  --mode full  Full copy: all rows from BANK!BANK_FINAL -> CALCULATOR!BANK_FINAL.
               Clears A2:F first (keeps header + H:I), then rewrites.

Both modes copy A:F only (DATE SYMBOL CLOSE LOW HIGH VOLUME).

Write layer: raw Google Sheets API batchUpdate.
  - One batchUpdate call carries RANGES_PER_BATCH ValueRange objects.
  - Each ValueRange covers ROWS_PER_RANGE rows.
  - Cost against the quota: 1 write request per batchUpdate call.
  - At 1M rows: ~5 batchUpdate calls total.

Read layer: raw Sheets API values.get (single call per range, no paging
needed — Sheets returns up to 10M cells per call).

--dry-run: prints the plan without any writes.
"""
import argparse
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

import atexit
from script_logger import log_start, log_end

_RUN_CTX = log_start("data_teleporter")
atexit.register(log_end, _RUN_CTX)

# ── CONFIG ────────────────────────────────────────────────────────────────────
CREDS_PATH       = str(get_creds_path())
ref_sheets_src   = "BANK"
ref_sheets_dest  = "CALCULATOR"
TAB_INC          = "BANK_INC"
TAB_NEW          = "BANK_NEW"
TAB_FINAL_SRC    = "BANK_FINAL"
TAB_FINAL_DEST   = "BANK_FINAL"
LAST_COL         = "F"          # A:F — DATE SYMBOL CLOSE LOW HIGH VOLUME
TEMPLATE_ROW     = 2            # H:I formula template row

# Batching — tuned for quota and scale
ROWS_PER_RANGE   = 5000         # rows per ValueRange in a batchUpdate
RANGES_PER_BATCH = 20           # ValueRanges per batchUpdate call (= 1 quota unit)
# => rows per batchUpdate call = 100,000
# => 1M rows = 10 batchUpdate calls
BATCH_SLEEP      = 2.0          # seconds between batchUpdate calls

MAX_RETRIES      = 5
RETRY_BASE_SECS  = 4.0          # exponential backoff base (doubles each attempt)
RETRY_CAP_SECS   = 120.0

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── AUTH ──────────────────────────────────────────────────────────────────────
def _build_service():
    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ── RETRY ─────────────────────────────────────────────────────────────────────
def _with_retry(fn, label: str):
    """Call fn(); retry up to MAX_RETRIES on HttpError with exponential backoff."""
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except HttpError as e:
            last = e
            wait = min(RETRY_CAP_SECS, RETRY_BASE_SECS * (2 ** (attempt - 1)))
            print(f"  ⚠ {label} attempt {attempt}/{MAX_RETRIES}: "
                  f"HTTP {e.status_code} — retrying in {wait:.0f}s")
            time.sleep(wait)
        except Exception as e:
            last = e
            wait = min(RETRY_CAP_SECS, RETRY_BASE_SECS * (2 ** (attempt - 1)))
            print(f"  ⚠ {label} attempt {attempt}/{MAX_RETRIES}: {e} — retrying in {wait:.0f}s")
            time.sleep(wait)
    raise RuntimeError(f"{label} failed after {MAX_RETRIES} attempts: {last}")


# ── RAW API HELPERS ───────────────────────────────────────────────────────────
def _read(svc, spreadsheet_id: str, range_: str) -> List[List[Any]]:
    """Read a range; returns list of rows (may be shorter than range if trailing blanks)."""
    resp = _with_retry(
        lambda: svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_,
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute(),
        f"read {range_}",
    )
    return resp.get("values", [])


def _clear(svc, spreadsheet_id: str, range_: str) -> None:
    """Clear a range (values only; formatting preserved)."""
    _with_retry(
        lambda: svc.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_,
        ).execute(),
        f"clear {range_}",
    )


def _batch_write(svc, spreadsheet_id: str,
                 value_ranges: List[Dict]) -> None:
    """
    Issue ONE batchUpdate call containing multiple ValueRange objects.
    Cost: 1 write request against the Sheets quota, regardless of size.
    """
    _with_retry(
        lambda: svc.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": value_ranges,
            },
        ).execute(),
        "batchUpdate",
    )


def _write_all_rows(svc, spreadsheet_id: str, tab: str,
                    rows: List[List[Any]], start_row: int = 2) -> None:
    """
    Write rows to tab starting at start_row, using batched batchUpdate calls.
    Prints progress every batchUpdate call.
    """
    if not rows:
        return
    total = len(rows)
    rows_per_call = ROWS_PER_RANGE * RANGES_PER_BATCH
    call_num = 0
    total_calls = (total + rows_per_call - 1) // rows_per_call

    for call_start in range(0, total, rows_per_call):
        call_rows = rows[call_start: call_start + rows_per_call]
        value_ranges = []
        for rng_start in range(0, len(call_rows), ROWS_PER_RANGE):
            chunk = call_rows[rng_start: rng_start + ROWS_PER_RANGE]
            abs_row = start_row + call_start + rng_start
            value_ranges.append({
                "range": f"'{tab}'!A{abs_row}:{LAST_COL}{abs_row + len(chunk) - 1}",
                "values": chunk,
            })
        call_num += 1
        rows_done = call_start + len(call_rows)
        print(f"  batchUpdate {call_num}/{total_calls}: "
              f"rows {call_start + start_row}–{rows_done + start_row - 1} "
              f"({rows_done:,}/{total:,})")
        _batch_write(svc, spreadsheet_id, value_ranges)
        if call_start + rows_per_call < total:
            time.sleep(BATCH_SLEEP)


# ── ROW HELPERS ───────────────────────────────────────────────────────────────
def _key(row: List[Any]) -> Tuple[str, str]:
    return (
        "" if not row or row[0] is None else str(row[0]).strip(),
        "" if len(row) < 2 or row[1] is None else str(row[1]).strip(),
    )


def _trim(row: List[Any]) -> List[Any]:
    """Exactly 6 columns (A:F); pad with '' if shorter."""
    r = [str(c) if c is not None else "" for c in row[:6]]
    while len(r) < 6:
        r.append("")
    return r


def _rows_equal(a: List[Any], b: List[Any]) -> bool:
    n = max(len(a), len(b))
    for i in range(n):
        av = "" if i >= len(a) or a[i] is None else str(a[i]).strip()
        bv = "" if i >= len(b) or b[i] is None else str(b[i]).strip()
        if av != bv:
            return False
    return True


# ── FORMULA CARRY-FORWARD ─────────────────────────────────────────────────────
def _carry_formulas(svc, dest_id: str, row_nums: List[int]) -> None:
    """Copy H:I template into blank H:I cells for the given rows."""
    if not row_nums:
        return
    try:
        tmpl = _read(svc, dest_id,
                     f"'{TAB_FINAL_DEST}'!H{TEMPLATE_ROW}:I{TEMPLATE_ROW}")
        tmpl_row = tmpl[0] if tmpl and tmpl[0] else []
        if not any(str(c).strip() for c in tmpl_row):
            print(f"  ⚠ Template H{TEMPLATE_ROW}:I{TEMPLATE_ROW} blank "
                  "— formula carry-forward skipped.")
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
        existing = _read(svc, dest_id,
                         f"'{TAB_FINAL_DEST}'!H{rs}:I{re_}")
        out, changed = [], False
        for offset in range(re_ - rs + 1):
            ex = existing[offset] if offset < len(existing) else []
            h  = ex[0] if ex else ""
            i_ = ex[1] if len(ex) > 1 else ""
            nh = h  if str(h).strip()  else (tmpl_row[0] if tmpl_row else "")
            ni = i_ if str(i_).strip() else (tmpl_row[1] if len(tmpl_row) > 1 else "")
            out.append([nh, ni])
            if nh != h or ni != i_:
                changed = True
        if changed:
            _batch_write(svc, dest_id, [{
                "range": f"'{TAB_FINAL_DEST}'!H{rs}:I{re_}",
                "values": out,
            }])
        time.sleep(0.1)


# ── INCREMENTAL MODE ──────────────────────────────────────────────────────────
def run_inc(svc, src_id: str, dest_id: str, dry_run: bool) -> None:
    # 1. read INC keys
    print(f"[1/5] Reading keys from {TAB_INC}...")
    inc_raw = _read(svc, src_id, f"'{TAB_INC}'!A:B")
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
    new_raw = _read(svc, src_id, f"'{TAB_NEW}'!A:{LAST_COL}")
    new_lookup: Dict[Tuple[str, str], List[Any]] = {}
    for row in new_raw:
        k = _key(row)
        if k[0] or k[1]:
            new_lookup.setdefault(k, _trim(row))
    missing = [k for k in inc_keys if k not in new_lookup]
    if missing:
        print(f"  ⚠ {len(missing)} key(s) not found in {TAB_NEW}; skipping.")
    payload = {k: new_lookup[k] for k in inc_keys if k in new_lookup}
    if not payload:
        print("  Nothing to apply.")
        return
    print(f"  {len(payload)} payload rows.")

    # 3. read FINAL index (A:B only — one read)
    print(f"[3/5] Building index of {TAB_FINAL_DEST}...")
    final_raw = _read(svc, dest_id, f"'{TAB_FINAL_DEST}'!A:B")
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
            to_delete.extend(sorted(dest_rows)[1:])
            to_overwrite.append((min(dest_rows), src_row))
        else:
            to_append.append(src_row)

    print(f"\n  PLAN → overwrites: {len(to_overwrite)} | "
          f"appends: {len(to_append)} | dupes to delete: {len(to_delete)}")
    if dry_run:
        print("  DRY RUN — no writes.")
        return

    # 4. execute
    print("[4/5] Executing...")

    # delete duplicates (rare; use Sheets API delete rows)
    if to_delete:
        print(f"  Deleting {len(to_delete)} duplicate row(s)...")
        import gspread
        from google.oauth2.service_account import Credentials as _Creds
        _creds = _Creds.from_service_account_file(CREDS_PATH, scopes=SCOPES)
        _gc = gspread.authorize(_creds)
        _ws = _gc.open_by_key(dest_id).worksheet(TAB_FINAL_DEST)
        for r in sorted(set(to_delete), reverse=True):
            try:
                _ws.delete_rows(r)
                time.sleep(0.05)
            except Exception as e:
                print(f"  ⚠ delete row {r}: {e}")
        # rebuild index
        final_raw = _read(svc, dest_id, f"'{TAB_FINAL_DEST}'!A:B")
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

    # overwrites: group into batchUpdate value_ranges
    if to_overwrite:
        print(f"  Overwriting {len(to_overwrite)} row(s)...")
        sorted_ow = sorted(to_overwrite, key=lambda x: x[0])
        value_ranges = [
            {
                "range": f"'{TAB_FINAL_DEST}'!A{r}:{LAST_COL}{r}",
                "values": [row],
            }
            for r, row in sorted_ow
        ]
        # send in batches of RANGES_PER_BATCH
        for i in range(0, len(value_ranges), RANGES_PER_BATCH):
            chunk = value_ranges[i: i + RANGES_PER_BATCH]
            _batch_write(svc, dest_id, chunk)
            if i + RANGES_PER_BATCH < len(value_ranges):
                time.sleep(BATCH_SLEEP)

    # appends
    append_start = 0
    if to_append:
        print(f"  Appending {len(to_append)} row(s)...")
        current_count = len(_read(svc, dest_id, f"'{TAB_FINAL_DEST}'!A:A"))
        append_start = current_count + 1
        _write_all_rows(svc, dest_id, TAB_FINAL_DEST, to_append,
                        start_row=append_start)

    # formula carry-forward
    formula_rows = [r for r, _ in to_overwrite]
    if to_append and append_start:
        formula_rows += list(range(append_start, append_start + len(to_append)))
    if formula_rows:
        print("  Carrying H:I formulas...")
        _carry_formulas(svc, dest_id, formula_rows)

    # 5. verify (one read)
    print("[5/5] Verifying...")
    verify_raw = _read(svc, dest_id, f"'{TAB_FINAL_DEST}'!A:{LAST_COL}")
    verify_index = {}
    for row in verify_raw:
        k = _key(row)
        if k[0] or k[1]:
            verify_index[k] = _trim(row)
    mismatches = sum(
        1 for k, src in payload.items()
        if k not in verify_index or not _rows_equal(src, verify_index[k])
    )
    print(f"\n[DONE] overwrites={len(to_overwrite)} appends={len(to_append)} "
          f"dupes_deleted={len(to_delete)} mismatches={mismatches}")


# ── FULL MODE ─────────────────────────────────────────────────────────────────
def run_full(svc, src_id: str, dest_id: str, dry_run: bool) -> None:
    # 1. read source BANK_FINAL A:F
    print(f"[1/3] Reading source {TAB_FINAL_SRC} (A:{LAST_COL})...")
    src_raw = _read(svc, src_id, f"'{TAB_FINAL_SRC}'!A:{LAST_COL}")
    start = 1 if src_raw and any(
        str(c).strip().upper() in ("DATE", "SYMBOL") for c in src_raw[0]
    ) else 0
    rows = [_trim(row) for row in src_raw[start:]
            if any(str(c).strip() for c in row)]
    print(f"  {len(rows):,} data rows.")

    rows_per_call = ROWS_PER_RANGE * RANGES_PER_BATCH
    total_calls   = (len(rows) + rows_per_call - 1) // rows_per_call
    print(f"  Write plan: {total_calls} batchUpdate call(s) × "
          f"up to {rows_per_call:,} rows each "
          f"(~{total_calls * BATCH_SLEEP:.0f}s at {BATCH_SLEEP}s/call).")

    if dry_run:
        print(f"\n  PLAN → clear A2:{LAST_COL}, write {len(rows):,} rows "
              f"in {total_calls} batchUpdate call(s).")
        print("  DRY RUN — no writes.")
        return

    # 2. clear A2:F (one API call)
    print(f"[2/3] Clearing A2:{LAST_COL} in destination {TAB_FINAL_DEST}...")
    _clear(svc, dest_id, f"'{TAB_FINAL_DEST}'!A2:{LAST_COL}")
    time.sleep(0.5)

    # 3. write all rows
    print(f"[3/3] Writing {len(rows):,} rows...")
    _write_all_rows(svc, dest_id, TAB_FINAL_DEST, rows, start_row=2)

    print(f"\n[DONE] {len(rows):,} rows written to {TAB_FINAL_DEST}.")


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Teleport BANK data to CALCULATOR")
    p.add_argument("--mode", required=True, choices=["inc", "full"],
                   help="inc = incremental | full = full copy")
    p.add_argument("--dry-run", action="store_true",
                   help="Print plan; do not write anything")
    args = p.parse_args()

    start = datetime.now()
    print(f"[START] {start.strftime('%Y-%m-%d %H:%M:%S')} "
          f"mode={args.mode}" + (" [DRY RUN]" if args.dry_run else ""))
    try:
        svc    = _build_service()
        src_id = resolve_sheet_id(ref_sheets_src)
        dest_id = resolve_sheet_id(ref_sheets_dest)
        if args.mode == "inc":
            run_inc(svc, src_id, dest_id, args.dry_run)
        else:
            run_full(svc, src_id, dest_id, args.dry_run)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(2)
    end = datetime.now()
    print(f"[END] {end.strftime('%Y-%m-%d %H:%M:%S')} "
          f"({(end - start).total_seconds():.1f}s)")
