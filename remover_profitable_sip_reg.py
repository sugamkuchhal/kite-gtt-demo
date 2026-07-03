#!/usr/bin/env python3
"""
remover_profitable_sip_reg.py

Pure sheet actuator for the profitable SIP REG healing process — no
email/Telegram here; comms are owned by algo_tickers_mailer.py, which
imports run_sip_reg().

Step 1 — OLD_SIP_REG_List tab (ref sheet: KWK)
  * Data rows from row 2 (row 1 is a header).
  * Column Q holds the status; exact text "PROFIT" flags a row.
  * For flagged rows, the tickers are taken from column A, then the
    A:E block is compacted upward (flagged rows' A:E dropped, order
    preserved) and trailing A:E cells cleared. Columns F:Q are never
    written to (formulas stay intact and recompute over shifted A:E).

Step 2 — SPECIAL_TARGET_KWK_SIP_REG tab (ref sheet: PORTFOLIO)
  * Column I (data from row 2, row 1 is a header).
  * Cells matching the flagged tickers are dropped; the column is
    compacted upward and trailing cells cleared.
  * Tickers not found in column I are recorded (non-fatal).

Can be run standalone (manual use) or imported by the mailer.
"""

import logging

import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id

# ==========================
# Config (constants)
# ==========================
SIP_REF = "KWK"
SIP_TAB = "OLD_SIP_REG_List"
PROFIT_TEXT = "PROFIT"
SIP_NUM_COLS = 17   # A:Q
SIP_TICKER_COL = 0  # column A (0-indexed)
SIP_STATUS_COL = 16 # column Q (0-indexed)
CLEAR_NUM_COLS = 5  # A:E block that gets compacted

TARGET_REF = "PORTFOLIO"
TARGET_TAB = "SPECIAL_TARGET_KWK_SIP_REG"
TARGET_COL = "I"

SERVICE_CREDS = str(get_creds_path())

# ==========================
# Core logic
# ==========================

def get_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_CREDS, scopes=scope)
    return gspread.authorize(creds)


def _validate_headers(sip_ws, target_ws):
    """Row 1 must carry headers; fail loudly if blank. Q1 is excluded —
    it holds the healing signal formula, not a header."""
    sip_a1 = (sip_ws.acell("A1").value or "").strip()
    target_i1 = (target_ws.acell(f"{TARGET_COL}1").value or "").strip()
    problems = []
    if not sip_a1:
        problems.append(f"{SIP_TAB}!A1 header is blank")
    if not target_i1:
        problems.append(f"{TARGET_TAB}!{TARGET_COL}1 header is blank")
    if problems:
        raise RuntimeError(
            "Header validation failed — sheet layout may have changed: "
            + "; ".join(problems)
        )
    logging.info(
        "Header validation passed: %s!A1=%r; %s!%s1=%r",
        SIP_TAB, sip_a1, TARGET_TAB, TARGET_COL, target_i1,
    )


def process_sip_list(ws):
    """
    Finds rows in OLD_SIP_REG_List where column Q == PROFIT (exact),
    compacts the A:E block upward with those rows dropped, and returns
    the list of flagged tickers (column A).
    """
    values = ws.get_values("A2:Q")
    old_len = len(values)

    keep_ae, flagged = [], []
    for i, row in enumerate(values):
        padded = row + [""] * (SIP_NUM_COLS - len(row))
        ticker = padded[SIP_TICKER_COL].strip()
        status = padded[SIP_STATUS_COL].strip().upper()
        if ticker and status == PROFIT_TEXT:
            flagged.append(ticker)
            logging.info("PROFIT flagged: %s (row %d)", ticker, i + 2)
        else:
            keep_ae.append(padded[:CLEAR_NUM_COLS])

    if not flagged:
        logging.info("No '%s' rows found in %s — nothing to clear.", PROFIT_TEXT, SIP_TAB)
        return []

    if keep_ae:
        ws.update(
            f"A2:E{len(keep_ae) + 1}",
            keep_ae,
            value_input_option="RAW",
        )
    if old_len > len(keep_ae):
        ws.batch_clear([f"A{len(keep_ae) + 2}:E{old_len + 1}"])

    logging.info(
        "%s A:E compacted: %d row(s) cleared, %d row(s) remain.",
        SIP_TAB, len(flagged), len(keep_ae),
    )
    return flagged


def purge_target_column(ws, remove_set):
    """
    Drops cells in SPECIAL_TARGET_KWK_SIP_REG column I matching the
    flagged tickers; compacts the column upward, clears trailing cells.
    """
    rng = f"{TARGET_COL}2:{TARGET_COL}"
    values = ws.get_values(rng)
    old_len = len(values)

    keep, found = [], set()
    for row in values:
        ticker = (row[0] if row else "").strip()
        if ticker in remove_set:
            found.add(ticker)
            continue
        keep.append([ticker])

    not_found = sorted(remove_set - found)
    for t in not_found:
        logging.info("[%s] not found in column %s: %s", TARGET_TAB, TARGET_COL, t)

    purged = old_len - len(keep)
    if purged > 0:
        if keep:
            ws.update(
                f"{TARGET_COL}2:{TARGET_COL}{len(keep) + 1}",
                keep,
                value_input_option="RAW",
            )
        ws.batch_clear([f"{TARGET_COL}{len(keep) + 2}:{TARGET_COL}{old_len + 1}"])
        logging.info(
            "[%s] purged %d cell(s) (%s) from column %s; %d remain.",
            TARGET_TAB, purged, ", ".join(sorted(found)), TARGET_COL, len(keep),
        )
    else:
        logging.info("[%s] no matching cells in column %s — unchanged.", TARGET_TAB, TARGET_COL)

    return {"purged": purged, "not_found": not_found}


def run_sip_reg():
    """
    Full profitable-SIP-REG cycle. Returns a result dict:
      {
        "cleared": [tickers cleared from OLD_SIP_REG_List],
        "target": {"purged": int, "not_found": [...]} | None,
        "error": None | str
      }
    """
    result = {"cleared": [], "target": None, "error": None}

    client = get_client()

    try:
        sip_ws = client.open_by_key(resolve_sheet_id(SIP_REF)).worksheet(SIP_TAB)
        target_ws = client.open_by_key(resolve_sheet_id(TARGET_REF)).worksheet(TARGET_TAB)
        _validate_headers(sip_ws, target_ws)
        flagged = process_sip_list(sip_ws)
    except Exception as e:
        logging.exception("%s processing failed: %s", SIP_TAB, e)
        result["error"] = f"{SIP_TAB} processing failed: {e}"
        return result

    result["cleared"] = flagged
    if not flagged:
        return result

    try:
        result["target"] = purge_target_column(target_ws, set(flagged))
    except Exception as e:
        logging.exception("[%s] purge failed: %s", TARGET_TAB, e)
        result["error"] = f"{TARGET_TAB} purge failed: {e}"

    logging.info("Profitable SIP REG processing complete.")
    return result

# ==========================
# Standalone entry point
# ==========================

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    from script_logger import log_start, log_end
    ctx = log_start("remover_profitable_sip_reg")
    try:
        result = run_sip_reg()
    finally:
        log_end(ctx)

    if result["error"]:
        raise SystemExit(1)
    if not result["cleared"]:
        logging.info("Nothing to clear — done.")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
