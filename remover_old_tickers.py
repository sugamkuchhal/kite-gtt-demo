#!/usr/bin/env python3
"""
remover_old_tickers.py

Pure sheet actuator for the removals process — no email/Telegram here;
comms are owned by algo_checklist_mailer.py, which imports run_removals().

Detection — Master_Live tab (ref sheet: TICKER)
  * Cell H2 holds a formula-driven count. When > 0, healers are triggered.
  * H3 and down contain the tickers to be removed.

Step 2 — FEED sheet tabs (ref sheet: FEED)
  * Tabs: SGST_FILTERED_TICKERS, SUPER_FILTERED_TICKERS,
    TURTLE_FILTERED_TICKERS — all share the A:C layout
    DATE | TICKER | CATEGORY (data from row 2).
  * Any row whose column B ticker is in the removal list is dropped;
    the A:C block is compacted upward and trailing rows cleared.
  * Tickers not found in a tab are recorded (non-fatal).
  * A failure on one tab is recorded and the remaining tabs still run,
    so the report always reflects the full picture.

Step 3 — NSE text lists (nse_stock_list.txt / nse_etf_list.txt)
  * Exchange prefix is stripped first (NSE:RELIANCE -> RELIANCE).
  * Each stripped ticker is looked up in both files; matching lines are
    removed, the file is written back, and committed via git_utils.
  * A ticker not found in either file is recorded (non-fatal).
  * A file-write failure is recorded and the other file still runs.

Can be run standalone (manual use) or imported by the mailer.
"""

import logging
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

from runtime_paths import get_creds_path, repo_root
from ref_sheets_utils import resolve_sheet_id
from git_utils import commit_file_if_changed

# ==========================
# Config (constants)
# ==========================
MASTER_LIVE_REF = "TICKER"
MASTER_LIVE_TAB = "Master_Live"
MASTER_LIVE_SIGNAL_CELL = "H2"
MASTER_LIVE_TICKER_START = "H3"

FEED_REF = "FEED"
FEED_TABS = [
    "SGST_FILTERED_TICKERS",
    "SUPER_FILTERED_TICKERS",
    "TURTLE_FILTERED_TICKERS",
]
FEED_NUM_COLS = 3  # A:C -> DATE | TICKER | CATEGORY
FEED_TICKER_COL = 1  # column B (0-indexed)

NSE_LIST_FILES = [
    "nse_stock_list.txt",
    "nse_etf_list.txt",
]

SERVICE_CREDS = str(get_creds_path())

# ==========================
# Core logic
# ==========================

def get_client():
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_CREDS, scopes=scope)
    return gspread.authorize(creds)


def read_master_live_tickers(client):
    """
    Reads tickers to remove from Master_Live!H3 downward (ref sheet: TICKER).
    Returns a list of non-empty ticker strings.
    """
    sheet_id = resolve_sheet_id(MASTER_LIVE_REF)
    ws = client.open_by_key(sheet_id).worksheet(MASTER_LIVE_TAB)
    values = ws.get_values(f"{MASTER_LIVE_TICKER_START}:H")
    removed = [row[0].strip() for row in values if row and row[0].strip()]
    logging.info(
        "Read %d ticker(s) from %s!%s: %s",
        len(removed), MASTER_LIVE_TAB, MASTER_LIVE_TICKER_START, ", ".join(removed),
    )
    return removed


def purge_feed_tab(client, sheet_id, tab_name, remove_set):
    """
    Drops rows from A2:C whose column B ticker is in remove_set,
    compacts the block upward and clears trailing rows.

    Returns a result dict for reporting.
    """
    ws = client.open_by_key(sheet_id).worksheet(tab_name)
    values = ws.get_values("A2:C")
    old_len = len(values)

    keep_rows = []
    found = set()
    for row in values:
        padded = row + [""] * (FEED_NUM_COLS - len(row))
        ticker = padded[FEED_TICKER_COL].strip()
        if ticker in remove_set:
            found.add(ticker)
            continue
        keep_rows.append(padded[:FEED_NUM_COLS])

    not_found = sorted(remove_set - found)
    for t in not_found:
        logging.info("[%s] not found in tab: %s", tab_name, t)

    purged = old_len - len(keep_rows)
    if purged > 0:
        if keep_rows:
            ws.update(
                f"A2:C{len(keep_rows) + 1}",
                keep_rows,
                value_input_option="RAW",
            )
        ws.batch_clear([f"A{len(keep_rows) + 2}:C{old_len + 1}"])
        logging.info(
            "[%s] purged %d row(s) (%s); %d row(s) remain.",
            tab_name, purged, ", ".join(sorted(found)), len(keep_rows),
        )
    else:
        logging.info("[%s] no matching rows — tab unchanged.", tab_name)

    return {
        "tab": tab_name,
        "purged": purged,
        "purged_tickers": sorted(found),
        "not_found": not_found,
        "remaining": len(keep_rows),
        "error": None,
    }


def _strip_exchange(ticker: str) -> str:
    """Strip exchange prefix: 'NSE:RELIANCE' -> 'RELIANCE', 'RELIANCE' -> 'RELIANCE'."""
    return ticker.split(":")[-1].strip()


def purge_nse_lists(tickers: list) -> list:
    """
    Removes stripped tickers from nse_stock_list.txt and nse_etf_list.txt.

    Each ticker has its exchange prefix stripped before matching
    (e.g. NSE:RELIANCE -> RELIANCE). Each file is written back and
    committed via git_utils if changed.

    Returns a list of per-file result dicts:
      {
        "file":            filename (str),
        "remove_set":      stripped tickers looked up (set),
        "purged":          count of lines removed (int),
        "purged_tickers":  tickers actually found and removed (sorted list),
        "not_found":       tickers not present in this file (sorted list),
        "committed":       True if git commit was made (bool),
        "error":           None | str
      }
    """
    stripped = {_strip_exchange(t) for t in tickers}
    logging.info("NSE list purge — looking for: %s", ", ".join(sorted(stripped)))

    root = repo_root()
    results = []

    for filename in NSE_LIST_FILES:
        filepath = root / filename
        res = {
            "file": filename,
            "remove_set": stripped,
            "purged": 0,
            "purged_tickers": [],
            "not_found": [],
            "committed": False,
            "error": None,
        }

        try:
            lines = filepath.read_text(encoding="utf-8").splitlines()
            keep, found = [], set()
            for line in lines:
                symbol = line.strip()
                if symbol in stripped:
                    found.add(symbol)
                else:
                    keep.append(line)

            not_found = sorted(stripped - found)
            res["purged"] = len(found)
            res["purged_tickers"] = sorted(found)
            res["not_found"] = not_found

            for t in not_found:
                logging.info("[%s] '%s' not found — skipping.", filename, t)

            if found:
                filepath.write_text("\n".join(keep) + ("\n" if keep else ""), encoding="utf-8")
                logging.info(
                    "[%s] removed %d ticker(s): %s; %d line(s) remain.",
                    filename, len(found), ", ".join(sorted(found)), len(keep),
                )
                committed = commit_file_if_changed(
                    filepath,
                    f"healer: remove {', '.join(sorted(found))} from {filename}",
                    repo_root=root,
                )
                res["committed"] = committed
            else:
                logging.info("[%s] no matching tickers — file unchanged.", filename)

        except Exception as e:
            logging.exception("[%s] purge failed: %s", filename, e)
            res["error"] = str(e)

        results.append(res)

    return results


def run_removals():
    """
    Full removals cycle. Returns a result dict:
      {
        "removed":   [tickers read from Master_Live!H3:H],
        "tabs":      [per-tab result dicts (see purge_feed_tab)],
        "nse_lists": [per-file result dicts (see purge_nse_lists)],
        "error":     None | str  (fatal error reading ticker list, if any)
      }
    A per-tab or per-file failure is recorded in that entry's result dict
    and the remaining steps still run.
    """
    result = {"removed": [], "tabs": [], "nse_lists": [], "error": None}

    client = get_client()

    try:
        removed = read_master_live_tickers(client)
    except Exception as e:
        logging.exception("Master_Live ticker read failed: %s", e)
        result["error"] = f"Master_Live ticker read failed: {e}"
        return result

    result["removed"] = removed
    if not removed:
        logging.info("No tickers found in Master_Live!%s — nothing to remove.", MASTER_LIVE_TICKER_START)
        return result

    logging.info("Tickers to purge from FEED tabs: %s", ", ".join(removed))
    remove_set = set(removed)

    feed_sheet_id = resolve_sheet_id(FEED_REF)
    for tab in FEED_TABS:
        try:
            result["tabs"].append(
                purge_feed_tab(client, feed_sheet_id, tab, remove_set)
            )
        except Exception as e:
            logging.exception("[%s] purge failed: %s", tab, e)
            result["tabs"].append({
                "tab": tab,
                "purged": 0,
                "purged_tickers": [],
                "not_found": [],
                "remaining": None,
                "error": str(e),
            })

    # Step 3: purge NSE text lists
    logging.info("Purging tickers from NSE text lists...")
    try:
        result["nse_lists"] = purge_nse_lists(removed)
    except Exception as e:
        logging.exception("NSE list purge failed: %s", e)
        result["nse_lists"] = [{"file": f, "error": str(e)} for f in NSE_LIST_FILES]

    logging.info("Removals processing complete.")
    return result

# ==========================
# Standalone entry point
# ==========================

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    from script_logger import log_start, log_end
    ctx = log_start("remover_old_tickers")
    try:
        result = run_removals()
    finally:
        log_end(ctx)

    if result["error"]:
        raise SystemExit(1)
    if not result["removed"]:
        logging.info("Nothing to remove — done.")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
