import logging
import time

from ref_sheets_utils import resolve_sheet_id
from script_logger import log_start, log_end
from date_ext_utils import get_ws, init_date
from google_sheets_utils import get_gsheet_client as get_client

LOOP_INTERVAL = 70  # seconds between iterations


def write_r1(value):
    try:
        gc = get_client()
        flag_ws = gc.open_by_key(resolve_sheet_id("PORTFOLIO")).worksheet("ALL_OLD_GTTs")
        flag_ws.update(range_name="R1", values=[[value]])
    except Exception:
        try:
            gc = get_client()
            flag_ws = gc.open_by_key(resolve_sheet_id("PORTFOLIO")).worksheet("ALL_OLD_GTTs")
            flag_ws.update(range_name="R1", values=[[False]])
        except Exception:
            pass


def run_until_done(ref_sheets, tab_name, src_cell="B1", dest_cell="A2"):
    while True:
        sh, ws = get_ws(ref_sheets, tab_name)
        result = init_date(sh.title, ws, src_cell, ws, dest_cell)
        if result is None:
            break
        print(f"{tab_name} -> ⏳ Next iteration in {LOOP_INTERVAL}s...")
        time.sleep(LOOP_INTERVAL)


def main():
    # KWK/Friday_Identifier — checks A2 before and after to determine if changed
    while True:
        sh4, ws4 = get_ws("KWK", "Friday_Identifier")
        try:
            before = ws4.acell("A2").value
            result = init_date(sh4.title, ws4, "B1", ws4, "A2")
            after = ws4.acell("A2").value
            changed = (after != before)
            write_r1(changed)
        except Exception:
            write_r1(False)
            result = None
        if result is None:
            break
        print(f"Friday_Identifier -> ⏳ Next iteration in {LOOP_INTERVAL}s...")
        time.sleep(LOOP_INTERVAL)

    run_until_done("PORTFOLIO", "CREDIT_CANDIDATES", src_cell="K24", dest_cell="K23")
    run_until_done("RTP", "DATE_Identifier")
    run_until_done("HUNDRED", "OPEN_LIST")
    run_until_done("CONSOLIDATED", "OPEN_LIST")


if __name__ == "__main__":
    _ctx = log_start("algo_date_ext")
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
    except Exception:
        logging.exception("algo_date_ext failed.")
        raise SystemExit(1)
    finally:
        log_end(_ctx)
