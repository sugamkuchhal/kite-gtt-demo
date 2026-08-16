import logging
import time

import atexit
from script_logger import log_start, log_end
from date_ext_utils import get_ws, init_date

_RUN_CTX = log_start("prepare_feed_date_ext")
atexit.register(log_end, _RUN_CTX)


LOOP_INTERVAL = 70  # seconds between iterations


def run_until_done(ref_sheets, tab_name):
    while True:
        sh, ws = get_ws(ref_sheets, tab_name)
        result = init_date(sh.title, ws, "B1", ws, "A2")
        if result is None:
            # Future date — no more copying possible, stop
            break
        print(f"{tab_name} -> ⏳ Next iteration in {LOOP_INTERVAL}s...")
        time.sleep(LOOP_INTERVAL)


def main():
    run_until_done("FEED", "SGST_OPEN_LIST")
    run_until_done("FEED", "SUPER_OPEN_LIST")
    run_until_done("FEED", "TURTLE_OPEN_LIST")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
    except Exception:
        logging.exception("prepare_feed_date_ext failed.")
        raise SystemExit(1)
