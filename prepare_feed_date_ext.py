import logging

import atexit
from script_logger import log_start, log_end
from date_ext_utils import get_ws, init_date

_RUN_CTX = log_start("prepare_feed_date_ext")
atexit.register(log_end, _RUN_CTX)


def main():
    sh1, ws1 = get_ws("FEED", "SGST_OPEN_LIST")
    init_date(sh1.title, ws1, "B1", ws1, "A2")

    sh2, ws2 = get_ws("FEED", "SUPER_OPEN_LIST")
    init_date(sh2.title, ws2, "B1", ws2, "A2")

    sh3, ws3 = get_ws("FEED", "TURTLE_OPEN_LIST")
    init_date(sh3.title, ws3, "B1", ws3, "A2")


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
