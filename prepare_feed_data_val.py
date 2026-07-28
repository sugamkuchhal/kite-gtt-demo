import logging

import atexit
from script_logger import log_start, log_end
from data_val_utils import get_ws, check_gt_threshold

_RUN_CTX = log_start("prepare_feed_data_val")
atexit.register(log_end, _RUN_CTX)


def main():
    sh1, ws1 = get_ws("FEED", "SGST_OPEN_LIST")
    check_gt_threshold(sh1.title, ws1, "G1")

    sh2, ws2 = get_ws("FEED", "SUPER_OPEN_LIST")
    check_gt_threshold(sh2.title, ws2, "G1")

    sh3, ws3 = get_ws("FEED", "TURTLE_OPEN_LIST")
    check_gt_threshold(sh3.title, ws3, "G1")


if __name__ == "__main__":
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
    except Exception:
        logging.exception("prepare_feed_data_val failed.")
        raise SystemExit(1)
