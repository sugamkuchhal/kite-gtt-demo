import logging

from script_logger import log_start, log_end
from data_val_utils import get_ws, check_gt_threshold


def main():
    sh4, ws4 = get_ws("KWK", "Friday_Identifier")
    check_gt_threshold(sh4.title, ws4, "F1")

    sh5, ws5 = get_ws("PORTFOLIO", "CREDIT_CANDIDATES")
    check_gt_threshold(sh5.title, ws5, "K1")

    sh6, ws6 = get_ws("RTP", "DATE_Identifier")
    check_gt_threshold(sh6.title, ws6, "F1")

    sh7, ws7 = get_ws("HUNDRED", "OPEN_LIST")
    check_gt_threshold(sh7.title, ws7, "F1")

    sh8, ws8 = get_ws("CONSOLIDATED", "OPEN_LIST")
    check_gt_threshold(sh8.title, ws8, "E1")


if __name__ == "__main__":
    _ctx = log_start("algo_data_val")
    try:
        main()
        raise SystemExit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        raise SystemExit(130)
    except Exception:
        logging.exception("algo_data_val failed.")
        raise SystemExit(1)
    finally:
        log_end(_ctx)
