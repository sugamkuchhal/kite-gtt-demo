import logging

from ref_sheets_utils import resolve_sheet_id
from script_logger import log_start, log_end
from date_ext_utils import get_client, get_ws, init_date


def main():
    sh4, ws4 = get_ws("KWK", "Friday_Identifier")
    try:
        changed = init_date(sh4.title, ws4, "B1", ws4, "A2")
        gc = get_client()
        flag_ws = gc.open_by_key(resolve_sheet_id("PORTFOLIO")).worksheet("ALL_OLD_GTTs")
        flag_ws.update(range_name="R1", values=[[bool(changed)]])
    except Exception:
        try:
            gc = get_client()
            flag_ws = gc.open_by_key(resolve_sheet_id("PORTFOLIO")).worksheet("ALL_OLD_GTTs")
            flag_ws.update(range_name="R1", values=[[False]])
        except Exception:
            pass

    sh5, ws5 = get_ws("PORTFOLIO", "CREDIT_CANDIDATES")
    init_date(sh5.title, ws5, "K24", ws5, "K23")

    sh6, ws6 = get_ws("RTP", "DATE_Identifier")
    init_date(sh6.title, ws6, "B1", ws6, "A2")

    sh7, ws7 = get_ws("HUNDRED", "OPEN_LIST")
    init_date(sh7.title, ws7, "B1", ws7, "A2")

    sh8, ws8 = get_ws("CONSOLIDATED", "OPEN_LIST")
    init_date(sh8.title, ws8, "B1", ws8, "A2")


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
