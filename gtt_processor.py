from kiteconnect import KiteConnect, exceptions as kite_exceptions
from kite_session import get_kite
from fetch_google_gtt_instructions import fetch_gtt_instructions_batch, get_instructions_sheet
from fetch_google_existing_gtts import fetch_existing_gtts_batch, get_tracking_sheet
# --- Batch size: single source of truth from config.py ---
try:
    import config
except Exception as e:
    raise SystemExit("Missing required module `config`. Please provide config.py with BATCH_SIZE defined.") from e

if not hasattr(config, "BATCH_SIZE"):
    raise SystemExit("config.BATCH_SIZE is not defined. Set BATCH_SIZE in config.py (no fallback).")

try:
    BATCH_SIZE = int(config.BATCH_SIZE)
    if BATCH_SIZE <= 0:
        raise ValueError("BATCH_SIZE must be a positive integer")
except Exception as e:
    raise SystemExit(f"config.BATCH_SIZE is invalid: {e}")
# --- End batch size setup ---

from google_sheets_utils import get_gsheet_client

import logging
import traceback
import time
import datetime
import hashlib
import random
import socket
import datetime


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logger.info(f"Using BATCH_SIZE={BATCH_SIZE}")

# --------------------------- Extracted Functions --------------------------

def resolve_order_variety():
    now = datetime.datetime.now().time()
    market_open = datetime.time(9, 15)
    market_close = datetime.time(15, 30)

    if market_open <= now <= market_close:
        return "regular"
    return "amo"



def process_place(
    status_manager, row_num, matches, exchange, symbol, side, quantity,
    limit_price, trigger_price, method, kite, last_price_float,
    update_status, failed_rows
):
    if matches:
        update_status(status_manager, row_num, "⚠️ duplicate found")
        return

    order = {
        "exchange": exchange,
        "tradingsymbol": symbol,
        "transaction_type": side,
        "quantity": quantity,
        "order_type": "LIMIT",
        "product": "CNC",
        "price": round(limit_price, 2),
        "validity": "DAY",
        "disclosed_quantity": 0,
        "trigger_price": round(trigger_price, 2),
        "tag": method,
    }

    logger.debug(f"Placing GTT for row {row_num}: symbol={symbol}, exchange={exchange}, trigger_price={trigger_price:.2f}, last_price={last_price_float:.2f}")

    order_display = order.copy()
    order_display['price'] = f"{order['price']:.2f}"
    order_display['trigger_price'] = f"{order['trigger_price']:.2f}"

    try:
        gtt_resp = safe_api_call(
            kite.place_gtt,
            trigger_type="single",
            tradingsymbol=symbol,
            exchange=exchange,
            trigger_values=[trigger_price],
            last_price=last_price_float,
            orders=[order],
        )
        logger.debug(f"Raw GTT response for row {row_num}: {gtt_resp}")
        gtt_id = gtt_resp.get("trigger_id")
        if not gtt_id:
            raise Exception(f"GTT response missing ID. Response: {gtt_resp}")
    except Exception as e:
        update_status(status_manager, row_num, f"❌ error: {e}")
        failed_rows.append({"row_number": row_num, "reason": str(e)})
        return

    update_status(status_manager, row_num, "✅ placed")

def process_update(
    status_manager, row_num, matches, quantity, trigger_price, symbol, exchange, side,
    limit_price, method, kite, last_price_float, update_status, failed_rows, conflict_rows, logger
):
    if len(matches) == 1:
        matched_row = matches[0]

        raw_gtt_id = matched_row.get("GTT_ID", "")
        gtt_id = normalize_gtt_id(raw_gtt_id)
        
        if not gtt_id:
            update_status(status_manager, row_num, "❌ no gtt_id to update")
            failed_rows.append({"row_number": row_num, "reason": f"No GTT_ID found for update (was: {raw_gtt_id})"})
            logger.error(f"Row {row_num}: GTT_ID is missing/invalid ('{raw_gtt_id}') for update.")
            return
        
        try:
            old_units = int(matched_row.get("UNITS", 0) or 0)
        except:
            old_units = 0

        old_price = float(matched_row.get("GTT PRICE", 0) or 0)
        price_diff = abs(old_price - trigger_price)

        if old_units == quantity and price_diff < 0.01:
            update_status(status_manager, row_num, "no update needed")
            logger.debug(
                f"GTT {gtt_id} matches existing: qty={old_units} vs {quantity}, price={old_price:.2f} vs {trigger_price:.2f}. Skipping update for row {row_num}"
            )
            return

        logger.debug(f"Modifying GTT {gtt_id} for row {row_num}")

        order_update = {
            "exchange": exchange,
            "tradingsymbol": symbol,
            "transaction_type": side,
            "quantity": quantity,
            "order_type": "LIMIT",
            "product": "CNC",
            "price": limit_price,
            "validity": "DAY",
            "disclosed_quantity": 0,
            "trigger_price": trigger_price,
            "tag": method,
        }

        try:
            safe_api_call(
                kite.modify_gtt,
                gtt_id,
                tradingsymbol=symbol,
                exchange=exchange,
                trigger_type="single",
                trigger_values=[trigger_price],
                last_price=last_price_float,
                orders=[order_update],
            )
            update_status(status_manager, row_num, "✅ updated")
        except kite_exceptions.KiteException as e:
            update_status(status_manager, row_num, f"❌ Kite error: {e}")
            failed_rows.append({"row_number": row_num, "reason": f"Kite error: {e}"})
            return
        except Exception as e:
            update_status(status_manager, row_num, f"❌ error: {e}")
            failed_rows.append({"row_number": row_num, "reason": str(e)})
            return

    elif len(matches) > 1:
        update_status(status_manager, row_num, "❌ conflict: multiple matches")
        conflict_rows.append({"row_number": row_num})
        logger.warning(f"Multiple matching rows found for UPDATE at row {row_num}, skipping update.")
    else:
        update_status(status_manager, row_num, "❌ no match found")
        failed_rows.append({"row_number": row_num, "reason": "No matching GTT to update"})

def process_delete(
    status_manager, row_num, matches, kite, update_status,
    failed_rows, conflict_rows, logger, symbol, exchange
):
    if len(matches) == 1:
        matched_row = matches[0]

        raw_gtt_id = matched_row.get("GTT_ID", "")
        gtt_id = normalize_gtt_id(raw_gtt_id)
    
        if not gtt_id:
            update_status(status_manager, row_num, "❌ no gtt_id to delete")
            failed_rows.append({"row_number": row_num, "reason": f"No GTT_ID found for delete (was: {raw_gtt_id})"})
            logger.error(f"Row {row_num}: GTT_ID is missing/invalid ('{raw_gtt_id}') for delete.")
            return

        logger.debug(f"Deleting GTT {gtt_id} for row {row_num}")

        try:
            safe_api_call(kite.delete_gtt, gtt_id)
            update_status(status_manager, row_num, "✅ deleted")
        except kite_exceptions.KiteException as e:
            update_status(status_manager, row_num, f"❌ Kite error: {e}")
            failed_rows.append({"row_number": row_num, "reason": f"Kite error: {e}"})
            return
        except Exception as e:
            update_status(status_manager, row_num, f"❌ error: {e}")
            failed_rows.append({"row_number": row_num, "reason": str(e)})
            return

    elif len(matches) > 1:
        update_status(status_manager, row_num, "❌ conflict: multiple matches")
        conflict_rows.append({"row_number": row_num})
        logger.warning(f"Multiple matching rows found for DELETE at row {row_num}, skipping delete.")
    else:
        update_status(status_manager, row_num, "❌ no match found")
        failed_rows.append({"row_number": row_num, "reason": "No matching GTT to delete"})

# ------------------------- End Extracted Functions --------------------------

def colnum_to_a1(col):
    s = ""
    while col > 0:
        col, rem = divmod(col-1, 26)
        s = chr(65 + rem) + s
    return s

def normalize_gtt_id(raw_gtt_id):
    """
    Normalize GTT_ID values coming from Sheets.
    - Accepts strings, ints, floats, None.
    - Returns an int > 0 for a valid id, or None for missing/invalid/zero/NaN.
    - Handles "1234", "1234.0", 1234, 1234.0, " NaN ", "", None robustly.
    """
    if raw_gtt_id is None:
        return None
    s = str(raw_gtt_id).strip()
    if s == "":
        return None
    s_low = s.lower()
    if s_low in ("nan", "none", "null"):
        return None
    # Reject obvious zero-like values
    if s in ("0", "0.0"):
        return None
    try:
        # Handles "1234", "1234.0", "1234.00"
        val = int(float(s))
        if val == 0:
            return None
        return val
    except Exception:
        # Not parseable -> treat as missing/invalid
        return None

def _parse_number_safe(x):
    """
    Parse numeric-like values tolerant of strings like "1,234.00", "10.0", 10, 10.0.
    Returns float on success, or None if unparsable / empty.
    """
    if x is None:
        return None
    try:
        s = str(x).strip()
        if s == "":
            return None
        # remove thousands separators (commas)
        s = s.replace(",", "")
        # guard against common non-numeric tokens
        if s.lower() in ("nan", "none", "null", "-"):
            return None
        return float(s)
    except Exception:
        return None

def _int_from_number_like(x):
    """
    Return an integer derived from x: if x is float but near integer, treat as that integer.
    If unparsable, return 0 (keeps previous behaviour where missing -> 0).
    """
    f = _parse_number_safe(x)
    if f is None:
        return 0
    # treat floats that are effectively integers as integers
    rounded = int(round(f))
    return rounded

def _floats_equal(a, b, tol=0.01):
    """
    Compare two numeric-like values with tolerance (default 0.01).
    Missing/unparsable values are treated as 0.0 to keep behavior consistent.
    """
    af = _parse_number_safe(a)
    bf = _parse_number_safe(b)
    if af is None:
        af = 0.0
    if bf is None:
        bf = 0.0
    return abs(af - bf) <= tol

def normalize_type_for_matching(raw_type: str):
    if raw_type is None:
        return ""
    raw = (raw_type or "").strip().upper()
    if any(keyword in raw for keyword in [" BUY", "RTP_BUY", "KWK", "SIP_REG"]):
        return "BUY"
    if any(keyword in raw for keyword in [" SELL", "RTP_SELL"]):
        return "SELL"
    if raw.startswith("TSL"):
        return "SELL"
    return raw

def rows_match_4_elements(instr_row, data_row):
    """
    Match on TICKER (case-insensitive), normalized TYPE, UNITS (int), and GTT PRICE (float with tolerance).
    - UNITS: missing/unparsable treated as 0 (keeps prior behavior)
    - GTT PRICE: compared with tolerance (0.01)
    - TYPE: normalized using normalize_type_for_matching
    """
    # TICKER: compare case-insensitive trimmed strings
    instr_ticker = (instr_row.get("TICKER", "") or "")
    data_ticker = (data_row.get("TICKER", "") or "")
    if str(instr_ticker).strip().upper() != str(data_ticker).strip().upper():
        return False

    # TYPE: use existing normalizer then compare
    instr_type = normalize_type_for_matching(instr_row.get("TYPE", ""))
    data_type = normalize_type_for_matching(data_row.get("TYPE", ""))
    if instr_type != data_type:
        return False

    # UNITS: parse integer-like, treat missing/unparsable as 0
    instr_units = _int_from_number_like(instr_row.get("UNITS", 0))
    data_units = _int_from_number_like(data_row.get("UNITS", 0))
    if instr_units != data_units:
        return False

    # GTT PRICE: allow small tolerance (0.01)
    if not _floats_equal(instr_row.get("GTT PRICE", 0), data_row.get("GTT PRICE", 0), tol=0.01):
        return False

    return True

def rows_match_2_elements(instr_row, data_row):
    # For updates: match on TICKER (case-insensitive) and normalized TYPE
    instr_ticker = (instr_row.get("TICKER", "") or "").strip().upper()
    data_ticker = (data_row.get("TICKER", "") or "").strip().upper()
    instr_type = normalize_type_for_matching(instr_row.get("TYPE", ""))
    data_type = normalize_type_for_matching(data_row.get("TYPE", ""))
    return instr_ticker == data_ticker and instr_type == data_type

def find_matching_data_rows(instr_row, data_rows, update_match=False):
    if update_match:
        return [row for row in data_rows if rows_match_2_elements(instr_row, row)]
    else:
        return [row for row in data_rows if rows_match_4_elements(instr_row, row)]

def determine_action(raw_action):
    raw_action = raw_action.strip().upper()
    if "INSERT" in raw_action or "PLACE" in raw_action:
        return "PLACE"
    if "UPDATE" in raw_action:
        return "UPDATE"
    if "DELETE" in raw_action:
        return "DELETE"
    return "UNKNOWN"

def parse_type_to_side(raw_type):
    norm = normalize_type_for_matching(raw_type)
    if norm == "BUY":
        return "BUY"
    return "SELL"

def safe_api_call(func, *args, max_retries=5, base_delay=1, **kwargs):
    """
    Execute API call with robust exponential backoff + jitter retry.
    - Retries on rate-limits, timeouts, connection resets, and HTTP 5xx-like errors.
    - max_retries default increased to 5 for increased resilience.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            attempt += 1
            retriable = __is_retriable_exception(e)
            # Always retry on retriable errors (up to max_retries), otherwise re-raise immediately.
            if not retriable:
                # If it's a KiteException with nested status or recognizable retryable form, double-check
                # otherwise treat as fatal and raise.
                raise e

            if attempt >= max_retries:
                # reached retry budget — raise the last exception
                logger.error(f"API call failed after {attempt} attempts: {e}")
                raise e

            # exponential backoff with jitter
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
            logger.warning(f"Transient error on API call (attempt {attempt}/{max_retries}): {e}. Retrying in {delay:.2f}s")
            time.sleep(delay)
            continue

    # Shouldn't reach here, but return None defensively
    return None

class SheetStatusManager:
    def __init__(self, sheet):
        self.sheet = sheet
        self.headers = None
        self.status_col = None
        self.status_updates = {}
        self._load_headers()
    
    def _load_headers(self):
        try:
            self.headers = safe_api_call(self.sheet.row_values, 1)
            try:
                self.status_col = self.headers.index("STATUS") + 1
            except ValueError:
                self.status_col = len(self.headers) + 1
                safe_api_call(self.sheet.update_cell, 1, self.status_col, "STATUS")
                self.headers.append("STATUS")
        except Exception as e:
            logger.error(f"Failed to load headers: {e}")
            self.headers = []
            self.status_col = 1
    
    def queue_status_update(self, row_number, status_text):
        self.status_updates[row_number] = status_text
    
    def flush_status_updates(self):
        if not self.status_updates:
            return
        
        try:
            updates = []
            for row_num, status in self.status_updates.items():
                try:
                    updates.append({
                        'range': f'{colnum_to_a1(self.status_col)}{row_num}',
                        'values': [[status]]
                    })
                except Exception as e:
                    logger.error(f"Failed to prepare status update for row {row_num}: {e}")
            
            if updates:
                safe_api_call(self.sheet.batch_update, updates)
                logger.debug(f"Batch updated {len(updates)} status cells")
        except Exception as e:
            logger.error(f"Failed to flush status updates: {e}")
        finally:
            self.status_updates.clear()

def update_status(status_manager, row_number, status_text):
    status_manager.queue_status_update(row_number, status_text)

def parse_ticker(ticker):
    if ":" in ticker:
        parts = ticker.split(":")
        return parts[0].strip(), parts[1].strip()
    return "NSE", ticker.strip()

def process_gtt_batch(kite, start_row, instruction_sheet, data_sheet):
    """
    Processes up to BATCH_SIZE instruction rows starting at start_row.
    Returns: (raw_rows_read, processed_count, failed_rows, conflict_rows)
    raw_rows_read = number of raw rows returned by the sheet read (used for pagination)
    processed_count = number of filtered instructions actually processed
    """
    raw_instructions, instructions = fetch_gtt_instructions_batch(instruction_sheet, start_row)
    raw_read = len(raw_instructions)
    if raw_read == 0:
        logger.info("No GTT instructions (raw) found to process.")
        return 0, 0, [], []

    raw_data_rows, data_rows = fetch_existing_gtts_batch(data_sheet, start_row)
    # Note: using same start_row for existing data fetch keeps earlier behavior; adjust if you need different offsets.
    failed_rows = []
    conflict_rows = []
    data_header = data_sheet.row_values(1)
    
    status_manager = SheetStatusManager(instruction_sheet)

    for idx, instr in enumerate(instructions):
        row_num = start_row + idx
        try:
            raw_ticker = instr.get("TICKER", "").strip()
            raw_type = instr.get("TYPE", "").strip()
            raw_action = instr.get("ACTION", "").strip()
            quantity = int(instr.get("UNITS", 0) or 0)
            method = instr.get("METHOD", "").strip()

            if not raw_ticker or not raw_type or not raw_action:
                update_status(status_manager, row_num, "❌ MISSING FIELD")
                failed_rows.append({"row_number": row_num, "reason": "Missing TICKER / TYPE / ACTION"})
                continue

            exchange, symbol = parse_ticker(raw_ticker)
            action = determine_action(raw_action)
            side = parse_type_to_side(raw_type)
            price_float = float(instr.get("GTT PRICE", 0) or 0)
            
            trigger_price = price_float
            limit_price = price_float
                
            last_price_float = float(instr.get("LIVE PRICE", 0) or 0)

            instr_match_obj = {
                "TICKER": raw_ticker,
                "TYPE": raw_type,
                "UNITS": quantity,
                "GTT PRICE": price_float,
            }

            if action == "UPDATE":
                matches = find_matching_data_rows(instr_match_obj, data_rows, update_match=True)
            else:
                matches = find_matching_data_rows(instr_match_obj, data_rows, update_match=False)

            if action == "PLACE":
                process_place(
                    status_manager, row_num, matches, exchange, symbol, side,
                    quantity, limit_price, trigger_price, method, kite,
                    last_price_float, update_status, failed_rows
                )
            elif action == "UPDATE":
                process_update(
                    status_manager, row_num, matches, quantity, trigger_price,
                    symbol, exchange, side, limit_price, method, kite,
                    last_price_float, update_status, failed_rows, conflict_rows, logger
                )
            elif action == "DELETE":
                process_delete(
                    status_manager, row_num, matches, kite,
                    update_status, failed_rows, conflict_rows, logger, symbol, exchange
                )
            else:
                update_status(status_manager, row_num, "❌ unknown action")
                failed_rows.append({"row_number": row_num, "reason": f"Unknown ACTION: {raw_action}"})

            time.sleep(0.1)

        except Exception as e:
            logger.error(f"Exception processing row {row_num}: {traceback.format_exc()}")
            try:
                update_status(status_manager, row_num, f"❌ exception: {e}")
            except Exception as inner_e:
                logger.error(f"Double-failure while updating status at row {row_num}: {inner_e}")
            failed_rows.append({"row_number": row_num, "reason": str(e)})

    status_manager.flush_status_updates()

    processed_count = len(instructions)
    return raw_read, processed_count, failed_rows, conflict_rows

def process_market_sheet(kite, worksheet, status_manager, logger):
    rows = worksheet.get_all_values()
    if not rows or len(rows) < 2:
        logger.info("No rows to process in MKT_INS")
        return

    headers = [h.strip().upper() for h in rows[0]]
    ix_ticker = headers.index("TICKER")
    ix_units = headers.index("UNITS")
    ix_action = headers.index("ACTION")
    ix_status = headers.index("STATUS")

    for row_num, row in enumerate(rows[1:], start=2):
        try:

            raw_ticker = row[ix_ticker].strip()
            exchange, symbol = parse_ticker(raw_ticker)

            units = _int_from_number_like(row[ix_units])
            
            if units <= 0:
                update_status(status_manager, row_num, "⏭ skipped: invalid or empty UNITS")
                continue

            side = normalize_type_for_matching(row[ix_action])

            if side not in ("BUY", "SELL"):
                update_status(
                    status_manager,
                    row_num,
                    f"❌ invalid ACTION for MARKET: {row[ix_action]}"
                )
                continue

            # Skip if STATUS already filled
            if row[ix_status]:
                continue

            product = "CNC"
            variety = resolve_order_variety()

            resp = safe_api_call(
                kite.place_order,
                tradingsymbol=symbol,
                exchange=exchange,
                transaction_type=side,
                quantity=units,
                order_type="MARKET",
                product=product,
                variety=variety,
                validity="DAY",
            )

            update_status(status_manager, row_num, "✅ market placed")
            logger.info(f"Row {row_num}: MARKET {side} {symbol} x{units} placed")

        except Exception as e:
            update_status(status_manager, row_num, f"❌ error: {e}")
            logger.error(f"Row {row_num}: error placing MARKET order: {e}")

def __is_retriable_exception(exc):
    """
    Heuristic to decide whether `exc` is a transient/retriable error.
    - Looks for common substrings (429, quota, timeout, connection reset, recv failure, 5xx)
    - Recognizes socket errors (connection reset) and HTTP/requests-like status_code attributes.
    """
    if exc is None:
        return False

    # If exception has HTTP/status code attribute and it's 5xx -> retriable
    for attr in ("status_code", "code", "errno"):
        try:
            val = getattr(exc, attr, None)
            if isinstance(val, int) and 500 <= val <= 599:
                return True
        except Exception:
            pass

    msg = ""
    try:
        msg = str(exc).lower()
    except Exception:
        msg = ""

    # common transient indicators
    transient_keywords = [
        "429", "quota", "rate limit", "timeout", "timed out", "connection reset",
        "connection aborted", "recv failure", "connection refused", "temporarily unavailable",
        "502", "503", "504", "socket.timeout", "connectionreseterror"
    ]
    for kw in transient_keywords:
        if kw in msg:
            return True

    # socket-level errors
    if isinstance(exc, socket.timeout):
        return True
    # Some libs wrap socket errors; check repr
    try:
        if "connectionreseterror" in repr(exc).lower():
            return True
    except Exception:
        pass

    return False

def main(instruction_sheet=None, data_sheet=None, kite=None):
    """
    Main runner. If instruction_sheet / data_sheet / kite are provided, use them.
    Otherwise resolve using config-driven helpers.
    """
    logger.info("Starting GTT processing batch script...")

    gsheet_client = get_gsheet_client()

    if instruction_sheet is None:
        instruction_sheet = get_instructions_sheet()
    if data_sheet is None:
        data_sheet = get_tracking_sheet()
    if kite is None:
        kite = get_kite()

    try:
        k1_val = instruction_sheet.acell("K1").value
        k1_num = float(k1_val) if k1_val not in (None, "") else 0
        if k1_num <= 0:
            logger.info(f"K1 <= 0 ({k1_val}) → Skipping entire GTT processing.")
            return
    except Exception as e:
        logger.error(f"Failed to read K1 → Skipping process. Error: {e}")
        return

    headers = instruction_sheet.row_values(1)
    try:
        status_col_idx = headers.index("STATUS") + 1  # 1-based indexing for Google Sheets

        # Determine last data row cheaply by checking column A's filled rows.
        # This avoids get_all_values() and avoids relying solely on row_count.
        try:
            last_data_row = instruction_sheet.row_count
        except Exception:
            # Fallback to row_count if col_values fails for some reason
            last_data_row = instruction_sheet.row_count

        if last_data_row >= 2:
            col_letter = colnum_to_a1(status_col_idx)
            clear_range = f'{col_letter}2:{col_letter}{last_data_row}'
            logger.info(f"Clearing STATUS column values in instruction sheet: {clear_range}")
            # batch_clear clears cell values but preserves formatting and formulas
            try:
                instruction_sheet.batch_clear([clear_range])
            except Exception as e:
                logger.warning(f"batch_clear failed ({e}), falling back to per-cell clears")
                # As a safe fallback, clear each cell individually (preserves formatting)
                for r in range(2, last_data_row + 1):
                    try:
                        instruction_sheet.update_cell(r, status_col_idx, "")
                    except Exception as inner_e:
                        logger.debug(f"Failed to clear cell {col_letter}{r}: {inner_e}")
    except ValueError:
        logger.info('STATUS column not found in header row—skipping clear step.')

    # --- NEW: recompute fresh last_data_row AFTER clearing STATUS ---
    try:
        # Short pause to let the Sheets API settle, then re-read column A for an up-to-date count
        time.sleep(0.2)
        col_a_vals = instruction_sheet.col_values(1)
        last_data_row = max(len(col_a_vals), 1)
    except Exception:
        # Defensive fallback if col_values fails
        last_data_row = instruction_sheet.row_count
    logger.info("Recomputed last_data_row after STATUS clear: %s", last_data_row)

    start_row = 2

    total_rows_processed = 0
    all_failed_rows = []
    all_conflict_rows = []

    # safety: count consecutive empty processed batches (filtered==0)
    consecutive_empty_batches = 0
    EMPTY_BATCH_LIMIT = 3  # stop after this many empty filtered batches in a row

    while True:
        raw_read, processed, failed_rows, conflict_rows = process_gtt_batch(
            kite, start_row, instruction_sheet, data_sheet
        )

        # nothing raw returned -> sheet end
        if raw_read == 0:
            logger.info("No more raw rows returned from sheet; stopping.")
            break

        # If we got no non-empty (filtered) instructions in this raw chunk...
        if processed == 0:
            consecutive_empty_batches += 1
            logger.info(
                f"No non-empty instructions in fetched {raw_read} raw rows at start_row={start_row} "
                f"(consecutive empty batches={consecutive_empty_batches})"
            )
            # If this chunk was smaller than a full batch, it's likely the tail of sheet -> stop
            if raw_read < BATCH_SIZE:
                logger.info("Raw rows fetched < BATCH_SIZE and no instructions in them — assuming end of data. Stopping.")
                break
            # Otherwise if we see repeated empty batches, break as a safety net
            if consecutive_empty_batches >= EMPTY_BATCH_LIMIT:
                logger.info(f"No usable data after {EMPTY_BATCH_LIMIT} consecutive empty batches — stopping.")
                break
        else:
            # reset counter when we processed something useful
            consecutive_empty_batches = 0

        total_rows_processed += processed
        all_failed_rows.extend(failed_rows)
        all_conflict_rows.extend(conflict_rows)
        start_row += raw_read

        time.sleep(1)

    logger.info(f"Total rows processed: {total_rows_processed}")
    if all_failed_rows:
        logger.warning(f"Failed rows count: {len(all_failed_rows)}")
        for fr in all_failed_rows:
            logger.warning(f"Failed row: {fr}")

    if all_conflict_rows:
        logger.warning(f"Conflict rows count: {len(all_conflict_rows)}")
        for cr in all_conflict_rows:
            logger.warning(f"Conflict row: {cr}")

def run_fetch_all_gtts_script():
    try:
        logger.info("Running fetch_all_gtts.py...")
        import fetch_all_gtts
        fetch_all_gtts.fetch_all_gtts()
        logger.info("fetch_all_gtts.py completed successfully")
    except Exception as e:
        logger.error(f"Failed to run fetch_all_gtts.py: {e}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Process GTT instructions. Only accepts --sheet-id and --sheet-name which override config values."
    )
    parser.add_argument("--sheet-id", dest="sheet_id", help="Instruction sheet ID (overrides lookup/config default)", type=str)
    parser.add_argument("--sheet-name", dest="sheet_name", help="Instruction TAB_NAME (overrides config.INSTRUCTION_TAB_NAME)", type=str)
    parser.add_argument("--market-order", action="store_true", help="Process Market Orders instead of GTT")

    args = parser.parse_args()

    # If provided on CLI, use them; otherwise get_instructions_sheet will fall back to config
    instruction_sheet = get_instructions_sheet(sheet_id=getattr(args, "sheet_id", None), sheet_name=getattr(args, "sheet_name", None))

    kite = get_kite()
    
    if args.market_order:

        # --- CLEAR STATUS COLUMN FOR MARKET MODE ---
        headers = instruction_sheet.row_values(1)
        try:
            status_col_idx = headers.index("STATUS") + 1
            last_row = instruction_sheet.row_count
            col_letter = colnum_to_a1(status_col_idx)
            clear_range = f"{col_letter}2:{col_letter}{last_row}"
            instruction_sheet.batch_clear([clear_range])
            logger.info(f"Cleared STATUS column for MARKET mode: {clear_range}")
        except ValueError:
            logger.warning("STATUS column not found; skipping clear step")

        # Process MKT_INS sheet directly
        status_manager = SheetStatusManager(instruction_sheet)
        process_market_sheet(kite, instruction_sheet, status_manager, logger)
        status_manager.flush_status_updates()
    else:
        # Default: GTT flow
        data_sheet = get_tracking_sheet()
        main(instruction_sheet=instruction_sheet, data_sheet=data_sheet, kite=kite)

    # run optional post-processing (kept as-is; it logs on failure)
    run_fetch_all_gtts_script()

    # ------------------ POST-CHECKS: specific cells & logging ------------------
    def _check_cell_and_log(spreadsheet, tab_name, cell_addr, friendly_name=None):
        """
        Read spreadsheet.worksheet(tab_name).acell(cell_addr).value and log:
         - INFO with ✅ message if value == "0"
         - ERROR with ❌ message otherwise
        Any exception becomes an ERROR log.
        """
        if friendly_name is None:
            friendly_name = f"{tab_name}!{cell_addr}"

        try:
            try:
                ws = spreadsheet.worksheet(tab_name)
            except Exception as e:
                logger.error(f"❌ Could not open worksheet '{tab_name}' to check {friendly_name}: {e}")
                return

            try:
                val = ws.acell(cell_addr).value
            except Exception as e:
                logger.error(f"❌ Could not read cell {friendly_name}: {e}")
                return

            # Normalize and compare to string "0"
            val_norm = (str(val).strip() if val is not None else "")
            if val_norm == "0":
                logger.info(f"✅ Post-check passed: {friendly_name} = 0 → Process completed successfully")
            else:
                logger.error(f"❌ Post-check failed: {friendly_name} = {val_norm or '<EMPTY/None>'} → Process not completed")

        except Exception as e:
            logger.error(f"❌ Unexpected error while checking {friendly_name}: {e}")

    # Use the spreadsheet object associated with instruction_sheet (so CLI override works)
    try:
        spreadsheet = instruction_sheet.spreadsheet  # gspread Worksheet -> Spreadsheet
    except Exception:
        # As a fallback, try to resolve spreadsheet from data_sheet if available
        try:
            spreadsheet = data_sheet.spreadsheet
        except Exception:
            spreadsheet = None

    if spreadsheet is None:
        logger.error("❌ Could not resolve Spreadsheet object for post-checks (neither instruction_sheet nor data_sheet provided a parent). Skipping post-checks.")
    else:
        # The four checks you requested:
        _check_cell_and_log(spreadsheet, "DUP_ZERODHA_GTT_DATA", "O1", "DUP_ZERODHA_GTT_DATA!O1")
        _check_cell_and_log(spreadsheet, "DUP_ZERODHA_GTT_DATA", "Q1", "DUP_ZERODHA_GTT_DATA!Q1")
        _check_cell_and_log(spreadsheet, "MATCH_OLD_GTT_INS", "L1", "MATCH_OLD_GTT_INS!L1")
        _check_cell_and_log(spreadsheet, "MATCH_OLD_GTT_INS", "N1", "MATCH_OLD_GTT_INS!N1")

    logger.info("Script finished.")
