import os
import time
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime
import re
from gspread_formatting import format_cell_range, cellFormat, numberFormat


CREDS_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
SOURCE_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE"
STOCK_SHEET_NAME = "NSE_Stock_Data"
ETF_SHEET_NAME = "NSE_ETF_Data"
DEST_SPREADSHEET_NAME = "Algo Master Data Bank"
INC_WS = "BANK_INC"
NEW_WS = "BANK_NEW"
TICKERS_WS = "TICKERS"
HEADER = ["DATE", "SYMBOL", "CLOSE", "LOW", "HIGH", "VOLUME", "TYPE"]


def resolve_creds_path() -> str:
    """
    Prefer env `CREDS_PATH`, else local `creds.json`, else the historical macOS path.
    This keeps GitHub Actions/local runs working without editing the script.
    """
    env_path = os.getenv("CREDS_PATH")
    if env_path:
        return env_path
    if os.path.exists("creds.json"):
        return "creds.json"
    return CREDS_PATH


def get_client():
    creds = Credentials.from_service_account_file(resolve_creds_path(), scopes=SCOPES)
    return gspread.authorize(creds)


def ZEROISH(x):
    if x is None:
        return True
    if str(x).strip() == "":
        return True
    try:
        return float(x) == 0.0
    except Exception:
        return False


def parse_date_formula(date_formula_str):
    # Accepts '=DATE(2025,7,16)', returns '2025-07-16'
    m = re.match(r"=DATE\((\d+),(\d+),(\d+)\)", str(date_formula_str))
    if m:
        y, m_, d = map(int, m.groups())
        return f"{y:04d}-{m_:02d}-{d:02d}"
    # Try to parse plain date strings
    try:
        dt = pd.to_datetime(date_formula_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return str(date_formula_str)


def load_and_clean(source_spreadsheet, tabname):
    ws = source_spreadsheet.worksheet(tabname)
    df = pd.DataFrame(ws.get_all_records())
    req = ["Symbol", "Current_Price", "Day_Low", "Day_High", "Volume (in Cr.)", "Last_Updated"]
    df = df[req]
    df.rename(columns={
        "Symbol": "SYMBOL",
        "Current_Price": "CLOSE",
        "Day_Low": "LOW",
        "Day_High": "HIGH",
        "Volume (in Cr.)": "VOLUME",
        "Last_Updated": "DATE"
    }, inplace=True)
    df["SYMBOL"] = df["SYMBOL"].apply(lambda x: f"NSE:{x}" if not str(x).startswith("NSE:") else x)
    for col in ["CLOSE", "LOW", "HIGH", "VOLUME"]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def clear_ws_except_header(ws):
    all_vals = ws.get_all_values()
    n_rows = len(all_vals)
    n_cols = len(all_vals[0]) if all_vals else 7
    if n_rows > 1:
        clear_range = f"A2:{chr(64+n_cols)}{n_rows}"
        ws.batch_clear([clear_range])


def get_ticker_type_map(client):
    ws = client.open(DEST_SPREADSHEET_NAME).worksheet(TICKERS_WS)
    # TICKERS sheet has headers: TICKER | TYPE
    data = ws.get_all_records(expected_headers=["TICKER", "TYPE"])
    # Create mapping with NSE: prefix, matching your SYMBOL columns everywhere else
    ticker_map = {("NSE:"+str(row["TICKER"]).strip() if not str(row["TICKER"]).startswith("NSE:") else str(row["TICKER"]).strip()): str(row["TYPE"]).strip()
                  for row in data if row.get("TICKER") and row.get("TYPE")}
    return ticker_map


def print_zero_log(date, symbol, colname, val):
    print(f"[ZERO] Ticker: {symbol}, Date: {date}, Field: {colname} is zero/blank/null in BANK_INC")


def print_fill_log(symbol, date, colname, fillval, filldate):
    print(f"[FILL] Ticker: {symbol}, Date: {date}, Field: {colname} filled with value {fillval} from {filldate}")


def load_bank_new_indexed(ws):
    vals = ws.get_all_values()
    rows = []
    by_ticker = {}
    idx_by_date_sym = {}
    if not vals or len(vals) < 2:
        return rows, by_ticker, idx_by_date_sym
    for i, row in enumerate(vals[1:], start=2):  # skip header
        row = (row + [""] * 7)[:7]
        date, symbol, close, low, high, vol, typ = row
        rows.append(row)
        idx_by_date_sym[(date, symbol)] = i  # row index for overwrite
        if symbol not in by_ticker: by_ticker[symbol] = []
        # Accept only YYYY-MM-DD for fast date comparison
        dt_val = pd.to_datetime(date, errors="coerce")
        by_ticker[symbol].append({
            "DATE": date, "PYDATE": dt_val, "CLOSE": close, "LOW": low, "HIGH": high, "VOLUME": vol
        })
    for sym in by_ticker:
        by_ticker[sym].sort(key=lambda r: r["PYDATE"] if pd.notnull(r["PYDATE"]) else datetime(1900,1,1))
    return rows, by_ticker, idx_by_date_sym


def get_last_nonzero_val(by_ticker, symbol, colname, cur_pydate):
    if symbol not in by_ticker: return None, None
    for row in reversed(by_ticker[symbol]):
        if pd.notnull(row["PYDATE"]) and row["PYDATE"] < cur_pydate:
            val = row[colname]
            if not ZEROISH(val):
                return val, row["DATE"]
    return None, None


def build_bankinc_rows(df_comb):
    bankinc_rows = []
    zero_cols = ["CLOSE", "LOW", "HIGH", "VOLUME"]

    # itertuples is notably faster than iterrows for large frames
    for row in df_comb.itertuples(index=False):
        date_val = pd.to_datetime(getattr(row, "DATE"), errors="coerce")
        date_formula_val = (
            f"=DATE({date_val.year},{date_val.month},{date_val.day})" if pd.notnull(date_val) else ""
        )

        close_val = getattr(row, "CLOSE")
        low_val = getattr(row, "LOW")
        high_val = getattr(row, "HIGH")
        vol_val = getattr(row, "VOLUME")

        values = [
            date_formula_val,
            getattr(row, "SYMBOL"),
            close_val if pd.notnull(close_val) else "",
            low_val if pd.notnull(low_val) else "",
            high_val if pd.notnull(high_val) else "",
            vol_val if pd.notnull(vol_val) else "",
            "",  # TYPE formula will be added later
        ]

        for cidx, cname in enumerate(zero_cols, start=2):  # C/D/E/F cols
            val = values[cidx]
            if ZEROISH(val):
                print_zero_log(values[0], values[1], cname, val)
        bankinc_rows.append(values)

    return bankinc_rows


def add_type_formulas(bankinc_rows, start_row=2):
    for idx, row in enumerate(bankinc_rows, start=start_row):
        row[6] = f"=IFERROR(VLOOKUP(B{idx},TICKERS!A:C,3,FALSE))"


def write_bank_inc(ws_inc, bankinc_rows):
    clear_ws_except_header(ws_inc)
    ws_inc.append_rows(bankinc_rows, value_input_option="USER_ENTERED")

def prepare_bank_new_changes(bankinc_rows, by_ticker, idx_by_date_sym):
    to_overwrite = []
    to_append = []

    for row in bankinc_rows:
        # Parse DATE value for BANK_NEW
        date_val = parse_date_formula(row[0])
        symbol = row[1]
        dt_obj = pd.to_datetime(date_val, errors="coerce")
        filled_row = list(row)

        for cidx, cname in zip([2, 3, 4, 5], ["CLOSE", "LOW", "HIGH", "VOLUME"]):
            val = filled_row[cidx]
            if ZEROISH(val):
                prev_val, prev_date = get_last_nonzero_val(by_ticker, symbol, cname, dt_obj)
                if prev_val is not None:
                    filled_row[cidx] = prev_val
                    print_fill_log(symbol, date_val, cname, prev_val, prev_date)

        # BANK_NEW uses only values (DATE, SYMBOL, CLOSE, LOW, HIGH, VOLUME)
        gs_values = [date_val, symbol, filled_row[2], filled_row[3], filled_row[4], filled_row[5]]
        key = (gs_values[0], gs_values[1])
        if key in idx_by_date_sym:
            to_overwrite.append((idx_by_date_sym[key], gs_values))
        else:
            to_append.append(gs_values)

    return to_overwrite, to_append


def _flush_overwrite_batch(ws_new, batch):
    indices, rows_block = zip(*batch)
    start, end = indices[0], indices[-1]
    ws_new.update(f"A{start}:F{end}", list(rows_block))


def apply_overwrites(ws_new, to_overwrite):
    # group by consecutive rows to reduce API calls
    if not to_overwrite:
        return

    to_overwrite.sort(key=lambda x: x[0])
    batch = []
    prev_idx = None

    for idx, vals in to_overwrite:
        if prev_idx is None or idx == prev_idx + 1:
            batch.append((idx, vals))
        else:
            if batch:
                _flush_overwrite_batch(ws_new, batch)
            batch = [(idx, vals)]
        prev_idx = idx

    if batch:
        _flush_overwrite_batch(ws_new, batch)


def apply_appends(ws_new, to_append):
    if to_append:
        ws_new.append_rows(to_append, value_input_option="USER_ENTERED")


def format_bank_new(ws_new):
    try:
        fmt = cellFormat(numberFormat=numberFormat(type="DATE", pattern="dd-mmm-yyyy"))
        format_cell_range(ws_new, "A2:A", fmt)
        print("[FORMAT] BANK_NEW column A formatted as dd-mmm-yyyy")
    except Exception as e:
        print(f"[WARN] Could not format BANK_NEW column A: {e}")


def process_and_update():
    start_time = datetime.now()
    print("")
    print(
        f"[NSE ETL PROCESS START] {start_time.strftime('%Y-%m-%d %H:%M:%S')} - Starting process_and_update"
    )

    client = get_client()
    dest_spreadsheet = client.open(DEST_SPREADSHEET_NAME)
    source_spreadsheet = client.open_by_url(SOURCE_SPREADSHEET_URL)

    # 1. LOAD today's data (stock + etf)
    df_stock = load_and_clean(source_spreadsheet, STOCK_SHEET_NAME)
    df_etf = load_and_clean(source_spreadsheet, ETF_SHEET_NAME)
    df_comb = pd.concat([df_stock, df_etf], ignore_index=True)

    # 2. BUILD BANK_INC rows & log [ZERO]
    bankinc_rows = build_bankinc_rows(df_comb)

    # 3. WRITE BANK_INC (batched, with TYPE formula)
    ws_inc = dest_spreadsheet.worksheet(INC_WS)
    add_type_formulas(bankinc_rows, start_row=2)
    write_bank_inc(ws_inc, bankinc_rows)

    # 4. PREP BANK_NEW updates (with only VALUES, no formulas!)
    ws_new = dest_spreadsheet.worksheet(NEW_WS)
    _all_rows, by_ticker, idx_by_date_sym = load_bank_new_indexed(ws_new)
    to_overwrite, to_append = prepare_bank_new_changes(bankinc_rows, by_ticker, idx_by_date_sym)

    # 5. BATCH OVERWRITE
    apply_overwrites(ws_new, to_overwrite)

    # 6. BATCH APPEND
    apply_appends(ws_new, to_append)

    # 7. FORMAT BANK_NEW DATE COLUMN
    format_bank_new(ws_new)

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    print(
        f"[NSE ETL PROCESS END]✅  {end_time.strftime('%Y-%m-%d %H:%M:%S')} - Finished process_and_update (duration: {elapsed:.2f}s)"
    )
    print("")


def check_cell_and_log(spreadsheet, tab_name, cell_addr, friendly_name=None):
    """
    Read spreadsheet.worksheet(tab_name).acell(cell_addr).value and print:
     - ✅ message if value == "0"
     - ❌ message otherwise (including errors)
    """
    if friendly_name is None:
        friendly_name = f"{tab_name}!{cell_addr}"

    try:
        try:
            ws = spreadsheet.worksheet(tab_name)
        except Exception as e:
            print(f"❌ Could not open worksheet '{tab_name}' to check {friendly_name}: {e}")
            return

        try:
            val = ws.acell(cell_addr).value
        except Exception as e:
            print(f"❌ Could not read cell {friendly_name}: {e}")
            return

        # Normalize and compare to string "0"
        val_norm = (str(val).strip() if val is not None else "")
        if val_norm == "0":
            print(f"✅ Post-check passed: {friendly_name} = 0 → Process completed successfully")
        else:
            print(f"❌ Post-check failed: {friendly_name} = {val_norm or '<EMPTY/None>'} → Process not completed")

    except Exception as e:
        print(f"❌ Unexpected error while checking {friendly_name}: {e}")


def run_post_checks():
    spreadsheet = None
    try:
        client_post = get_client()
        spreadsheet = client_post.open(DEST_SPREADSHEET_NAME)
    except Exception as e:
        spreadsheet = None
        print(
            f"❌ Could not open destination spreadsheet '{globals().get('DEST_SPREADSHEET_NAME', '<not-set>')}' for post-checks: {e}"
        )

    if spreadsheet is None:
        print("❌ Could not resolve Spreadsheet object for post-checks. Skipping post-checks.")
        return

    # The checks this ETL expects
    check_cell_and_log(spreadsheet, "BANK_FINAL", "H1", "BANK_FINAL!H1")
    check_cell_and_log(spreadsheet, "BANK_FINAL", "I1", "BANK_FINAL!I1")
    check_cell_and_log(spreadsheet, "EXTREME_CHANGES", "J1", "EXTREME_CHANGES!J1")


def main():
    process_and_update()

    sleep_s = int(os.getenv("POST_CHECK_SLEEP_SECONDS", "180"))
    print(f"[MAIN] Waiting {sleep_s} seconds before post-checks...")
    time.sleep(sleep_s)
    run_post_checks()


if __name__ == "__main__":
    main()
