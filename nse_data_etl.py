import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime
import re
from gspread_formatting import format_cell_range, cellFormat, numberFormat

from runtime_paths import get_creds_path


CREDS_PATH = str(get_creds_path())
SOURCE_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE"
STOCK_SHEET_NAME = "NSE_Stock_Data"
ETF_SHEET_NAME = "NSE_ETF_Data"
DEST_SPREADSHEET_NAME = "Algo Master Data Bank"
INC_WS = "BANK_INC"
NEW_WS = "BANK_NEW"
TICKERS_WS = "TICKERS"
HEADER = ["DATE", "SYMBOL", "CLOSE", "LOW", "HIGH", "VOLUME", "TYPE"]

def ZEROISH(x):
    if x is None: return True
    if str(x).strip() == "": return True
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

def load_and_clean(client, tabname):
    ws = client.open_by_url(SOURCE_SPREADSHEET_URL).worksheet(tabname)
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


def print_zero_log(row, date, symbol, colname, val):
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

def process_and_update():

    start_time = datetime.now()
    print(f"")
    print(f"[NSE ETL PROCESS START] {start_time.strftime('%Y-%m-%d %H:%M:%S')} - Starting process_and_update")

    creds = Credentials.from_service_account_file(CREDS_PATH, scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)

    ticker_type_map = get_ticker_type_map(client)

    # 1. LOAD today's data (stock + etf)
    df_stock = load_and_clean(client, STOCK_SHEET_NAME)
    df_etf = load_and_clean(client, ETF_SHEET_NAME)
    df_comb = pd.concat([df_stock, df_etf], ignore_index=True)

    # 2. BUILD BANK_INC rows & log [ZERO]
    bankinc_rows = []
    zero_cols = ["CLOSE", "LOW", "HIGH", "VOLUME"]
    for _, row in df_comb.iterrows():
        date_val = pd.to_datetime(row["DATE"], errors='coerce')
        date_formula_val = f"=DATE({date_val.year},{date_val.month},{date_val.day})" if pd.notnull(date_val) else ""
        values = [
            date_formula_val, row["SYMBOL"],
            row["CLOSE"] if pd.notnull(row["CLOSE"]) else "",
            row["LOW"] if pd.notnull(row["LOW"]) else "",
            row["HIGH"] if pd.notnull(row["HIGH"]) else "",
            row["VOLUME"] if pd.notnull(row["VOLUME"]) else "",
            ""  # TYPE formula will be added later
        ]
        for cidx, cname in enumerate(zero_cols, start=2):  # C/D/E/F cols
            val = values[cidx]
            if ZEROISH(val):
                print_zero_log(values, values[0], values[1], cname, val)
        bankinc_rows.append(values)

    # 3. WRITE BANK_INC (batched, with TYPE formula)
    ws_inc = client.open(DEST_SPREADSHEET_NAME).worksheet(INC_WS)
    clear_ws_except_header(ws_inc)
    for idx, row in enumerate(bankinc_rows, start=2):
        row[6] = f'=IFERROR(VLOOKUP(B{idx},TICKERS!A:C,3,FALSE))'
    ws_inc.append_rows(bankinc_rows, value_input_option="USER_ENTERED")

    # 4. PREP BANK_NEW updates (with only VALUES, no formulas!)
    ws_new = client.open(DEST_SPREADSHEET_NAME).worksheet(NEW_WS)
    all_rows, by_ticker, idx_by_date_sym = load_bank_new_indexed(ws_new)
    to_overwrite = []
    to_append = []

    for row in bankinc_rows:
        # Parse DATE value for BANK_NEW
        date_val = parse_date_formula(row[0])
        symbol = row[1]
        dt_obj = pd.to_datetime(date_val, errors='coerce')
        filled_row = list(row)
        for cidx, cname in zip([2,3,4,5], ["CLOSE", "LOW", "HIGH", "VOLUME"]):
            val = filled_row[cidx]
            if ZEROISH(val):
                prev_val, prev_date = get_last_nonzero_val(by_ticker, symbol, cname, dt_obj)
                if prev_val is not None:
                    filled_row[cidx] = prev_val
                    print_fill_log(symbol, date_val, cname, prev_val, prev_date)
        # TYPE as value (lookup from TICKERS)
        gs_values = [
            date_val, symbol,
            filled_row[2], filled_row[3], filled_row[4], filled_row[5]
        ]
        key = (gs_values[0], gs_values[1])
        if key in idx_by_date_sym:
            to_overwrite.append((idx_by_date_sym[key], gs_values))
        else:
            to_append.append(gs_values)

    # 5. BATCH OVERWRITE (group by consecutive rows if possible)
    if to_overwrite:
        to_overwrite.sort(key=lambda x: x[0])
        batch = []
        prev_idx = None
        for idx, vals in to_overwrite:
            if prev_idx is None or idx == prev_idx + 1:
                batch.append((idx, vals))
            else:
                if batch:
                    indices, rows_block = zip(*batch)
                    start, end = indices[0], indices[-1]
                    ws_new.update(f"A{start}:F{end}", list(rows_block))
                batch = [(idx, vals)]
            prev_idx = idx
        if batch:
            indices, rows_block = zip(*batch)
            start, end = indices[0], indices[-1]
            ws_new.update(f"A{start}:G{end}", list(rows_block))

    # 6. BATCH APPEND
    if to_append:
        ws_new.append_rows(to_append, value_input_option="USER_ENTERED")

    try:
        fmt = cellFormat(
            numberFormat=numberFormat(type='DATE', pattern='dd-mmm-yyyy')
        )
        format_cell_range(ws_new, 'A2:A', fmt)
        print("[FORMAT] BANK_NEW column A formatted as dd-mmm-yyyy")
    except Exception as e:
        print(f"[WARN] Could not format BANK_NEW column A: {e}")
    
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    print(f"[NSE ETL PROCESS END]✅  {end_time.strftime('%Y-%m-%d %H:%M:%S')} - Finished process_and_update (duration: {elapsed:.2f}s)")
    print(f"")

if __name__ == "__main__":
    process_and_update()

    print("[MAIN] Waiting 180 seconds before post-checks...")
    import time
    time.sleep(180)

    # ------------------ POST-CHECKS: simple prints against DEST_SPREADSHEET_NAME/BANK_FINAL ----
    def _check_cell_and_log(spreadsheet, tab_name, cell_addr, friendly_name=None):
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
    
    # Explicitly open the destination spreadsheet used by this ETL (authoritative bank)
    # This recreates a client so the post-check is independent of local client variables.
    spreadsheet = None
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(CREDS_PATH, scopes=scope)
        client_post = gspread.authorize(creds)
        # DEST_SPREADSHEET_NAME is the destination used elsewhere in this module (Algo Master Data Bank)
        spreadsheet = client_post.open(DEST_SPREADSHEET_NAME)
    except Exception as e:
        spreadsheet = None
        print(f"❌ Could not open destination spreadsheet '{globals().get('DEST_SPREADSHEET_NAME', '<not-set>')}' for post-checks: {e}")
    
    if spreadsheet is None:
        print("❌ Could not resolve Spreadsheet object for post-checks. Skipping post-checks.")
    else:
        # The checks this ETL expects in BANK_FINAL (H1 and I1)
        _check_cell_and_log(spreadsheet, "BANK_FINAL", "H1", "BANK_FINAL!H1")
        _check_cell_and_log(spreadsheet, "BANK_FINAL", "I1", "BANK_FINAL!I1")
        _check_cell_and_log(spreadsheet, "EXTREME_CHANGES", "J1", "EXTREME_CHANGES!J1")
    
