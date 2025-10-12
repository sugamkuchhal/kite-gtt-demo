# zerodha_tick_size_no_notfound.py
import gspread
from kiteconnect import KiteConnect
from gspread_formatting import format_cell_range, CellFormat, NumberFormat
import sys

API_KEY_FILE = "api_key.txt"
CREDS_JSON_PATH = "/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json"
SHEET_URL = "https://docs.google.com/spreadsheets/d/143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE/edit"
TICKERS_SHEET_NAME = "TICKERS_TICK_SIZE"
ZERODHA_SHEET_NAME = "ZERODHA_TICKERS"

CLEAR_SENTINEL = ""                  # what to write into cleared cells (blank)
NUMBER_PATTERN = "0.00"              # gspread-formatting pattern for 2 decimals

# ----------------------- load API key -----------------------
with open(API_KEY_FILE) as f:
    lines = [ln.strip() for ln in f.readlines() if ln.strip() != ""]
    if not lines:
        print("api_key.txt empty or missing")
        sys.exit(1)
    API_KEY = lines[0]

# ----------------------- build instrument map -----------------------
kite = KiteConnect(api_key=API_KEY)
print("Fetching instruments from Kite (this may take a while)...")
instruments = kite.instruments()  # may raise if API not accessible
instrument_map = {}
for inst in instruments:
    try:
        key = f"{inst['exchange']}:{inst['tradingsymbol']}"
        ts = inst.get("tick_size")
        if ts is not None:
            instrument_map[key] = float(ts)
    except Exception:
        continue
print(f"Loaded {len(instrument_map)} instruments.")

# ----------------------- open sheet (single) -----------------------
gc = gspread.service_account(filename=CREDS_JSON_PATH)
ss = gc.open_by_url(SHEET_URL)
tick_sheet = ss.worksheet(TICKERS_SHEET_NAME)

# ----------------------- read column A (detect last non-empty row) -----------------------
col_a = tick_sheet.col_values(1)  # returns up to last non-empty in col A
if len(col_a) <= 1:
    print("No tickers found in TICKERS_TICK_SIZE!A2:A (column A only has header or is empty). Exiting.")
    sys.exit(0)

# last_row includes header row 1; data rows are 2..last_row
last_row = len(col_a)
n_data_rows = last_row - 1
print(f"Detected data rows A2..A{last_row} ({n_data_rows} rows). Will only process non-empty A cells.")

# ----------------------- CLEAR C/D/E RIGHT AT START -----------------------
clear_range = f"C2:E{last_row}"
print(f"Clearing {clear_range} right at start ...")
tick_sheet.batch_clear([clear_range])

# initialize update arrays of length n_data_rows (one inner list per row)
updates_col_c = [[CLEAR_SENTINEL] for _ in range(n_data_rows)]
updates_col_d = [[CLEAR_SENTINEL] for _ in range(n_data_rows)]
updates_col_e = [[CLEAR_SENTINEL] for _ in range(n_data_rows)]

# ----------------------- read embedded Zerodha mapping from TICKERS_TICK_SIZE G:J and build lookup -----------------------
i_col = tick_sheet.col_values(9)   
j_col = tick_sheet.col_values(10)  

# Build lookup: value_in_I -> first value_in_J (strip)
zerodha_lookup = {}
max_zero = max(len(i_col), len(j_col))
for idx in range(1, max_zero):  # start at 1 to skip header row (I1/J1)
    key_val = i_col[idx].strip() if idx < len(i_col) else ""
    alt_val = j_col[idx].strip() if idx < len(j_col) else ""
    if key_val and key_val not in zerodha_lookup:
        zerodha_lookup[key_val] = alt_val  # alt_val may be blank

# ----------------------- process only non-empty A2:A rows -----------------------
found_original = []
found_with_alternate = []
not_found_even_with_alt = []  # keeps mains for which alternate existed but alternate's tick missing
log_lines = []

for i in range(n_data_rows):
    # row number in sheet
    sheet_row = i + 2
    main_ticker = col_a[i + 1].strip() if (i + 1) < len(col_a) else ""
    if not main_ticker:
        # leave cleared blanks (we already initialized to CLEAR_SENTINEL)
        continue

    # try main ticker
    ts_main = instrument_map.get(main_ticker)
    if ts_main is not None:
        # numeric float; gspread will write numeric value
        updates_col_c[i] = [round(ts_main, 2)]
        updates_col_d[i] = [CLEAR_SENTINEL]
        updates_col_e[i] = [CLEAR_SENTINEL]
        found_original.append(main_ticker)
        continue

    # main not found -> try Zerodha lookup
    log_lines.append(f"[Row {sheet_row}] Main not in instrument_map: {main_ticker}")
    alt = zerodha_lookup.get(main_ticker, "").strip()
    if not alt:
        # no alternate found in Zerodha mapping: leave C/D/E blank
        updates_col_c[i] = [CLEAR_SENTINEL]
        updates_col_d[i] = [CLEAR_SENTINEL]
        updates_col_e[i] = [CLEAR_SENTINEL]
        not_found_even_with_alt.append(main_ticker)
        log_lines.append(f"  -> Not found in {ZERODHA_SHEET_NAME} col C")
        continue

    # we have an alternate; write it into column D
    updates_col_c[i] = [CLEAR_SENTINEL]
    updates_col_d[i] = [alt]

    # try to find alternate in instrument_map
    ts_alt = instrument_map.get(alt)
    if ts_alt is not None:
        updates_col_e[i] = [round(ts_alt, 2)]
        found_with_alternate.append((main_ticker, alt))
        log_lines.append(f"  -> Found alternate '{alt}' with tick_size {ts_alt:.2f}")
    else:
        # alternate exists in Zerodha sheet but NOT in instrument map:
        # Leave column E blank (do NOT write any sentinel). Log for summary.
        updates_col_e[i] = [CLEAR_SENTINEL]
        not_found_even_with_alt.append(main_ticker)
        log_lines.append(f"  -> Alternate '{alt}' not found in instrument_map; leaving E blank")

# ----------------------- batch update columns (numerics for C and E where applicable) -----------------------
range_c = f"C2:C{last_row}"
range_d = f"D2:D{last_row}"
range_e = f"E2:E{last_row}"

# Validate lengths
assert len(updates_col_c) == n_data_rows
assert len(updates_col_d) == n_data_rows
assert len(updates_col_e) == n_data_rows

print(f"Writing updates to {range_c}, {range_d}, {range_e} ...")
tick_sheet.update(range_name=range_c, values=updates_col_c)
tick_sheet.update(range_name=range_d, values=updates_col_d)
tick_sheet.update(range_name=range_e, values=updates_col_e)

# ----------------------- apply number formatting (2 decimals) to C and E -----------------------
fmt_2dec = CellFormat(numberFormat=NumberFormat(type="NUMBER", pattern=NUMBER_PATTERN))
print("Applying number format 2 decimals to columns C and E...")
format_cell_range(tick_sheet, range_c, fmt_2dec)
format_cell_range(tick_sheet, range_e, fmt_2dec)

# ----------------------- summary -----------------------
print("\n====== Tick Size Update Summary ======")
print(f"Rows scanned (A2..A{last_row}): {n_data_rows}")
print(f"Tickers found originally: {len(found_original)}")

print(f"\nTickers resolved using alternate: {len(found_with_alternate)}")
if found_with_alternate:
    for main, alt in found_with_alternate[:200]:
        print(f"  - {main} -> {alt}")

print(f"\nTickers not found even with alternate: {len(not_found_even_with_alt)}")
if not_found_even_with_alt:
    for t in not_found_even_with_alt[:500]:
        print("  -", t)

if len(not_found_even_with_alt) == 0:
    print("✅ Tick size process completed successfully")
else:
    print(f"❌ Tick size process not completed. {len(not_found_even_with_alt)} tickers not found:")

print("\nDetailed logs (first 300 lines):")
for l in log_lines[:300]:
    print(l)
print("======================================")
