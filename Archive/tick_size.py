import gspread
from kiteconnect import KiteConnect

# Load secrets
with open("api_key.txt") as f:
    lines = [line.strip() for line in f.readlines()]
    API_KEY = lines[0]
    API_SECRET = lines[1]
    USER_ID = lines[2]
    PASSWORD = lines[3]
    TOTP_SECRET = lines[4]

# Setup KiteConnect
kite = KiteConnect(api_key=API_KEY)

print("Fetching instruments...")
instruments = kite.instruments()
instrument_map = {
    f"{i['exchange']}:{i['tradingsymbol']}": i['tick_size']
    for i in instruments
}

# Setup Google Sheets
gc = gspread.service_account(filename="/Users/sugamkuchhal/Documents/kite-gtt-demo/creds.json")
sheet = gc.open_by_url("https://docs.google.com/spreadsheets/d/143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE/edit")
worksheet = sheet.worksheet("TICKERS_TICK_SIZE")

# Read tickers from Column A (skip header)
tickers_col_a = worksheet.col_values(1)[1:]  # A2 onwards
# Read alternative tickers from Column D (skip header)
alt_tickers_col_d = worksheet.col_values(4)[1:]  # D2 onwards

# Prepare batch updates for Columns C and E
updates_col_c = []
updates_col_e = []

# For summary
main_success_count = 0
main_fail_count = 0
alt_success_count = 0
alt_fail_count = 0
alt_not_available_count = 0
failed_main_list = []
alt_fail_list = []

for idx, (ticker, alt_ticker) in enumerate(zip(tickers_col_a, alt_tickers_col_d), start=2):
    ticker = ticker.strip()
    alt_ticker = alt_ticker.strip()
    # Ignore blank main tickers
    if ticker == "":
        updates_col_c.append([""])
        updates_col_e.append([""])
        continue

    tick_size = instrument_map.get(ticker)
    if tick_size is not None:
        updates_col_c.append([str(tick_size)])
        updates_col_e.append([""])
        main_success_count += 1
    else:
        main_fail_count += 1
        updates_col_c.append([""])
        failed_main_list.append((ticker, alt_ticker))
        if alt_ticker == "":
            alt_not_available_count += 1
            updates_col_e.append(["1"])
            alt_fail_count += 1
            alt_fail_list.append((ticker, alt_ticker))
        else:
            alt_tick_size = instrument_map.get(alt_ticker)
            if alt_tick_size is not None:
                updates_col_e.append([str(alt_tick_size)])
                alt_success_count += 1
            else:
                updates_col_e.append(["1"])
                alt_fail_count += 1
                alt_fail_list.append((ticker, alt_ticker))

# Prepare ranges for batch update
last_row = len(tickers_col_a) + 1
range_c = f"C2:C{last_row}"
range_e = f"E2:E{last_row}"

# Batch update Columns C and E
worksheet.update(range_name=range_c, values=updates_col_c)
worksheet.update(range_name=range_e, values=updates_col_e)

# ---- PRINT SUMMARY ----
def print_table(title, rows):
    print(title)
    print('| Main Ticker'.ljust(18) + '| Alternate Ticker |')
    print('|' + '-'*16 + '|' + '-'*17 + '|')
    for t, a in rows:
        print(f'| {t.ljust(15)} | {a.ljust(15)} |')

total_processed = sum(1 for t in tickers_col_a if t.strip() != "")
print("\n=========== Tick Size Summary ===========")
print(f"Total tickers processed:  {total_processed}\n")
print(f"✅ Main ticker successes:  {main_success_count}")
print(f"❌ Main ticker failures:   {main_fail_count}\n")

if failed_main_list:
    print("Failed main tickers (with alternates):")
    print_table('', failed_main_list)
    print()

print(f"Alternate tickers not available: {alt_not_available_count}")
print(f"✅ Alternate ticker successes: {alt_success_count}")
print(f"❌ Alternate ticker failures:  {alt_fail_count}\n")

if alt_fail_list:
    print("Tickers where alternate also failed:")
    print_table('', alt_fail_list)
    print()

print("=========================================")
