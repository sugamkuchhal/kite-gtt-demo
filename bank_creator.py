import gspread
from google.oauth2.service_account import Credentials
import time
import logging
from datetime import datetime

from runtime_paths import get_creds_path
from ref_sheets_utils import resolve_sheet_id
# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Google credentials
CREDENTIALS_FILE = str(get_creds_path())
ref_sheets = "BANK"
tab_name_symbol = "SYMBOL"
tab_name_calc = "CALC"
tab_name_inp = "INP"
tab_name_bank_creator = "BANK_CREATOR"

# Authorize
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
client = gspread.authorize(creds)
sheet_id = resolve_sheet_id(ref_sheets)
spreadsheet = client.open_by_key(sheet_id)

# Sheets
symbol_sheet = spreadsheet.worksheet(tab_name_symbol)
calc_sheet = spreadsheet.worksheet(tab_name_calc)
inp_sheet = spreadsheet.worksheet(tab_name_inp)
bank_sheet = spreadsheet.worksheet(tab_name_bank_creator)

# Get symbols
symbols = symbol_sheet.col_values(1)[1:]  # Skip header (A2:A)

# Helper to clean and convert cell values
def clean_row(row):
    cleaned = []
    for cell in row:
        cell = cell.strip()
        # Check for empty
        if not cell:
            cleaned.append("")
            continue
        # Try parsing as date
        try:
            if "-" in cell and len(cell.split("-")[-1]) == 4:
                parsed_date = datetime.strptime(cell, "%d-%b-%Y").date()
                cleaned.append(parsed_date.isoformat())
                continue
        except:
            pass
        # Try parsing as float
        try:
            cleaned.append(float(cell.replace(",", "")))
        except:
            cleaned.append(cell)
    return cleaned

# Process each symbol
for i, symbol in enumerate(symbols):
    logging.info(f"🔄 [{i+1}/{len(symbols)}] Processing symbol: {symbol}")

    # 1. Set CALC!A1 = symbol
    calc_sheet.update_acell("A1", symbol)
    logging.info(f"🟡 Triggered calculation for symbol: {symbol}")

    # 2. Wait until CALC!T1 == 9 (up to 3 decimal places)
    for attempt in range(60):  # Max ~60 seconds
        t1_value = calc_sheet.acell("T1").value
        try:
            if round(float(t1_value), 3) == 9.000:
                logging.info(f"✅ Calculation complete for {symbol} (T1={t1_value}) after {attempt + 1} sec")
                break
        except:
            logging.debug(f"Attempt {attempt+1}: T1 not ready")
        time.sleep(1)
    else:
        logging.warning(f"⏰ Timed out waiting for T1=9 for {symbol}. Skipping.")
        continue

    # 3. Read & clean INP!A2:F900
    inp_data = inp_sheet.get("A2:F900")
    filtered_data = [clean_row(row) for row in inp_data if any(cell.strip() for cell in row)]

    if not filtered_data:
        logging.info(f"⚠️ No data found in INP sheet for {symbol}. Skipping.")
        continue

    # 4. Append to BANK sheet
    last_row = len(bank_sheet.get_all_values())
    start_row = last_row + 1
    needed_rows = start_row + len(filtered_data)

    if needed_rows > bank_sheet.row_count:
        rows_to_add = needed_rows - bank_sheet.row_count
        bank_sheet.add_rows(rows_to_add)
        logging.info(f"📐 Added {rows_to_add} rows to {tab_name_bank_creator} to fit incoming data.")

    # Update
    bank_sheet.update(values=filtered_data, range_name=f"A{start_row}")
    logging.info(f"📦 Appended {len(filtered_data)} cleaned rows to {tab_name_bank_creator} for {symbol}")

logging.info("🎉✅ All symbols processed successfully.")
