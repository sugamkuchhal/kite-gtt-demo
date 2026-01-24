import pandas as pd
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from runtime_paths import get_creds_path

# --- CONFIG ---
SPREADSHEET_NAME = "SARAS Portfolio - Stocks"
WORKSHEET_NAME = "ALL_ORDERS"
CREDENTIALS_FILE = str(get_creds_path())

# --- STEP 1: DOWNLOAD DATA FROM GOOGLE SHEETS ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
client = gspread.authorize(creds)

sheet = client.open(SPREADSHEET_NAME)
worksheet = sheet.worksheet(WORKSHEET_NAME)
data = worksheet.get_all_records()

df = pd.DataFrame(data)

# Parse dates (handles `19-Mar-2025` etc.)
df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True, errors='coerce')

# --- STEP 2: FIFO PROCESSING ---
# Sort for FIFO; add 'Order ID' for stable tie-breaks if present
sort_cols = [c for c in ['TICKER', 'DATE', 'TYPE', 'Order ID'] if c in df.columns]
df = df.sort_values(by=sort_cols, kind='mergesort')

today = pd.to_datetime("today").normalize()

portfolio = []
buy_status_records = []
sell_trade_records = []
buy_sell_match_rows = []  # NEW: stacked BUY↔SELL matches

# Optional map (kept from your code, though we now prefer row['CATEGORY'] per order)
if 'CATEGORY' in df.columns:
    ticker_to_category = df.set_index('TICKER')['CATEGORY'].to_dict()
else:
    ticker_to_category = {}

for ticker, group in df.groupby("TICKER"):
    buys = []
    # fallback category if needed
    fallback_category = ticker_to_category.get(ticker, "")

    for _, row in group.iterrows():
        # --- Normalize row fields ---
        units = int(pd.to_numeric(row['UNITS'], errors='coerce')) if 'UNITS' in row else 0
        # PRICE may come as '1,234.56'
        price = float(str(row['PRICE']).replace(",", "")) if 'PRICE' in row else 0.0
        date = row['DATE']
        order_id = row.get('Order ID', '')
        method = row.get('METHOD', '')
        category_row = row.get('CATEGORY', fallback_category)
        trade_type = str(row.get('TYPE', '')).upper()

        if trade_type == 'BUY':
            buy_entry = {
                'units': units,
                'price': price,
                'date': date,
                'original_units': units,
                'order_id': order_id,
                'ticker': ticker,
                'category': category_row,   # preserve BUY category per row
                'method': method,           # preserve BUY method per row
                'realized_amount': 0.0,
                # NEW fields for stacked output
                'head_emitted': False,
                'buy_group_id': f"{ticker}|{order_id}"
            }
            buys.append(buy_entry)
            buy_status_records.append(buy_entry)

        elif trade_type == 'SELL':
            remaining = units
            touched_buys = []
            trade_amount = 0.0
            day_amt_gap = 0.0

            sell_method = method                 # preserve SELL method
            sell_category = category_row         # preserve SELL category

            while remaining > 0 and buys:
                buy = buys[0]
                available = buy['units']
                used = min(available, remaining)

                # Track per-buy sell breakdown for day-amount-gap later
                if 'sell_breakdown' not in buy:
                    buy['sell_breakdown'] = []
                buy['sell_breakdown'].append({
                    'date': date,
                    'units': used
                })

                # Compute cost and day-amount-gap contribution for SELL-level metrics
                age_days = (date - buy['date']).days + 1
                cost = used * buy['price']
                trade_amount += cost
                day_amt_gap += cost * age_days
                touched_buys.append(buy)

                # Update realized and inventory
                buy['realized_amount'] += used * price
                buy['units'] -= used
                remaining -= used

                # --- NEW: emit a row for this BUY↔SELL match (handles partials) ---
                is_head = not buy['head_emitted']
                buy['head_emitted'] = True

                buy_sell_match_rows.append({
                    'TICKER': ticker,
                    'BUY_GROUP_ID': buy['buy_group_id'],
                    'BUY_ROW_IS_HEAD': is_head,
                    'BUY_ID': buy['order_id'],
                    'BUY_DATE': buy['date'],               # upload_to_sheet formats dates
                    'BUY_CATEGORY': buy['category'],
                    'BUY_METHOD': buy['method'],
                    'BUY_UNITS': buy['original_units'],
                    'BUY_PRICE': buy['price'],
                    'SELL_ID': order_id,
                    'SELL_DATE': date,
                    'SELL_UNITS': units,                    # full units for this SELL order
                    'SELL_PRICE': price,
                    'SELL_CATEGORY': sell_category,         # NEW
                    'SELL_METHOD': sell_method,             # NEW
                    'MATCHED_UNITS': used,                  # portion matched to THIS BUY
                    'PNL_PER_MATCH': round((price - buy['price']) * used, 2),
                    'BUY_UNITS_LEFT_AFTER': buy['units']
                })
                # --- END NEW ---

                if buy['units'] == 0:
                    buys.pop(0)

            # Record SELL-level aggregate (kept exactly like your code)
            sell_record = {
                'Order ID': order_id,
                'TICKER': ticker,
                'CATEGORY': category_row,
                'TYPE': 'SELL',
                'UNITS': units,
                'PRICE': price,
                'DATE': date,
                'METHOD': method,
                'TRADE AMOUNT': round(trade_amount, 2),
                'TRADE COUNT': len(touched_buys),
                'DAY AMOUNT GAP': round(day_amt_gap, 2)
            }
            sell_trade_records.append(sell_record)

    # --- Emit head-only rows for BUYs untouched by any SELL ---
    for b in buys:
        if not b.get('head_emitted', False):
            buy_sell_match_rows.append({
                'TICKER': ticker,
                'BUY_GROUP_ID': b['buy_group_id'],
                'BUY_ROW_IS_HEAD': True,
                'BUY_ID': b['order_id'],
                'BUY_DATE': b['date'],
                'BUY_CATEGORY': b['category'],
                'BUY_METHOD': b['method'],
                'BUY_UNITS': b['original_units'],
                'BUY_PRICE': b['price'],
                'SELL_ID': '',
                'SELL_DATE': '',
                'SELL_UNITS': '',
                'SELL_PRICE': '',
                'SELL_CATEGORY': '',          # NEW
                'SELL_METHOD': '',            # NEW
                'MATCHED_UNITS': '',
                'PNL_PER_MATCH': '',
                'BUY_UNITS_LEFT_AFTER': b['units']
            })

    # --- Open position snapshot for this ticker (unchanged) ---
    open_units = sum(b['units'] for b in buys)
    open_amount = sum(b['units'] * b['price'] for b in buys)
    open_trade_count = len([b for b in buys if b['units'] > 0])
    open_day_amt_gap = sum(((today - b['date']).days + 1) * b['units'] * b['price'] for b in buys)

    if open_units > 0:
        portfolio.append({
            'TICKER': ticker,
            'CATEGORY': fallback_category,  # historical behavior retained
            'OPEN UNITS': open_units,
            'OPEN AMOUNT': round(open_amount, 2),
            'OPEN TRADE COUNT': open_trade_count,
            'OPEN DAY AMOUNT GAP': round(open_day_amt_gap, 2)
        })

# --- STEP 3: BUY TRADE STATUS OUTPUT (unchanged) ---
buy_status_output = []
for b in buy_status_records:
    status = "OPEN" if b['units'] > 0 else "CLOSE"
    open_days = (today - b['date']).days if b['units'] > 0 else 0
    trade_amount = b['original_units'] * b['price']
    realized_amount = b['realized_amount']

    day_amount_gap = 0.0
    if 'sell_breakdown' in b:
        for sell in b['sell_breakdown']:
            gap_days = (sell['date'] - b['date']).days + 1
            day_amount_gap += sell['units'] * b['price'] * gap_days

    if b['units'] > 0:
        gap_days = (today - b['date']).days + 1
        day_amount_gap += b['units'] * b['price'] * gap_days

    row = {
        'Order ID': b.get('order_id', ''),
        'TICKER': b['ticker'],
        'CATEGORY': b['category'],
        'TYPE': 'BUY',
        'UNITS': b['original_units'],
        'PRICE': b['price'],
        'DATE': b['date'],
        'METHOD': b.get('method', ''),
        'STATUS': status,
        'UNSOLD UNITS': b['units'],
        'OPEN DAYS': open_days,
        'TRADE AMOUNT': round(trade_amount, 2),
        'REALIZED AMOUNT': round(realized_amount, 2),
        'CURRENT PRICE': '',   # placeholder; formulas applied on upload
        'UNREALIZED AMOUNT': '',
        'FINAL AMOUNT': '',
        'PROFIT AMOUNT': '',
        'PROFIT STATUS': '',
        'PROFIT %AGE': '',
        'DAY AMOUNT GAP': round(day_amount_gap, 2)
    }
    buy_status_output.append(row)

# --- STEP 4: UPLOAD TO GOOGLE SHEETS ---
def upload_to_sheet(title, df_data, apply_formulas=False):
    try:
        ws = sheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(title=title, rows="1000", cols="50")
    ws.clear()

    # Safe conversion: datetime → YYYY-MM-DD string
    def safe_value(val):
        if pd.isna(val):
            return ''
        if isinstance(val, (pd.Timestamp, datetime)):
            return val.strftime('%Y-%m-%d')
        return val

    # Make sure df_data is a DataFrame (can be empty)
    df_data = df_data if isinstance(df_data, pd.DataFrame) else pd.DataFrame(df_data)

    if df_data.empty:
        ws.update([["(no data)"]])
        return

    upload_values = [[safe_value(cell) for cell in row] for row in df_data.itertuples(index=False, name=None)]
    ws.update([df_data.columns.values.tolist()] + upload_values, value_input_option='USER_ENTERED')

    if apply_formulas:
        row_count = len(df_data) + 1
        formula_map = {
            'CURRENT PRICE': '=INDEX(SORT(GOOGLEFINANCE(B{r},"close",TODAY()-5,TODAY()),1,FALSE),2,2)',
            'UNREALIZED AMOUNT': '=J{r}*N{r}',
            'FINAL AMOUNT': '=M{r}+O{r}',
            'PROFIT AMOUNT': '=P{r}-L{r}',
            'PROFIT STATUS': '=IF(Q{r}>=0, "PROFIT", "LOSS")',
            'PROFIT %AGE': '=Q{r}/L{r}'
        }

        header = df_data.columns.tolist()
        for col_name in formula_map:
            if col_name in header:
                col_index = header.index(col_name) + 1
                formulas = [[formula_map[col_name].format(r=r)] for r in range(2, row_count + 1)]
                start_cell = gspread.utils.rowcol_to_a1(2, col_index)
                end_cell = gspread.utils.rowcol_to_a1(row_count, col_index)
                ws.update(range_name=f"{start_cell}:{end_cell}", values=formulas, value_input_option='USER_ENTERED')

# --- Prepare BUY_SELL_MATCHES DataFrame (sorted for readability) ---
matches_df = pd.DataFrame(buy_sell_match_rows)
if not matches_df.empty:
    # Stable sort: TICKER, BUY_DATE asc, BUY_ID asc; show head rows first
    matches_df['_BUY_DATE_SORT'] = pd.to_datetime(matches_df['BUY_DATE'], errors='coerce')
    matches_df = matches_df.sort_values(
        by=['TICKER', '_BUY_DATE_SORT', 'BUY_ID', 'BUY_ROW_IS_HEAD'],
        ascending=[True, True, True, False],
        kind='mergesort'
    ).drop(columns=['_BUY_DATE_SORT'])

# --- FINAL UPLOAD ---
upload_to_sheet("FIFO_Summary", pd.DataFrame(portfolio))
upload_to_sheet("Buy_Trade_Status", pd.DataFrame(buy_status_output), apply_formulas=True)
upload_to_sheet("Sell_Trade_Status", pd.DataFrame(sell_trade_records))
upload_to_sheet("BUY_SELL_MATCHES", matches_df)

print("✅ FIFO_Summary, Buy_Trade_Status, Sell_Trade_Status, and BUY_SELL_MATCHES updated.")
