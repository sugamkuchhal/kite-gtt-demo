# config.py
from ref_sheets_utils import resolve_sheet_id

# Sheet IDs and names
ref_sheets = "PORTFOLIO"
sheet_id = resolve_sheet_id(ref_sheets)
tab_name_instruction = "GTT_INSTRUCTIONS"  # Instruction sheet tab name
tab_name_data_management = "GTT_DATA"  # Data management sheet tab name

BATCH_SIZE = 1000

# Columns expected in the GTT_INSTRUCTIONS sheet
INSTRUCTIONS_COLUMNS = {
    "TICKER": "TICKER",
    "TYPE": "TYPE",
    "UNITS": "UNITS",
    "PRICE": "GTT PRICE",
    "DATE": "GTT DATE",
    "ACTION": "ACTION",
    "METHOD": "METHOD",
    "STATUS": "STATUS",
    "LIVE_PRICE": "LIVE PRICE",
    "TICK_SIZE": "TICK SIZE"
}

# Columns expected in the GTT_DATA sheet
DATA_COLUMNS = {
    "TICKER": "TICKER",
    "TYPE": "TYPE",
    "UNITS": "UNITS",
    "PRICE": "GTT PRICE",
    "DATE": "GTT DATE",
    "GTT_ID": "GTT ID"
}
