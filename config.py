# config.py

# Phase-2 Sheets config: ALGO_NAME + TAB_NAME (IDs live in algo_sheets_lookup.py)
INSTRUCTION_ALGO_NAME = "GTT_MASTER"
INSTRUCTION_TAB_NAME = "GTT_INSTRUCTIONS"

DATA_MANAGEMENT_ALGO_NAME = "GTT_MASTER"
DATA_MANAGEMENT_TAB_NAME = "GTT_DATA"

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
