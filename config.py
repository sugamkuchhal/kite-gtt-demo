# config.py

# Sheet IDs and names
INSTRUCTION_SHEET_ID = "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s"
INSTRUCTION_SHEET_NAME = "GTT_INSTRUCTIONS"  # Instruction sheet tab name

DATA_MANAGEMENT_SHEET_ID = "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s"
DATA_MANAGEMENT_SHEET_NAME = "GTT_DATA"  # Data management sheet tab name

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
