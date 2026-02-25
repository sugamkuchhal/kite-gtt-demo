"""Central ALGO_NAME -> Spreadsheet ID lookup (Phase 2)."""

ALGO_SHEET_ID_BY_NAME = {
    # Confirmed IDs from codebase
    "PORTFOLIO_STOCKS": "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s",
    "GTT_MASTER": "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s",
    "ALGO_MASTER_DATA_BANK": "1TX4Q8YG0-d2_L1YOhvb9OYDgklvHj3eFK76JN7Pdavg",
    "NSE_MARKET_DATA_BANK": "143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE",
    "DATA_TELEPORT_DEST": "1IZJYejcWZN72f_3Fm1L2IHgbjVxOthTfqwnsnynCcXk",

    # Pending IDs to be provided by user
    "ALGO_MASTER_FEED_SHEET": "1ayv2CiY078ZaZHVl2LIQNNcedEuOC56bR1fL6JMBQaM",
    "KWK_DEEP_BEAR_REVERSAL": "1Hfmx691NWmRVIKP4Qqel4JQ7lLk-4IQc22COSopr-gs",
    "RTP_REVERSE_TRIGGER_POINT_SALVAGING": "1z5kRtLfDkBNWeA2hVDwxM-cdLpyYxa82rMcLHv3jCA8",
    "DMB_100_DMA_STOCK_SCREENER_WITH_BOH": "1LohCh021TNOdTIAJODwcu-lbeQMBPQNaSl4VI1jLaqY",
    "DMB_CONSOLIDATED_BREAKOUT_WITH_BOH": "10mE4Pbdk0NqD_dKzg8T4imqrn-36rXJzLhZcFbjfREc",
    "US_DGB_SGST_REVERSAL_VALIDATION_WITH_BOH": "1r9tdr82pgpu91HAVfLY_4MsDgofG9bU5hFWsyJkK930",
}


def get_sheet_id(algo_name: str) -> str:
    if algo_name not in ALGO_SHEET_ID_BY_NAME:
        raise KeyError(f"Unknown ALGO_NAME '{algo_name}'. Add it to algo_sheets_lookup.py")
    spreadsheet_id = ALGO_SHEET_ID_BY_NAME[algo_name]
    if not spreadsheet_id:
        raise ValueError(
            f"Spreadsheet ID missing for ALGO_NAME '{algo_name}'. "
            "Please populate algo_sheets_lookup.py before running this script."
        )
    return spreadsheet_id
