#!/bin/bash
set -euo pipefail

python3 prepare_feed_date_ext.py
python3 prepare_feed_data_val.py
python3 prepare_feed_list.py --ref-sheets "FEED" --source-sheet "SGST_OPEN_LIST" --dest-sheet "SGST_FILTERED_TICKERS"
python3 prepare_feed_list.py --ref-sheets "FEED" --source-sheet "SUPER_OPEN_LIST" --dest-sheet "SUPER_FILTERED_TICKERS"
python3 prepare_feed_list.py --ref-sheets "FEED" --source-sheet "TURTLE_OPEN_LIST" --dest-sheet "TURTLE_FILTERED_TICKERS"
