#!/bin/bash

python3 prepare_us_feed_date_ext.py
python3 prepare_us_feed_data_val.py

python3 prepare_us_feed_list.py --sheet-name "US D G B - SGST (Reversal Validation) With BOH" \
    --source-sheet "US_OPEN_LIST" \
    --dest-sheet "US_FILTERED_TICKERS"

python3 ops_sort.py --sheet-name="US D G B - SGST (Reversal Validation) With BOH" \
    --green-tab="GTT_List" \
    --red-tab="Old_GTT_List" \
    --yellow-tab="Action_List"

echo "✅ All tasks completed."
