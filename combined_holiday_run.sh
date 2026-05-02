#!/usr/bin/env bash
set -euo pipefail

python3 zerodha_tick_size.py
python3 nse_combined_fetcher.py --mode stock --ticker-file nse_stock_list.txt --worksheet "NSE_Stock_Data" --max-workers 3 --batch-size 50
python3 nse_combined_fetcher.py --mode etf --ticker-file nse_etf_list.txt --worksheet "NSE_ETF_Data" --max-workers 3 --batch-size 50
python3 nse_data_etl.py
python3 prepare_feed_date_ext.py
python3 prepare_feed_data_val.py
python3 prepare_feed_list.py --ref-sheets "FEED" --source-sheet "SGST_OPEN_LIST" --dest-sheet "SGST_FILTERED_TICKERS"
python3 prepare_feed_list.py --ref-sheets "FEED" --source-sheet "SUPER_OPEN_LIST" --dest-sheet "SUPER_FILTERED_TICKERS"
python3 prepare_feed_list.py --ref-sheets "FEED" --source-sheet "TURTLE_OPEN_LIST" --dest-sheet "TURTLE_FILTERED_TICKERS"
python3 set_field_false.py
python3 fetch_all_gtts.py
python3 fetch_holdings.py
python3 date_ext.py
python3 data_val.py
bash combined_run.sh
bash combined_home_run.sh
python3 ops_sort.py --ref-sheets "PORTFOLIO" --green-tab "Mailing_List" --red-tab "Old_Mailing_List" --yellow-tab "Action_Mailing_List" --loose-update
python3 algo_tickers_mailer.py --emails "sugamkuchhal@gmail.com"
