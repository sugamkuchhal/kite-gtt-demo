#!/bin/bash
set -euo pipefail

python3 nse_combined_fetcher.py --mode stock --ticker-file nse_stock_list.txt --worksheet "NSE_Stock_Data" --max-workers 3 --batch-size 50
python3 nse_combined_fetcher.py --mode etf --ticker-file nse_etf_list.txt --worksheet "NSE_ETF_Data" --max-workers 3 --batch-size 50

python3 nse_data_etl.py
python3 data_teleporter.py --mode inc
