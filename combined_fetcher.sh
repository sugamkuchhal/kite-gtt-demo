#!/bin/bash
set -euo pipefail

if python3 "$(dirname "$0")/is_trigger_true.py" | grep -qi true; then
    echo "Running: NSE_Full_Stock_Data"
    python3 nse_combined_fetcher.py --mode stock --worksheet "NSE_Full_Stock_Data" --max-workers 3 --batch-size 50
    echo ""
fi

python3 nse_combined_fetcher.py --mode stock --ticker-file nse_stock_list.txt --worksheet "NSE_Stock_Data" --max-workers 3 --batch-size 50
python3 nse_combined_fetcher.py --mode etf --ticker-file nse_etf_list.txt --worksheet "NSE_ETF_Data" --max-workers 3 --batch-size 50
python3 nse_data_etl.py
python3 data_teleporter.py --mode inc
