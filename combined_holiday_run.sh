#!/usr/bin/env bash
set -euo pipefail

python3 fetch_all_gtts.py
python3 fetch_holdings.py
python3 date_ext.py
python3 data_val.py
bash combined_run.sh
bash combined_home_run.sh
python3 ops_sort.py --ref-sheets "PORTFOLIO" --green-tab "Mailing_List" --red-tab "Old_Mailing_List" --yellow-tab "Action_Mailing_List" --loose-update
python3 algo_tickers_mailer.py --emails "sugamkuchhal@gmail.com"
