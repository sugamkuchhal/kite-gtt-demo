#!/usr/bin/env bash
set -euo pipefail

python3 fetch_all_gtts.py
python3 fetch_holdings.py
python3 date_ext.py
python3 data_val.py
