
bash combined_home_run_eod.sh

python3 algo_tickers_mailer.py --emails "sugam.kuchhal.iimc@gmail.com"
python3 corporate_actions_mailer.py --emails "sugam.kuchhal.iimc@gmail.com"
python3 dividend_action_mailer.py --emails "sugam.kuchhal.iimc@gmail.com"
python3 algo_winners_mailer.py --emails "sugam.kuchhal.iimc@gmail.com,sharma.virat@gmail.com"

python3 all_old_gtt_ins_backup.py

python3 db/display_push.py
