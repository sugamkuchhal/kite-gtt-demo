
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "DEL_GTT_INS"
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "INS_GTT_INS"
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "GTT_INS"
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "ALTER_GTT_INS"
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "INS_GTT_INS"

python3 ops_sort.py --ref-sheets "PORTFOLIO" --green-tab "Mailing_List" --red-tab "Old_Mailing_List" --yellow-tab "Action_Mailing_List" --loose-update
python3 algo_tickers_mailer.py --emails "sugam.kuchhal.iimc@gmail.com"

python3 all_old_gtt_ins_backup.py
