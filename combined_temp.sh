
# python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "MKT_INS" --market-order
# python3 ops_sort.py --ref-sheets "PORTFOLIO" --green-tab "GTT_List" --red-tab "Old_GTT_List" --yellow-tab "Action_GTT_List" --loose-update
# python3 ops_sort.py --ref-sheets "PORTFOLIO" --green-tab "TSL_List" --red-tab "Old_TSL_List" --yellow-tab "Action_TSL_List" --loose-update
python3 ops_sort.py --ref-sheets "RTP" --green-tab "GTT_List" --red-tab "Old_GTT_List" --yellow-tab "Action_List"
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "DEL_GTT_INS"
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "INS_GTT_INS"
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "GTT_INS"
# python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "ALTER_GTT_INS"
# python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "INS_GTT_INS"
# python3 ops_sort.py --ref-sheets "PORTFOLIO" --green-tab "Mailing_List" --red-tab "Old_Mailing_List" --yellow-tab "Action_Mailing_List" --loose-update
# python3 algo_tickers_mailer.py --emails "sugamkuchhal@gmail.com"
python3 ALL_OLD_GTT_INS_BACKUP.py
