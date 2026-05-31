
gtt_processor --ref-sheets "PORTFOLIO" --sheet-name "MKT_INS" --market-order
; 
ops_sort --ref-sheets "PORTFOLIO" --green-tab "GTT_List" --red-tab "Old_GTT_List" --yellow-tab "Action_GTT_List" --loose-update
;
ops_sort --ref-sheets "PORTFOLIO" --green-tab "TSL_List" --red-tab "Old_TSL_List" --yellow-tab "Action_TSL_List" --loose-update
;
ops_sort --ref-sheets "RTP" --green-tab "GTT_List" --red-tab "Old_GTT_List" --yellow-tab "Action_List"
;
gtt_processor --ref-sheets "PORTFOLIO" --sheet-name "DEL_GTT_INS"
;
gtt_processor --ref-sheets "PORTFOLIO" --sheet-name "INS_GTT_INS"
;
gtt_processor --ref-sheets "PORTFOLIO" --sheet-name "GTT_INS"
;
gtt_processor --ref-sheets "PORTFOLIO" --sheet-name "ALTER_GTT_INS"
;
gtt_processor --ref-sheets "PORTFOLIO" --sheet-name "INS_GTT_INS"
;
ops_sort --ref-sheets "PORTFOLIO" --green-tab "Mailing_List" --red-tab "Old_Mailing_List" --yellow-tab "Action_Mailing_List" --loose-update
;
algo_tickers_mailer --emails "sugamkuchhal@gmail.com"
;
ALL_OLD_GTT_INS_BACKUP
;
