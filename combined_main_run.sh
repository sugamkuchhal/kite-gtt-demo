#!/bin/bash

python3 date_ext.py
python3 data_val.py

echo "Running: SGST Reversal Validation"
python3 ops_sort.py --ref-sheets "SGST" --green-tab "GTT_List" --red-tab "Old_GTT_List" --yellow-tab "Action_List"

echo "Running: Super BreakOut"
python3 ops_sort.py --ref-sheets "SUPER" --green-tab "GTT_List" --red-tab "Old_GTT_List" --yellow-tab "Action_List"

echo "Running: Turtle Trading"
python3 ops_sort.py --ref-sheets "TURTLE" --green-tab "GTT_List" --red-tab "Old_GTT_List" --yellow-tab "Action_List"

if python3 "$(dirname "$0")/is_trigger_true.py" | grep -qi true; then
    echo "Running: KWK"
    python3 ops_sort.py --ref-sheets "KWK" --green-tab "MKT_List" --red-tab "OLD_MKT_List" --yellow-tab "Action_List"
    python3 ops_sort_kwk.py --ref-sheets "KWK" --kwk-sheet "KWK" --action-sheet "Action_List" --special-target-ref-sheets "PORTFOLIO" --special-target-sheet "SPECIAL_TARGET_KWK_SIP_REG"

    echo ""
    echo "Running: SIP_REG"
    python3 ops_sort.py --ref-sheets "KWK" --green-tab "SIP_REG_List" --red-tab "OLD_SIP_REG_List" --yellow-tab "Action_SIP_REG_List"
    python3 ops_sort_sip_reg.py --ref-sheets "KWK" --action-sheet "OLD_SIP_REG_List" --special-target-ref-sheets "PORTFOLIO" --special-target-sheet "SPECIAL_TARGET_KWK_SIP_REG" --uncheck

    python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "MKT_INS" --market-order
    echo ""
fi

echo "Running: Portfolio Stocks (GTT)"
python3 ops_sort.py --ref-sheets "PORTFOLIO" --green-tab "GTT_List" --red-tab "Old_GTT_List" --yellow-tab "Action_GTT_List" --loose-update

echo "Running: Portfolio Stocks (TSL)"
python3 ops_sort.py --ref-sheets "PORTFOLIO" --green-tab "TSL_List" --red-tab "Old_TSL_List" --yellow-tab "Action_TSL_List" --loose-update

echo "Running: RTP Salvaging"
python3 ops_sort.py --ref-sheets "RTP" --green-tab "GTT_List" --red-tab "Old_GTT_List" --yellow-tab "Action_List"

# echo "Running: 100 DMA Stock Screener"
# python3 ops_sort.py --sheet-name="SARAS D M B - 100 DMA Stock Screener with BOH" --green-tab="MKT_List" --red-tab="OLD_MKT_List" --yellow-tab="Action_List"

# echo "Running: Consolidated BreakOut"
# python3 ops_sort.py --sheet-name="SARAS D M B - Consolidated BreakOut with BOH" --green-tab="MKT_List" --red-tab="OLD_MKT_List" --yellow-tab="Action_List"

echo "Running: Mailer List"
python3 ops_sort.py --ref-sheets "PORTFOLIO" --green-tab "Mailing_List" --red-tab "Old_Mailing_List" --yellow-tab "Action_Mailing_List" --loose-update

echo "✅ All tasks completed."
