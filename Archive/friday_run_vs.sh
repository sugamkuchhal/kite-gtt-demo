#!/bin/bash

echo "Running: SGST Reversal Validation"

python3 ops_sort_vs.py --sheet-name="VS D G B - SGST (Reversal Validation) With BOH" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_List"

echo "Running: Super BreakOut"

python3 ops_sort_vs.py --sheet-name="VS D G B - Super BreakOut With BOH" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_List"

echo "Running: Turtle Trading"

python3 ops_sort_vs.py --sheet-name="VS D G B - Turtle Trading with BOH" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_List"

echo "Running: KWK"

python3 ops_sort_vs.py --sheet-name="VS W M B - KWK (Deep Bear Reversal)" --green-tab="MKT_List" --red-tab="OLD_MKT_List" --yellow-tab="Action_List"

python3 ops_sort_kwk_vs.py --sheet-name "VS W M B - KWK (Deep Bear Reversal)" --kwk-sheet "KWK" --action-sheet "Action_List" --special-target-sheet-file "VS Portfolio" --special-target-sheet "SPECIAL_TARGET_KWK_SIP_REG"

echo "Running: SIP_REG"

python3 ops_sort_vs.py --sheet-name="VS W M B - KWK (Deep Bear Reversal)" --green-tab="SIP_REG_List" --red-tab="OLD_SIP_REG_List" --yellow-tab="Action_SIP_REG_List"

python3 ops_sort_sip_reg_vs.py --sheet-name "VS W M B - KWK (Deep Bear Reversal)" --action-sheet "OLD_SIP_REG_List" --special-target-sheet-file "VS Portfolio" --special-target-sheet "SPECIAL_TARGET_KWK_SIP_REG" --uncheck

echo "Running: Portfolio Stocks (GTT)"

python3 ops_sort_vs.py --sheet-name="VS Portfolio" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_GTT_List" --loose-update

echo "Running: Portfolio Stocks (TSL)"

python3 ops_sort_vs.py --sheet-name="VS Portfolio" --green-tab="TSL_List" --red-tab="Old_TSL_List" --yellow-tab="Action_TSL_List" --loose-update

echo "Running: RTP Salvaging"

python3 ops_sort_vs.py --sheet-name="VS D G C - RTP (Reverse Trigger Point Salvaging)" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_List"

# echo "Running: 100 DMA Stock Screener"

# python3 ops_sort_vs.py --sheet-name="VS D M B - 100 DMA Stock Screener with BOH" --green-tab="MKT_List" --red-tab="OLD_MKT_List" --yellow-tab="Action_List"

# echo "Running: Consolidated BreakOut"

# python3 ops_sort_vs.py --sheet-name="VS D M B - Consolidated BreakOut with BOH" --green-tab="MKT_List" --red-tab="OLD_MKT_List" --yellow-tab="Action_List"

echo "Running: GTT Processor"

python3 gtt_processor_vs.py --sheet-id "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI" --sheet-name "GTT_INSTRUCTIONS"
python3 gtt_processor_vs.py --sheet-id "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI" --sheet-name "DEL_GTT_INSTRUCTIONS"
python3 gtt_processor_vs.py --sheet-id "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI" --sheet-name "INS_GTT_INSTRUCTIONS"
python3 gtt_processor_vs.py --sheet-id "145TqrpQ3Twx6Tezh28s5GnbowlBb_qcY5UM1RvfIclI" --sheet-name "GTT_INSTRUCTIONS"

echo "✅ All tasks completed."
