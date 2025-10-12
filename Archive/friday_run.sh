#!/bin/bash

echo "Running: SGST Reversal Validation"

python3 ops_sort.py --sheet-name="SARAS D G B - SGST (Reversal Validation) With BOH" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_List"

echo "Running: Super BreakOut"

python3 ops_sort.py --sheet-name="SARAS D G B - Super BreakOut With BOH" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_List"

echo "Running: Turtle Trading"

python3 ops_sort.py --sheet-name="SARAS D G B - Turtle Trading with BOH" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_List"

echo "Running: KWK"

python3 ops_sort.py --sheet-name="SARAS W M B - KWK (Deep Bear Reversal)" --green-tab="MKT_List" --red-tab="OLD_MKT_List" --yellow-tab="Action_List"

python3 ops_sort_kwk.py --sheet-name "SARAS W M B - KWK (Deep Bear Reversal)" --kwk-sheet "KWK" --action-sheet "Action_List" --special-target-sheet-file "SARAS Portfolio - Stocks" --special-target-sheet "SPECIAL_TARGET_KWK_SIP_REG"

echo "Running: SIP_REG"

python3 ops_sort.py --sheet-name="SARAS W M B - KWK (Deep Bear Reversal)" --green-tab="SIP_REG_List" --red-tab="OLD_SIP_REG_List" --yellow-tab="Action_SIP_REG_List"

python3 ops_sort_sip_reg.py --sheet-name "SARAS W M B - KWK (Deep Bear Reversal)" --action-sheet "OLD_SIP_REG_List" --special-target-sheet-file "SARAS Portfolio - Stocks" --special-target-sheet "SPECIAL_TARGET_KWK_SIP_REG" --uncheck

echo "Running: Portfolio Stocks (GTT)"

python3 ops_sort.py --sheet-name="SARAS Portfolio - Stocks" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_GTT_List" --loose-update

echo "Running: Portfolio Stocks (TSL)"

python3 ops_sort.py --sheet-name="SARAS Portfolio - Stocks" --green-tab="TSL_List" --red-tab="Old_TSL_List" --yellow-tab="Action_TSL_List" --loose-update

echo "Running: RTP Salvaging"

python3 ops_sort.py --sheet-name="SARAS D G C - RTP (Reverse Trigger Point Salvaging)" --green-tab="GTT_List" --red-tab="Old_GTT_List" --yellow-tab="Action_List"

# echo "Running: 100 DMA Stock Screener"

# python3 ops_sort.py --sheet-name="SARAS D M B - 100 DMA Stock Screener with BOH" --green-tab="MKT_List" --red-tab="OLD_MKT_List" --yellow-tab="Action_List"

# echo "Running: Consolidated BreakOut"

# python3 ops_sort.py --sheet-name="SARAS D M B - Consolidated BreakOut with BOH" --green-tab="MKT_List" --red-tab="OLD_MKT_List" --yellow-tab="Action_List"

echo "Running: GTT Processor"

python3 gtt_processor.py --sheet-id "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s" --sheet-name "GTT_INSTRUCTIONS"
python3 gtt_processor.py --sheet-id "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s" --sheet-name "DEL_GTT_INSTRUCTIONS"
python3 gtt_processor.py --sheet-id "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s" --sheet-name "INS_GTT_INSTRUCTIONS"
python3 gtt_processor.py --sheet-id "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s" --sheet-name "GTT_INSTRUCTIONS"

echo "✅ All tasks completed."
