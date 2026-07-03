#!/bin/bash

echo "Running: MIDDAY GTT Processor"
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "DEL_GTT_INS"
python3 gtt_processor.py --ref-sheets "PORTFOLIO" --sheet-name "INS_GTT_INS"

echo "✅ All tasks completed."
