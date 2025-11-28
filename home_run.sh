#!/bin/bash

echo "Running: GTT Processor"
python3 gtt_processor.py --sheet-id "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s" --sheet-name "DEL_GTT_INS"
python3 gtt_processor.py --sheet-id "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s" --sheet-name "INS_GTT_INS"
python3 gtt_processor.py --sheet-id "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s" --sheet-name "GTT_INS"
python3 gtt_processor.py --sheet-id "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s" --sheet-name "ALTER_GTT_INS"
python3 gtt_processor.py --sheet-id "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s" --sheet-name "INS_GTT_INS"

echo "✅ All tasks completed."
