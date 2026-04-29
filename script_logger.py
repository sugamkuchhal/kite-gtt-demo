from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
import time

IST = ZoneInfo("Asia/Kolkata")


@dataclass
class RunLogContext:
    script_name: str
    start_epoch: float
    start_label: str


def log_start(script_name: str) -> RunLogContext:
    start_epoch = time.time()
    start_label = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

    print()
    print("=================")
    print(f"==={script_name}===")
    print(f"==={start_label}===")
    print("=================")
    print()

    return RunLogContext(script_name=script_name, start_epoch=start_epoch, start_label=start_label)


def log_end(ctx: RunLogContext) -> None:
    end_epoch = time.time()
    end_label = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
    total_seconds = int(end_epoch - ctx.start_epoch)

    print()
    print("=================")
    print(f"==={ctx.script_name}===")
    print(f"==={end_label}===")
    print(f"==={total_seconds} seconds===")
    print("=================")
    print()
