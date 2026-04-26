from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runtime_paths import repo_root

REF_SHEET_ID = "143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE"
REF_TAB_NAME = "Ref_Sheets"
REF_GID = 169959016
REF_COLUMNS = ["ref-sheets", "type", "sheet-id", "sheet-name"]
REF_JSON_PATH = repo_root() / "ref_sheets.json"

SEED_ROWS = [
    {
        "ref-sheets": "CONSOLIDATED",
        "type": "ALGO",
        "sheet-id": "10mE4Pbdk0NqD_dKzg8T4imqrn-36rXJzLhZcFbjfREc",
        "sheet-name": "SARAS D M B - Consolidated BreakOut with BOH",
    },
    {
        "ref-sheets": "HUNDRED",
        "type": "ALGO",
        "sheet-id": "1LohCh021TNOdTIAJODwcu-lbeQMBPQNaSl4VI1jLaqY",
        "sheet-name": "SARAS D M B - 100 DMA Stock Screener with BOH",
    },
    {
        "ref-sheets": "KWK",
        "type": "ALGO",
        "sheet-id": "1Hfmx691NWmRVIKP4Qqel4JQ7lLk-4IQc22COSopr-gs",
        "sheet-name": "SARAS W M B - KWK (Deep Bear Reversal)",
    },
    {
        "ref-sheets": "PORTFOLIO",
        "type": "ALGO",
        "sheet-id": "14G8Yinl28F9ZROedyhiH4p5jCz2bcfA2goVB21PVE1s",
        "sheet-name": "SARAS Portfolio - Stocks",
    },
    {
        "ref-sheets": "RTP",
        "type": "ALGO",
        "sheet-id": "1z5kRtLfDkBNWeA2hVDwxM-cdLpyYxa82rMcLHv3jCA8",
        "sheet-name": "SARAS D G C - RTP (Reverse Trigger Point Salvaging)",
    },
    {
        "ref-sheets": "SGST",
        "type": "ALGO",
        "sheet-id": "1Zg0kKrySRcEko8FhPeFU_Dcr7giCmzxdkpVqgx1lhFM",
        "sheet-name": "SARAS D G B - SGST (Reversal Validation) With BOH",
    },
    {
        "ref-sheets": "SUPER",
        "type": "ALGO",
        "sheet-id": "1H-VK8w_5TJl-sMPKnB5sY1sKFcs2oHEolIyTVwxSUgk",
        "sheet-name": "SARAS D G B - Super BreakOut With BOH",
    },
    {
        "ref-sheets": "TURTLE",
        "type": "ALGO",
        "sheet-id": "1mHnUq9BeEIr-PX2CDpfkkxHBxyKqXg6rdDhcG4CqqsE",
        "sheet-name": "SARAS D G B - Turtle Trading with BOH",
    },
    {
        "ref-sheets": "BANK",
        "type": "BASE",
        "sheet-id": "1TX4Q8YG0-d2_L1YOhvb9OYDgklvHj3eFK76JN7Pdavg",
        "sheet-name": "Algo Master Data Bank",
    },
    {
        "ref-sheets": "CALCULATOR",
        "type": "BASE",
        "sheet-id": "1IZJYejcWZN72f_3Fm1L2IHgbjVxOthTfqwnsnynCcXk",
        "sheet-name": "Algo Master Data Calculator",
    },
    {
        "ref-sheets": "FEED",
        "type": "BASE",
        "sheet-id": "1ayv2CiY078ZaZHVl2LIQNNcedEuOC56bR1fL6JMBQaM",
        "sheet-name": "Algo Master Feed Sheet",
    },
    {
        "ref-sheets": "SECTOR",
        "type": "BASE",
        "sheet-id": "1GUJeD57cqkx0FXK2riq0zBc02KfP3jfFY6bzlid16a0",
        "sheet-name": "Algo Master Sector Sheet",
    },
    {
        "ref-sheets": "SEED",
        "type": "BASE",
        "sheet-id": "1Vsysm1iHaogaj3gBI8eM-uMa2-ZNGI4WU1QEpM6XF4Y",
        "sheet-name": "Algo Master Seed Sheet",
    },
    {
        "ref-sheets": "TICKER",
        "type": "BASE",
        "sheet-id": "143py3t5oTsz0gAfp8VpSJlpR5VS8Z4tfl067pMtW1EE",
        "sheet-name": "Algo Master Ticker Sheet",
    },
]


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({col: row.get(col, "") for col in REF_COLUMNS})
    return normalized


def _build_payload(rows: list[dict[str, Any]], source_mode: str) -> dict[str, Any]:
    normalized_rows = _normalize_rows(rows)
    return {
        "source": {
            "spreadsheet_id": REF_SHEET_ID,
            "tab_name": REF_TAB_NAME,
            "gid": REF_GID,
            "mode": source_mode,
        },
        "schema": {
            "columns": REF_COLUMNS,
            "header_row_index": 1,
        },
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": len(normalized_rows),
        "rows": normalized_rows,
    }


def _read_live_rows() -> list[dict[str, Any]]:
    from google_sheets_utils import read_sheet

    rows, _ = read_sheet(REF_SHEET_ID, REF_TAB_NAME)
    return rows


def create_or_refresh_ref_sheets_json(
    output_path: Path | None = None,
    prefer_live: bool = True,
) -> dict[str, Any]:
    """
    Create/refresh ref_sheets.json.
    - prefer_live=True: tries live Google Sheet first, falls back to seed rows.
    - prefer_live=False: writes using bundled seed rows.
    """
    path = output_path or REF_JSON_PATH

    source_mode = "seed"
    rows: list[dict[str, Any]] = SEED_ROWS
    if prefer_live:
        try:
            rows = _read_live_rows()
            source_mode = "live"
        except Exception:
            rows = SEED_ROWS
            source_mode = "seed_fallback"

    payload = _build_payload(rows, source_mode=source_mode)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return payload


if __name__ == "__main__":
    data = create_or_refresh_ref_sheets_json(prefer_live=True)
    print(
        f"Ref sheets JSON written ({data['row_count']} rows, mode={data['source']['mode']}) -> {REF_JSON_PATH}"
    )
