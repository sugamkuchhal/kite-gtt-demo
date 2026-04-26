from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from runtime_paths import repo_root

REF_SHEETS_JSON_PATH = repo_root() / "ref_sheets.json"


def _load_ref_sheets_payload(path: Path | None = None) -> dict[str, Any]:
    target = path or REF_SHEETS_JSON_PATH
    with target.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload


def _normalize_ref_key(ref_sheets: str) -> str:
    return ref_sheets.strip().upper()


def resolve_sheet_meta(ref_sheets: str, path: Path | None = None) -> dict[str, Any]:
    """
    Resolve a ref-sheets key (case-insensitive) to its metadata row from ref_sheets.json.
    Raises ValueError when key is not found.
    """
    normalized_key = _normalize_ref_key(ref_sheets)
    rows = _load_ref_sheets_payload(path).get("rows", [])

    for row in rows:
        row_key = str(row.get("ref-sheets", "")).strip().upper()
        if row_key == normalized_key:
            return row

    raise ValueError(f"Unknown ref_sheets key: '{ref_sheets}'")


def resolve_sheet_id(ref_sheets: str, path: Path | None = None) -> str:
    """
    Resolve ref-sheets key (case-insensitive) to sheet-id.
    Raises ValueError when key is not found or sheet-id is empty.
    """
    meta = resolve_sheet_meta(ref_sheets, path=path)
    sheet_id = str(meta.get("sheet-id", "")).strip()
    if not sheet_id:
        raise ValueError(f"Missing sheet-id for ref_sheets key: '{ref_sheets}'")
    return sheet_id
