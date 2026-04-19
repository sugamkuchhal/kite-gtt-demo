from ref_sheets import REF_SHEETS


def resolve_ref_sheet(ref_name: str) -> dict:
    """Return reference sheet metadata for a logical key (e.g. FEED, PORTFOLIO)."""
    if not ref_name:
        raise ValueError("ref_name is required")

    key = str(ref_name).strip().upper()
    meta = REF_SHEETS.get(key)
    if not meta:
        valid = ", ".join(sorted(REF_SHEETS.keys()))
        raise ValueError(f"Unknown ref sheet '{ref_name}'. Valid values: {valid}")

    return {"name": key, **meta}


def resolve_ref_sheet_id(ref_name: str) -> str:
    sheet = resolve_ref_sheet(ref_name)
    sheet_id = sheet.get("sheet_id") or sheet.get("url")
    if not sheet_id:
        raise ValueError(f"Ref sheet '{ref_name}' is missing both 'sheet_id' and 'url'")
    return sheet_id
