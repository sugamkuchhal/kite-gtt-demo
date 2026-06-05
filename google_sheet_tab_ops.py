#!/usr/bin/env python3
"""
Run common Google Sheets tab maintenance operations.

Supported operations:
1. Formula copy: for each selected column, copy formulas and formatting from
   --start-row through that column's last non-empty row (or --end-row when
   supplied), then paste them one row down.
2. Hard copy: for each selected column, copy calculated values from --start-row
   through that column's last non-empty row (or --end-row when supplied), then
   paste them one row down and make the destination format match the source
   format.
3. Date update: set a specific A1 cell to a supplied date/value.

Authentication uses the same service-account credentials path as the rest of
this repository (runtime_paths.get_creds_path()). Share the target Google Sheet
with that service-account email before running this script. The --sheet-id value
may be either a raw Google spreadsheet ID/key or a ref_sheets key such as SARAS.
"""

from __future__ import annotations

import argparse
import re
import textwrap
from dataclasses import dataclass
from typing import Iterable, Sequence

SHEETS_SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

A1_COLUMN_RE = re.compile(r"^[A-Za-z]+$")


@dataclass(frozen=True)
class ColumnRef:
    """A resolved sheet column."""

    name: str
    index: int
    letter: str


@dataclass(frozen=True)
class RowCopyResult:
    """Source and destination rows used for one column copy operation."""

    column: ColumnRef
    source_start_row: int
    source_end_row: int
    destination_start_row: int
    destination_end_row: int


def col_num_to_letter(col_num: int) -> str:
    """Convert a 1-based column number to an A1 column letter."""
    if col_num < 1:
        raise ValueError(f"Column number must be >= 1, got {col_num}.")

    letters = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def col_letter_to_num(col_letter: str) -> int:
    """Convert an A1 column letter to a 1-based column number."""
    if not A1_COLUMN_RE.match(col_letter):
        raise ValueError(f"Invalid A1 column letter: {col_letter!r}")

    number = 0
    for char in col_letter.upper():
        number = number * 26 + (ord(char) - ord("A") + 1)
    return number


def split_csv(value: str | Sequence[str] | None) -> list[str]:
    """Parse comma-separated CLI option values into trimmed items.

    The CLI accepts multiple columns in one option, such as ``A,C,F``, and also
    allows the same option to be repeated, such as ``--formula-columns A
    --formula-columns C,F``.
    """
    if not value:
        return []

    values = [value] if isinstance(value, str) else value
    parsed_items: list[str] = []
    for raw_value in values:
        parsed_items.extend(item.strip() for item in raw_value.split(",") if item.strip())
    return parsed_items


def resolve_columns(sheet, columns: Sequence[str], header_row: int) -> list[ColumnRef]:
    """
    Resolve user-supplied columns.

    Each column may be an A1 column letter (A, Z, AA) or a header name found in
    --header-row. Header names are matched exactly after trimming whitespace.
    """
    if not columns:
        return []

    headers = [str(header).strip() for header in sheet.row_values(header_row)]
    resolved: list[ColumnRef] = []

    for raw_col in columns:
        col = raw_col.strip()
        is_a1_letter = A1_COLUMN_RE.match(col) is not None
        is_short_a1_letter = is_a1_letter and len(col) <= 2

        # Prefer obvious A1 letters such as A, Z, AA. For longer all-letter
        # values, prefer an exact header match so headers like DATE or PRICE are
        # not accidentally treated as very distant columns.
        if is_short_a1_letter or (is_a1_letter and col not in headers):
            index = col_letter_to_num(col)
            if index <= getattr(sheet, "col_count", index):
                resolved.append(ColumnRef(name=col.upper(), index=index, letter=col.upper()))
                continue

        if col not in headers:
            raise ValueError(
                f"Column {col!r} is neither an A1 column letter nor a header in row {header_row}."
            )
        index = headers.index(col) + 1
        resolved.append(ColumnRef(name=col, index=index, letter=col_num_to_letter(index)))

    return resolved


def detect_last_non_empty_row_in_column(sheet, column: ColumnRef) -> int:
    """Return the last row number containing any value in one column."""
    values = sheet.col_values(column.index)
    for offset, cell in enumerate(reversed(values)):
        if str(cell).strip():
            return len(values) - offset
    return 0


def get_column_end_row(sheet, column: ColumnRef, explicit_end_row: int | None) -> int:
    """Return the explicit end row, or detect it independently for the column."""
    if explicit_end_row is not None:
        return explicit_end_row
    return detect_last_non_empty_row_in_column(sheet, column)


def make_grid_range(sheet_id: int, start_row: int, end_row: int, col_index: int) -> dict:
    """Build a zero-indexed, end-exclusive single-column GridRange."""
    return {
        "sheetId": sheet_id,
        "startRowIndex": start_row - 1,
        "endRowIndex": end_row,
        "startColumnIndex": col_index - 1,
        "endColumnIndex": col_index,
    }


def fill_formulas_down(
    spreadsheet,
    sheet,
    columns: Iterable[ColumnRef],
    start_row: int,
    explicit_end_row: int | None = None,
) -> list[RowCopyResult]:
    """
    Copy formulas and cell formatting one row down for each selected column.

    Each column gets its own end row. For example, with the default
    ``--start-row 4``, column A ending at row 100, and column C ending at row
    80, the source ranges are A4:A100 and C4:C80 and the destination ranges are
    A5:A101 and C5:C81. Google Sheets copyPaste requests are used so relative
    references are adjusted the same way a manual copy/paste would adjust them
    in the UI. The formula paste is followed by a format paste so fonts, colors,
    number formats, borders, and similar cell formatting are preserved from the
    source rows.
    """
    requests = []
    results: list[RowCopyResult] = []

    for col in columns:
        end_row = get_column_end_row(sheet, col, explicit_end_row)
        if end_row < start_row:
            continue

        destination_start_row = start_row + 1
        destination_end_row = end_row + 1
        source = make_grid_range(sheet.id, start_row, end_row, col.index)
        destination = make_grid_range(sheet.id, destination_start_row, destination_end_row, col.index)
        results.append(RowCopyResult(col, start_row, end_row, destination_start_row, destination_end_row))

        for paste_type in ("PASTE_FORMULA", "PASTE_FORMAT"):
            requests.append(
                {
                    "copyPaste": {
                        "source": source,
                        "destination": destination,
                        "pasteType": paste_type,
                        "pasteOrientation": "NORMAL",
                    }
                }
            )

    if requests:
        spreadsheet.batch_update({"requests": requests})

    return results


def hard_copy_values(
    spreadsheet,
    sheet,
    columns: Iterable[ColumnRef],
    start_row: int,
    explicit_end_row: int | None = None,
) -> list[RowCopyResult]:
    """
    Copy each selected column range's calculated values and formatting one row down.

    Each column gets its own end row. For example, with the default
    ``--start-row 4``, column B ending at row 100, and column D ending at row
    80, values are read from B4:B100 and D4:D80 and written to B5:B101 and
    D5:D81. Source formatting is also copied to B5:B101 and D5:D81, so the
    destination format matches the source format while formulas are replaced by
    hard-coded calculated values.
    """
    format_requests = []
    updates = []
    results: list[RowCopyResult] = []

    for col in columns:
        end_row = get_column_end_row(sheet, col, explicit_end_row)
        if end_row < start_row:
            continue

        destination_start_row = start_row + 1
        destination_end_row = end_row + 1
        source_range = f"{col.letter}{start_row}:{col.letter}{end_row}"
        destination_range = f"{col.letter}{destination_start_row}:{col.letter}{destination_end_row}"
        values = sheet.get(source_range, value_render_option="UNFORMATTED_VALUE")
        expected_rows = end_row - start_row + 1
        padded_values = [row[:1] if row else [""] for row in values]
        padded_values.extend([[""] for _ in range(expected_rows - len(padded_values))])
        updates.append({"range": destination_range, "values": padded_values})
        format_requests.append(
            {
                "copyPaste": {
                    "source": make_grid_range(sheet.id, start_row, end_row, col.index),
                    "destination": make_grid_range(sheet.id, destination_start_row, destination_end_row, col.index),
                    "pasteType": "PASTE_FORMAT",
                    "pasteOrientation": "NORMAL",
                }
            }
        )
        results.append(RowCopyResult(col, start_row, end_row, destination_start_row, destination_end_row))

    if updates:
        sheet.batch_update(updates, value_input_option="RAW")
    if format_requests:
        spreadsheet.batch_update({"requests": format_requests})

    return results


def set_cell_value(sheet, cell: str | None, value: str | None) -> None:
    """Set a single cell when both --date-cell and --date-value are provided."""
    if not cell and not value:
        return
    if not cell or value is None:
        raise ValueError("Provide both --date-cell and --date-value, or neither.")
    sheet.update(cell, [[value]], value_input_option="USER_ENTERED")


def resolve_spreadsheet_id(sheet_id_or_ref: str) -> str:
    """Resolve a raw spreadsheet ID or repository ref_sheets key to a sheet ID.

    Most repository scripts accept short keys from ref_sheets.json, such as
    ``SARAS``. Keep that same behavior here while preserving support for raw
    Google spreadsheet IDs: unknown keys are returned unchanged so gspread can
    open them directly and report any access/not-found errors.
    """
    from ref_sheets_utils import resolve_sheet_id

    try:
        return resolve_sheet_id(sheet_id_or_ref)
    except ValueError:
        return sheet_id_or_ref


def get_service_account_email() -> str:
    """Return the service-account email used for authentication."""
    from oauth2client.service_account import ServiceAccountCredentials

    from runtime_paths import get_creds_path

    credentials = ServiceAccountCredentials.from_json_keyfile_name(str(get_creds_path()), SHEETS_SCOPE)
    return credentials.service_account_email


def format_sheet_target(sheet_id_or_ref: str, spreadsheet_id: str) -> str:
    """Describe the requested sheet target for errors and logs."""
    if spreadsheet_id == sheet_id_or_ref:
        return spreadsheet_id
    return f"{spreadsheet_id} (resolved from {sheet_id_or_ref!r})"


def open_spreadsheet_or_exit(client, sheet_id_or_ref: str, spreadsheet_id: str):
    """Open a spreadsheet, or exit with an actionable access message."""
    from gspread.exceptions import SpreadsheetNotFound

    try:
        return client.open_by_key(spreadsheet_id)
    except PermissionError as exc:
        service_account_email = get_service_account_email()
        target = format_sheet_target(sheet_id_or_ref, spreadsheet_id)
        raise SystemExit(
            "Google Sheets permission denied while opening spreadsheet "
            f"{target}. Share this Google Sheet with the service account "
            f"{service_account_email!r} as an editor, then rerun the command. "
            "If it is already shared, verify that the workflow is using the "
            "same creds.json/service account you shared with."
        ) from exc
    except SpreadsheetNotFound as exc:
        target = format_sheet_target(sheet_id_or_ref, spreadsheet_id)
        raise SystemExit(
            "Google spreadsheet not found while opening "
            f"{target}. Check that --sheet-id is a valid raw spreadsheet ID "
            "or a ref_sheets.json key, and that the authenticated service "
            "account has access to it."
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fill formulas, hard-copy values, and set a date cell in a Google Sheet tab.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Notes:
              - Argument order does not matter. argparse reads options by name.
              - --sheet-id and --tab are required for sheet operations.
              - --sheet-id accepts either a raw Google spreadsheet ID/key or a
                ref_sheets key from ref_sheets.json, such as SARAS.
              - Formula copy, hard copy, and date update are independent. Provide only
                the operation arguments you need.
              - Multiple columns can be sent in one run as comma-separated values,
                for example A,C,F. You can also repeat the same column option.
              - Row operations are applied independently per selected column:
                for each column, copy from --start-row through that column's
                last non-empty row and paste one row down. If --end-row is
                supplied, it overrides per-column detection for every column.
              - Formula copy preserves formatting by copying formulas first and
                then copying the source row format. Hard copy writes calculated
                values first and then copies the source format to the destination.

            Examples:
              Formula copy only, with multiple columns in one argument:
                python google_sheet_tab_ops.py --sheet-id SHEET_ID --tab TAB --formula-columns A,C,F

              Formula copy only, repeating the same option also works:
                python google_sheet_tab_ops.py --sheet-id SHEET_ID --tab TAB --formula-columns A --formula-columns C,F

              Hard copy only, with multiple columns in one argument:
                python google_sheet_tab_ops.py --sheet-id SHEET_ID --tab TAB --hard-copy-columns H,I,J

              Date cell only:
                python google_sheet_tab_ops.py --sheet-id SHEET_ID --tab TAB --date-cell B2 --date-value 2026-06-05

              Formula copy + hard copy + date update, in any argument order:
                python google_sheet_tab_ops.py --date-value 2026-06-05 --hard-copy-columns H,I --tab TAB --date-cell B2 --sheet-id SHEET_ID --formula-columns A,C
            """
        ),
    )
    parser.add_argument(
        "--sheet-id",
        help=(
            "Google spreadsheet ID/key or ref_sheets key (for example SARAS). "
            "Required unless using --print-service-account."
        ),
    )
    parser.add_argument("--tab", help="Worksheet/tab name inside the spreadsheet. Required unless using --print-service-account.")
    parser.add_argument(
        "--formula-columns",
        action="append",
        default=[],
        help=(
            "Comma-separated A1 columns or header names to fill formulas and formatting down. "
            "Can be repeated, e.g. --formula-columns A,C --formula-columns 'Net Total'."
        ),
    )
    parser.add_argument(
        "--hard-copy-columns",
        action="append",
        default=[],
        help=(
            "Comma-separated A1 columns or header names to replace with current values and source formatting. "
            "Can be repeated, e.g. --hard-copy-columns H,I --hard-copy-columns J."
        ),
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=4,
        help="First source row to copy in each selected column. Defaults to 4; destination starts at the next row.",
    )
    parser.add_argument(
        "--end-row",
        type=int,
        default=None,
        help=(
            "Optional last source row to copy. When omitted, the last source row is detected separately "
            "for each selected column; destination ends at that column's next row."
        ),
    )
    parser.add_argument(
        "--header-row",
        type=int,
        default=1,
        help="Header row used when columns are supplied by name. Defaults to 1.",
    )
    parser.add_argument("--date-cell", help="A1 cell to update at the end, e.g. B2.")
    parser.add_argument(
        "--date-value",
        help="Date/value to put in --date-cell. USER_ENTERED is used, so dates like 2026-06-05 are parsed by Sheets.",
    )
    parser.add_argument(
        "--print-service-account",
        action="store_true",
        help="Print the service-account email that must have access to the Sheet, then exit.",
    )
    return parser


def parse_args() -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(args, parser)
    return args


def validate_args(args: argparse.Namespace, parser: argparse.ArgumentParser | None = None) -> None:
    """Validate cross-option requirements that argparse cannot express directly."""
    if args.start_row < 1:
        message = "--start-row must be >= 1."
        if parser:
            parser.error(message)
        raise ValueError(message)
    if args.header_row < 1:
        message = "--header-row must be >= 1."
        if parser:
            parser.error(message)
        raise ValueError(message)

    if args.print_service_account:
        return

    if not args.sheet_id or not args.tab:
        message = "--sheet-id and --tab are required unless using --print-service-account."
        if parser:
            parser.error(message)
        raise ValueError(message)

    formula_columns = split_csv(args.formula_columns)
    hard_copy_columns = split_csv(args.hard_copy_columns)
    has_date_update = args.date_cell or args.date_value is not None

    if not formula_columns and not hard_copy_columns and not has_date_update:
        message = "Nothing to do. Provide --formula-columns, --hard-copy-columns, and/or --date-cell/--date-value."
        if parser:
            parser.error(message)
        raise ValueError(message)

    if (args.date_cell and args.date_value is None) or (not args.date_cell and args.date_value is not None):
        message = "Provide both --date-cell and --date-value for a date update, or neither."
        if parser:
            parser.error(message)
        raise ValueError(message)


def main() -> None:
    args = parse_args()

    if args.print_service_account:
        print(get_service_account_email())
        return

    formula_columns = split_csv(args.formula_columns)
    hard_copy_columns = split_csv(args.hard_copy_columns)
    needs_row_operation = bool(formula_columns or hard_copy_columns)

    from google_sheets_utils import get_gsheet_client

    client = get_gsheet_client()
    spreadsheet_id = resolve_spreadsheet_id(args.sheet_id)
    if spreadsheet_id != args.sheet_id:
        print(f"Resolved --sheet-id {args.sheet_id!r} to spreadsheet ID from ref_sheets.json.")
    spreadsheet = open_spreadsheet_or_exit(client, args.sheet_id, spreadsheet_id)
    sheet = spreadsheet.worksheet(args.tab)

    if needs_row_operation:
        formula_refs = resolve_columns(sheet, formula_columns, args.header_row)
        hard_copy_refs = resolve_columns(sheet, hard_copy_columns, args.header_row)

        formula_results = fill_formulas_down(spreadsheet, sheet, formula_refs, args.start_row, args.end_row)
        hard_copy_results = hard_copy_values(spreadsheet, sheet, hard_copy_refs, args.start_row, args.end_row)

        for result in formula_results:
            print(
                "Formula and formatting copy completed for column "
                f"{result.column.letter} from rows {result.source_start_row}:{result.source_end_row} "
                f"to rows {result.destination_start_row}:{result.destination_end_row}."
            )
        for result in hard_copy_results:
            print(
                "Hard copy and formatting copy completed for column "
                f"{result.column.letter} from rows {result.source_start_row}:{result.source_end_row} "
                f"to rows {result.destination_start_row}:{result.destination_end_row}."
            )

        skipped_columns = [
            col.letter
            for col in [*formula_refs, *hard_copy_refs]
            if col.letter not in {result.column.letter for result in [*formula_results, *hard_copy_results]}
        ]
        if skipped_columns:
            print(
                "No row copy needed for columns "
                f"{', '.join(skipped_columns)} because their detected end rows are before start row {args.start_row}."
            )

    set_cell_value(sheet, args.date_cell, args.date_value)
    if args.date_cell:
        print(f"Set {args.date_cell} to {args.date_value}.")


if __name__ == "__main__":
    main()
