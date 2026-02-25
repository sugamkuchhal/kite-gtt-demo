# Google Sheets Refactor (Simple English)

This note explains what was done in the previous Google Sheets refactor PR in plain language.

## What changed

1. A shared helper file (`google_sheets_utils.py`) was made the main place for Google Sheets access.
2. Many scripts were updated to use this helper instead of each script doing its own auth logic.
3. Common helper functions were introduced/used:
   - `get_gsheet_client(...)`
   - `open_spreadsheet(...)`
   - `open_worksheet(...)`
   - `extract_spreadsheet_id(...)`
4. Retry + throttling logic was centralized in the helper to reduce transient Sheets errors (429/5xx).
5. One script that used direct Google API client (`set_field_false.py`) was moved to the same helper approach.

## Why this was done

- Reduce duplication: auth/open code was repeated across many scripts.
- Improve consistency: one standard way to call Google Sheets.
- Improve reliability: centralized retries/throttling instead of ad-hoc handling.

## What did NOT change

- Business workflows and domain logic were intended to remain the same.
- The refactor focused mainly on how scripts connect to Sheets, not what they compute.

## Things to watch after such refactors

- Scope differences (`readonly` vs read/write) in scripts that only read.
- Scripts that depended on specific open mode (`open`, `open_by_key`, `open_by_url`).
- Any script with local retry wrappers that may overlap with helper retries.

## Short summary

Before: many scripts had many different Google Sheets connection patterns.

After: scripts mostly use one common utility module for auth/open/retry/throttle.
