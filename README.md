# Kite + Google Sheets Automation (Scripts)

This repo is a collection of Python scripts used to:
- interact with Zerodha Kite (GTTs, orders, holdings)
- read/write Google Sheets (instructions, data, feeds)
- run scheduled chains via GitHub Actions

## Local setup

### 1) Create a virtualenv and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2) Provide required secret files (repo root)

These files are intentionally ignored by git (see `.gitignore`):

- `api_key.txt`
  - line 1: Kite `API_KEY`
  - line 2: Kite `API_SECRET` (only required by `auto_login.py` and flows that refresh tokens)
- `access_token.txt`
  - Kite access token (single line)
- `creds.json`
  - Google service account credentials JSON for Sheets access
- `smtp_token.json`
  - (If used) SMTP token/config JSON for email flows

### 3) Run scripts

Examples:

```bash
python3 preflight.py
python3 fetch_all_gtts.py
python3 gtt_processor.py --sheet-id "<sheet-id>" --sheet-name "<tab-name>"
```

## GitHub Actions

- `.github/workflows/unified.yml` runs the main scheduled chains (midday vs EOD).
- `.github/workflows/refresh-token.yml` refreshes the Kite access token and updates the `ACCESS_TOKEN` secret.
- `.github/workflows/quality.yml` is a lightweight check (syntax/undefined names) intended to fail fast without adding runtime dependencies or changing behavior.

## Suggested next improvements (non-breaking)

- Consolidate credential loading (files + env vars) into a shared utility used by all scripts.
- Replace ad-hoc `print()` with structured logging (consistent formatting + levels).
- Add a small unit-test suite around pure functions (date handling, parsing, sheet range building).
- Gradually refactor scripts into a package (`src/` layout) while keeping CLI entrypoints.
