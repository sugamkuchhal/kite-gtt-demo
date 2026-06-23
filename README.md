# kite-gtt-demo

Automated GTT (Good Till Triggered) order management for Zerodha Kite, driven by a Google Sheets instruction sheet. Runs on a scheduled GitHub Actions workflow during Indian market hours.

## How it works

1. You maintain a Google Sheet (named `PORTFOLIO`) with two tabs:
   - **GTT_INSTRUCTIONS** — where you specify what orders to place, update, or delete
   - **GTT_DATA** — tracks live GTT IDs returned by the Kite API

2. The main script (`gtt_processor.py`) reads instructions from the sheet, talks to the Kite API to place/update/delete GTTs, and writes status emojis (✅ / ❌ / ⚠️) back to the sheet.

3. A GitHub Actions workflow (`unified_v2.yml`) runs the scripts automatically on a schedule aligned with NSE market hours (IST), or can be triggered manually.

## Repository structure

| File / folder | Purpose |
|---|---|
| `gtt_processor.py` | Core GTT processing logic — place, update, delete |
| `config.py` | Sheet names, column mappings, batch size |
| `kite_session.py` | Zerodha Kite session management (token refresh) |
| `auto_login.py` | Headless login to refresh the Kite access token |
| `google_sheets_utils.py` | Google Sheets client helpers |
| `google_sheet_tab_ops.py` | Sheet tab maintenance (copy rows, update dates) |
| `fetch_google_gtt_instructions.py` | Read instruction rows from Google Sheets |
| `fetch_google_existing_gtts.py` | Read existing GTT tracking data from Google Sheets |
| `nse_combined_fetcher.py` | Fetch NSE instrument/tick-size data |
| `zerodha_tick_size.py` | Resolve tick sizes for instruments |
| `fifo_portfolio.py` | FIFO portfolio P&L calculations |
| `data_teleporter.py` | Move data between sheets |
| `ops_sort.py` | Sort sheet rows |
| `runtime_paths.py` | Resolve file paths for credentials (local + CI) |
| `script_logger.py` | Consistent start/end logging with IST timestamps |
| `ref_sheets.json` | Map of logical sheet names → Google Spreadsheet IDs |
| `holidays.txt` | NSE trading holiday dates (DD-MM-YYYY, one per line) |
| `requirements/full.txt` | All Python dependencies |
| `.github/workflows/unified_v2.yml` | CI/CD schedule and manual trigger |

## Setup

### 1. Credentials

Three files are needed (never commit these):

| File | Contents |
|---|---|
| `api_key.txt` | Line 1: Kite API key. Line 2: Kite API secret |
| `access_token.txt` | Current Kite access token (refreshed daily by `auto_login.py`) |
| `creds.json` | Google service account JSON key |

In CI, set these via environment variables or GitHub Secrets and point to them with:

```
SECRETS_DIR=/path/to/secrets   # directory containing all three files
# or individually:
API_KEY_PATH=...
ACCESS_TOKEN_PATH=...
CREDS_JSON_PATH=...
```

### 2. Google Sheets

- Create a spreadsheet and note its ID.
- Add the ID to `ref_sheets.json` under the key `"PORTFOLIO"`.
- Share the sheet with your service account email (from `creds.json`).
- Create tabs named `GTT_INSTRUCTIONS` and `GTT_DATA` with the columns defined in `config.py`.

### 3. Python dependencies

```bash
pip install -r requirements/full.txt
```

### 4. Run manually

```bash
python gtt_processor.py
```

## GTT_INSTRUCTIONS sheet columns

| Column | Description |
|---|---|
| `TICKER` | NSE trading symbol (e.g. `RELIANCE`) |
| `TYPE` | `BUY` or `SELL` |
| `UNITS` | Quantity |
| `GTT PRICE` | Trigger price |
| `GTT DATE` | Date the instruction was set |
| `ACTION` | `PLACE`, `UPDATE`, or `DELETE` |
| `METHOD` | Tag/label for the order |
| `STATUS` | Written back by the script (✅ placed / ❌ error / etc.) |
| `LIVE PRICE` | Last traded price (used for GTT condition) |
| `TICK SIZE` | Minimum price increment for the instrument |

## GitHub Actions

The workflow in `.github/workflows/unified_v2.yml` supports three modes:

- **`eod`** — end-of-day processing
- **`midday`** — intraday refresh
- **`refresh`** — token/data refresh

You can also trigger a specific script directly via `workflow_dispatch` with the `script` input.

Market holidays listed in `holidays.txt` are respected — scheduled runs are skipped on those dates.

## Key design decisions

- **Single source of truth for batch size**: `config.BATCH_SIZE` — no magic numbers elsewhere.
- **Exponential backoff with jitter**: all Kite API calls go through `safe_api_call()`, which retries on transient errors (429, 5xx, timeouts, connection resets).
- **Batched Sheet writes**: status updates are queued and flushed in one `batch_update` call to minimise API quota usage.
- **Idempotent processing**: duplicate GTTs are detected before placing; "no update needed" is logged when the existing GTT already matches.
