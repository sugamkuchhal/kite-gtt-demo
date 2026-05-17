# Script Lifecycle Standard — Phase B Inventory Matrix

This matrix maps current lifecycle posture vs. target posture for runnable action scripts listed in `requirements/action_scripts.json`.

## Classification legend
- **G1 (Aligned Base):** Uses `log_start/log_end` + `atexit.register` + main guard.
- **G2 (Partial):** Has some lifecycle components, but missing one or more core pieces.
- **G3 (Legacy):** Minimal lifecycle scaffolding; highest standardization need.

## Target posture (for all groups)
- Main guard present.
- Deterministic lifecycle wrapper (`parse_args` if applicable, run wrapper, controlled exits).
- Standardized start/end logging.
- Consistent exception boundary and exit-code handling.
- Shutdown delay handled as explicit lifecycle concern (preserve existing values only).

## Inventory matrix

| Script | Current group | Current posture (high-level) | Shutdown delay state | Phase-C migration risk |
|---|---|---|---|---|
| `auto_login.py` | G3 | main guard; no standardized lifecycle logger wrapper | none | Medium |
| `set_field_false.py` | G1 | log_start/log_end + atexit + main guard | none | Low |
| `fetch_all_gtts.py` | G1 | log_start/log_end + atexit + main guard | none | Low |
| `fetch_all_orders.py` | G1 | log_start/log_end + atexit + main guard | none | Low |
| `gtt_processor.py` | G1 | log_start/log_end + atexit + argparse + main guard | has 60s end delay | Medium |
| `zerodha_tick_size.py` | G2 | lifecycle logging exists; no canonical main guard entrypoint | none | Medium |
| `nse_combined_fetcher.py` | G1 | log_start/log_end + atexit + argparse + main guard | has 60s end delay | Medium |
| `nse_data_etl.py` | G1 | log_start/log_end + atexit + main guard | none | Medium |
| `prepare_feed_date_ext.py` | G2 | lifecycle logging exists; no canonical main guard entrypoint | none | Medium |
| `prepare_feed_data_val.py` | G2 | lifecycle logging exists; no canonical main guard entrypoint | none | Medium |
| `prepare_feed_list.py` | G1 | log_start/log_end + atexit + argparse + main guard | has 60s end delay | Medium |
| `append_new_orders.py` | G1 | log_start/log_end + atexit + main guard | none | Low |
| `fifo_portfolio.py` | G2 | lifecycle logging exists; no canonical main guard entrypoint | none | Medium |
| `fetch_holdings.py` | G1 | log_start/log_end + atexit + main guard | none | Low |
| `date_ext.py` | G2 | lifecycle logging partial; no atexit wrapper | none | Medium |
| `data_val.py` | G2 | lifecycle logging partial; no atexit wrapper | none | Medium |
| `ops_sort.py` | G1 | log_start/log_end + atexit + argparse + main guard | has 60s end delay | Low |
| `ops_sort_kwk.py` | G1 | log_start/log_end + atexit + argparse + main guard | has 60s end delay | Low |
| `ops_sort_sip_reg.py` | G1 | log_start/log_end + atexit + argparse + main guard | has 60s end delay | Low |
| `is_trigger_true.py` | G3 | main guard; no standardized lifecycle logger wrapper | none | Medium |
| `algo_tickers_mailer.py` | G1 | log_start/log_end + atexit + argparse + main guard | none | Low |

## Proposed Phase-C batch plan

### Batch C1 — Low-risk alignment (start here)
- `set_field_false.py`
- `fetch_all_gtts.py`
- `fetch_all_orders.py`
- `append_new_orders.py`
- `fetch_holdings.py`
- `algo_tickers_mailer.py`
- `ops_sort.py`
- `ops_sort_kwk.py`
- `ops_sort_sip_reg.py`

**Goal:** Normalize wrapper/exit handling while preserving current outputs and existing delay behavior.

### Batch C2 — Medium-risk structural alignment
- `gtt_processor.py`
- `nse_combined_fetcher.py`
- `prepare_feed_list.py`
- `nse_data_etl.py`

**Goal:** Keep behavior identical while isolating lifecycle wrapper from large business flow.

### Batch C3 — Entry-point normalization (non-main-guard scripts)
- `zerodha_tick_size.py`
- `prepare_feed_date_ext.py`
- `prepare_feed_data_val.py`
- `fifo_portfolio.py`
- `date_ext.py`
- `data_val.py`

**Goal:** Introduce canonical `main()` + guard shape without changing run semantics.

### Batch C4 — Legacy uplift
- `auto_login.py`
- `is_trigger_true.py`

**Goal:** Add full lifecycle scaffolding (logging + exception boundary + consistent exits).

## Acceptance criteria for Phase B completion
1. Inventory exists and is committed.
2. Every action script is classified (G1/G2/G3) with risk.
3. A migration batch plan is defined before code refactor begins.
4. No runtime/business behavior changed in this phase.
