# Script Lifecycle Standard — Phase C Execution Plan

This document translates Phase B batches into a concrete, no-functional-change implementation sequence.

## Phase-C principles
- No business logic changes.
- No data-shape/range/API behavior changes.
- Preserve existing sleeps/retries/ordering unless moved to equivalent lifecycle wrapper location.
- One script-family at a time with rollback-ready commits.

## Global implementation checklist (applies to every script in every batch)
1. Introduce/normalize `main()` lifecycle wrapper.
2. Keep existing business flow in `run(args)` (or equivalent function) unchanged.
3. Standardize start/end logging using existing repo logger conventions.
4. Add top-level exception boundary (expected vs unexpected) with stable exit codes.
5. Preserve existing shutdown delay semantics (value and effective timing).
6. Keep existing CLI flags/defaults untouched.
7. Validate with syntax check + script smoke command.

---

## Batch C1 — Low-risk alignment

### Scripts
- `set_field_false.py`
- `fetch_all_gtts.py`
- `fetch_all_orders.py`
- `append_new_orders.py`
- `fetch_holdings.py`
- `algo_tickers_mailer.py`
- `ops_sort.py`
- `ops_sort_kwk.py`
- `ops_sort_sip_reg.py`

### Work items
- Normalize wrapper shape and consistent exit handling.
- Ensure existing per-script sleep behavior remains exactly equivalent.
- Keep all existing argument signatures and logs compatible.

### Verification
- `python -m py_compile` on all C1 scripts.
- `--help` smoke for argparse-based scripts.
- quick import/smoke for non-argparse scripts.

### Risk
- **Low** (already close to target posture).

---

## Batch C2 — Medium-risk structural alignment

### Scripts
- `gtt_processor.py`
- `nse_combined_fetcher.py`
- `prepare_feed_list.py`
- `nse_data_etl.py`

### Work items
- Isolate lifecycle wrapper from heavy business functions.
- Keep large function internals untouched.
- Preserve existing terminal summaries and post-check logging behavior.

### Verification
- syntax check + `--help` where available.
- module import smoke for large scripts.
- compare before/after key lifecycle log lines and delays.

### Risk
- **Medium** (larger files, multiple responsibilities).

---

## Batch C3 — Entry-point normalization

### Scripts
- `zerodha_tick_size.py`
- `prepare_feed_date_ext.py`
- `prepare_feed_data_val.py`
- `fifo_portfolio.py`
- `date_ext.py`
- `data_val.py`

### Work items
- Introduce canonical `main()` + `if __name__ == "__main__"` entrypoint where missing.
- Keep existing execution semantics and side-effect order.
- Preserve current module-level constants and config resolution.

### Verification
- syntax check for all scripts.
- command smoke using current invocation patterns.

### Risk
- **Medium** (entrypoint changes can affect invocation behavior if not careful).

---

## Batch C4 — Legacy uplift

### Scripts
- `auto_login.py`
- `is_trigger_true.py`

### Work items
- Add full lifecycle scaffolding (start/end logging, exception boundary, controlled exits).
- Keep business calls/order unchanged.

### Verification
- syntax check + direct smoke invocation.
- ensure no CLI contract regressions.

### Risk
- **Medium** (lowest scaffolding baseline today).

---

## Cross-batch validation strategy

### Required checks per PR
1. Syntax check (`python -m py_compile ...`).
2. Smoke command(s) for changed scripts.
3. Git diff audit to confirm no business logic edits.
4. Log-shape review for standardized lifecycle start/end lines.

### Regression guardrails
- Do not alter sheet ranges or worksheet names.
- Do not alter API endpoints or request payload fields.
- Do not alter retry counters/backoff formulas.
- Do not alter post-check cell addresses.

---

## PR slicing recommendation
- One PR per batch (`C1`/`C2`/`C3`/`C4`) to keep review focused.
- If needed, split C2 into two PRs due to script size (`gtt_processor` separate).

---

## Exit criteria for Phase C completion
1. All action scripts conform to lifecycle wrapper standard.
2. Startup/shutdown logging and exception boundaries are consistent.
3. Existing wait semantics are preserved and documented.
4. No functional behavior changes observed in smoke/regression checks.
