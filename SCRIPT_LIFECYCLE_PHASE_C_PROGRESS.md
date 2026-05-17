# Script Lifecycle Standard — Phase C Progress

## Completed batches and scripts

### C1 completed
- set_field_false.py
- fetch_all_gtts.py
- fetch_all_orders.py
- append_new_orders.py
- fetch_holdings.py
- algo_tickers_mailer.py
- ops_sort.py
- ops_sort_kwk.py
- ops_sort_sip_reg.py

### C2 completed
- gtt_processor.py
- nse_combined_fetcher.py
- prepare_feed_list.py

### C3 completed (partially)
- prepare_feed_date_ext.py
- prepare_feed_data_val.py
- date_ext.py
- data_val.py

### C4 completed
- auto_login.py
- is_trigger_true.py

## Remaining for full Phase C closure

### Remaining C2 target
- nse_data_etl.py

### Remaining C3 targets
- zerodha_tick_size.py ✅ completed
- fifo_portfolio.py ✅ completed

## Notes
- The remaining two C3 scripts (`zerodha_tick_size.py`, `fifo_portfolio.py`) still rely heavily on module-level execution and need controlled entrypoint normalization in a dedicated pass.
- A focused finalization PR should complete:
  1. `main()` extraction for the remaining scripts,
  2. consistent top-level exception boundary,
  3. syntax + smoke checks.


## Latest update
- Completed lifecycle entrypoint normalization for `zerodha_tick_size.py` (main wrapper + stable exit codes).
- Phase C lifecycle normalization targets are complete.
