# Script Lifecycle Standard (Phase A)

## Purpose
This document defines a consistent startup/shutdown scaffolding standard for repository scripts **without changing functional behavior**.

## Scope
- Applies to runnable scripts invoked directly or via `bin/*` wrappers.
- Focuses only on lifecycle concerns: startup, logging, exception boundaries, shutdown, and exit semantics.
- Excludes business logic changes (Sheets ranges, API call behavior, sorting/filtering rules, strategy decisions, etc.).

## Non-Goals
- No changes to data transformations, matching logic, order placement logic, or side-effect ordering.
- No quota/retry algorithm redesign in this phase.
- No mandatory CLI flag changes in this phase.

---

## Lifecycle Contract
Each script should converge to this shape:

1. `parse_args()`
2. `main()` lifecycle wrapper
3. `run(args)` containing current business flow
4. deterministic shutdown logging
5. optional configured shutdown delay
6. controlled process exit code

### Canonical sequence
1. Initialize run context (script name + start timestamp).
2. Parse/validate arguments.
3. Execute business flow.
4. Emit completion status + duration.
5. Apply configured shutdown delay (if non-zero for that script).
6. Exit with standard code.

---

## Logging Standard

### Start log (required)
- script name
- run start timestamp
- execution mode / key args (only non-sensitive)

### End log (required)
- script name
- final status (`success` / `failed`)
- elapsed duration (seconds)
- optional summary counts

### Error log (required)
- concise error message
- exception type
- stack trace for unexpected errors

### Output channel policy
- Lifecycle and errors should use logger.
- Human-readable summaries may use `print` if currently required.
- Do not duplicate the same message in both `print` and logger.

---

## Exception Boundary Standard
At top-level wrapper:

- `KeyboardInterrupt` -> graceful warning + non-zero exit.
- expected operational failures (auth/network/API/quota/input) -> error log + stable non-zero exit.
- unexpected exceptions -> error log + traceback + generic non-zero exit.

Business-level exception handling inside `run(args)` should remain unchanged during standardization unless strictly needed for wrapper alignment.

---

## Shutdown Delay Standard
Some scripts currently have end delays (for example `time.sleep(60)`).

Standard rules:
- delay is a script-level lifecycle setting (default `0` unless script already has a delay).
- delay is applied in one consistent location in the lifecycle wrapper.
- delay must be logged (start/end of wait).
- preserve existing per-script delay values in migration unless explicitly changed.

---

## Exit Code Standard
Target matrix for top-level scripts:

- `0`: success
- `2`: argument/config validation error
- `3`: external dependency/runtime operation error (APIs, Sheets, network)
- `1`: unexpected internal error

If current behavior differs, migration should stage adoption carefully and document script-specific exceptions.

---

## Structural Layout Standard
Recommended order inside each script:

1. imports
2. constants/config
3. helper functions
4. `parse_args()`
5. `run(args)` (business flow)
6. `main()` (lifecycle wrapper)
7. `if __name__ == "__main__":` block

No business logic should be modified while reordering/extracting lifecycle scaffolding.

---

## Compatibility & Safety Guardrails
- Preserve existing CLI functional flags and defaults.
- Preserve side-effect order (read/write/update/clear/sort actions).
- Preserve current waits/retries unless explicitly moving equivalent behavior into wrapper.
- Validate no functional drift using existing commands and log comparisons.

---

## Phase-A Deliverables
1. This lifecycle standard specification.
2. Script inventory matrix (next phase) mapping current vs target lifecycle pattern.
3. Migration batches with risk labels (low/medium/high) before any code movement.

