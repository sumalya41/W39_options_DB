# Databento Options Research Project

A bounded, causal research project for a simple Databento MBO options strategy on the raw Euro FX options feed. The project follows a frozen session-split workflow, keeps raw data immutable, and uses return-based logic throughout. This repository is intentionally structured to reject a weak signal rather than to force a strategy into the test split.

## Data-source clarification

The raw parquet (`data/databento_options_clean.parquet`) and the notebook-based schema check in `databento-options (1).ipynb` confirm the same MBO footprint: `ts_recv`, `ts_event`, `ts_index`, `price`, `rtype`, `publisher_id`, `instrument_id`, `action`, `side`, `size`, `channel_id`, `order_id`, `flags`, `ts_in_delta`, `sequence`, `symbol`, and `session_date`. The feed is a Databento MBO options dataset (Euro FX options / futures) rather than a ready-made BBO feed; the book must be reconstructed from order events.

## Project goal

Evaluate whether a simple near-the-money option signal based on short-horizon underlying return continuation has a valid causal edge. The final decision was to reject the strategy after development and validation evidence, leaving the test split untouched.

## Raw data and policy

- Raw dataset: `data/databento_options_clean.parquet`
- Raw data are read-only and never modified in place.
- Derived outputs are written under `data/derived/` and `outputs/` only.
- The project uses a frozen configuration in `config/config.yaml` and operates by whole-session splits.

## Run order

1. `scripts/phase01_inventory.py`
   - Inventory, schema checks, symbol parsing, null counts.
2. `scripts/phase02_contracts.py`
   - Contract identity, option/future mapping hypothesis, coverage table.
3. `scripts/phase03_cleaning_audit.py`
   - Cleaning-log audit for partial sessions, reversals, data issues.
4. `scripts/phase04_bbo.py`
   - Replay MBO stream into top-of-book output.
5. `scripts/phase05_research.py`
   - Causal decision-grid construction and research table.
6. `scripts/phase06_eda.py`
   - EDA summary, spread distribution, entry-pass analysis.
7. `scripts/phase07_hypothesis.py`
   - Freeze and evaluate development hypothesis.
8. `scripts/phase08_backtester.py`
   - Minimal strategy backtest on development + validation.
9. `scripts/phase09_validation.py`
   - Validation, benchmark, robustness, and final decision.
10. `scripts/phase10_verification.py`
   - Causality and ledger verification checks.

## Configuration notes

The canonical config is `config/config.yaml`.

Key items:
- `exclude_sessions`: excludes the partial/holiday session `2026-04-06`.
- `splits.development`: 2026-03-09 through 2026-03-25.
- `splits.validation`: 2026-03-26 through 2026-03-31.
- `splits.test`: 2026-04-01 through 2026-04-08.
- `decision_grid_minutes`: 1
- `signal.lookback_minutes`: 5
- `max_abs_log_moneyness`: 0.02
- `max_rel_spread`: 0.05
- `quote_max_age_seconds`: 5
- `underlying_max_age_seconds`: 5
- `holding_minutes`: 15
- `initial_cash`: 100000
- `seed`: 42

The development threshold is frozen from the development-return distribution and is not hard-coded in the final backtest logic.

## Dependency versions

Collected from the project environment via `pip freeze`:

- databento==0.87.0
- databento-dbn==0.70.0
- duckdb==1.5.6
- exchange_calendars==4.13.2
- hypothesis==6.168.3
- matplotlib==3.11.2
- numba==0.68.0
- numpy==2.5.3
- pandas==3.0.6
- polars==1.44.2
- pyarrow==25.0.1
- pydantic==2.13.5
- PyYAML==6.0.3
- scipy==1.18.1
- seaborn==0.13.2
- statsmodels==0.15.0
- pytest==9.1.1
- tqdm==4.70.1
- psutil==7.2.2
- py-vollib==1.0.12

## Seeds and determinism

- Seed: `42`
- The frozen config and all phase scripts use deterministic logic without hidden mutable state.
- Any development-only threshold or test gate must be fixed before validation/test windows are touched.

## AI-use statement

This project used AI assistance as a coding and reasoning aid during implementation, but every analytical decision and numerical conclusion was checked against generated project artifacts and the rulebook in `todo.md` and `prompt/eurex_options_strategy_playbook(1).md`. The final report uses only values originating from the generated facts, figures, and ledger outputs in the project. No numerical claim in the report was invented beyond those generated artifacts.

## Validation status

Targeted verification passed:

- `pytest tests/test_phase10_verification.py -q` → 6 passed

Project outcome:

- The Phase 7 signal was rejected as non-surviving on development data.
- The Phase 8 simple backtest remained negative on development + validation data.
- The final decision is to reject the strategy and keep the test split unopened.
