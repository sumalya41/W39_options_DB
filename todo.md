# TODO — Options Data EDA & Simple Strategy Assignment

Data in hand: `data/databento_options_clean.parquet` (~298 MB, already decoded to parquet,
35,045,335 rows). The raw-schema audit and the Databento notebook cross-check confirm a **Databento
MBO options feed** (Euro FX options / underlying futures) with the expected `ts_recv` / `ts_event` /
`ts_index` / `action` / `side` / `order_id` / `flags` columns. The repository still uses
**`prompt/eurex_options_strategy_playbook(1).md` as the operational playbook**, but the dataset-specific
facts are now aligned to the actual feed and not to a guessed Eurex label. Key consequence: there are
no ready-made bid/ask columns — top-of-book must be **reconstructed from the per-order MBO stream**
before any spread/EDA/backtest work.

## ABSOLUTE RULES (non-negotiable, apply to every module below)

1. **Functional, modular programming — ABSOLUTE RULE.** Every unit of logic is a pure function:
   same input → same output, no hidden state, no mutation of arguments. Side effects (file I/O,
   logging, network) live only at the thin edges (`io/`, driver scripts) and are never mixed into
   analysis/decision code. No classes used for behavior/state machines — use plain functions,
   `functools.reduce`/`fold` over immutable records, `NamedTuple`/`@dataclass(frozen=True)` for
   data, and composition (small functions chained, not god-objects). Classes are allowed only as
   pure data containers (frozen) or `Protocol`/typing aids, never to hold mutable state across
   calls. Every module exposes a small set of named, independently testable functions — never a
   script that runs top-to-bottom with implicit shared state.
2. **Time-series statistics discipline.** Before computing anything on a series: check and record
   stationarity (ADF / KPSS), autocorrelation structure (ACF/PACF), and heteroskedasticity.
   Regressions use HAC/Newey-West or session-clustered standard errors, never plain OLS SEs on
   serially correlated data. Never treat an MBO/BBO stream as i.i.d. State the sampling scheme
   (event-time vs clock-time grid) for every statistic, because it changes the autocorrelation.
3. **Return-based analysis, not price levels.** All modeling, EDA regressions and signal
   construction operate on **simple returns** (`log(x_t / x_{t-1})`) of the underlying future and of
   option mid, never on raw price/premium levels, which are non-stationary and make regressions
   spurious. Price *levels* are shown only in descriptive/sanity exhibits (e.g. price-vs-strike
   cross-sections), explicitly labeled as descriptive, not as regression inputs.
4. **Marcos López de Prado principles**, applied where the data supports them:
   - **Purging** of overlapping label/outcome windows at train/validation/test boundaries, plus an
     **embargo** period after each split boundary (already in Phase 9, now implemented as a pure
     mask function over timestamps, not ad hoc slicing).
   - **Fractional differentiation** (`frac_diff`, fixed-width window) as the default way to make a
     price/return series stationary while retaining maximal memory, used as an alternative to
     plain differencing in the stats module.
   - **CUSUM filter** for event-based sampling of the underlying return series (alternative to a
     fixed clock grid), reported side by side with the clock-time grid per the playbook.
   - **Sample uniqueness / overlap weighting** noted as a known limitation when labels (holding
     windows) overlap in time, even if not fully implemented, because the dataset is small.
   - These are descriptive/robustness tools, not a replacement for the playbook's own causality,
     purge/embargo and walk-forward split rules, which remain the source of truth.
5. **Memory safety / no OOM — ABSOLUTE RULE.** The dataset is 35M+ rows; a single eager
   `.collect()`, `list(...)` over the full event/row stream, or a `to_pandas()` on an unprojected
   frame WILL kill the kernel and cannot be caught once the OS/Kaggle OOM-killer fires. Every
   function that walks the raw MBO stream must be O(1) or O(bounded-chunk) in memory, never
   O(n_rows):
   - Polars: always `scan_parquet(...).select(needed cols)` before any `.collect()`; use
     `collect(engine="streaming")`/`sink_parquet`; never call an eager op on an unprojected scan.
   - Per-session / per-contract processing in a loop, not one pass over the whole file.
   - The functional MBO→BBO replay (`mbo.book.replay_stream`) is a **generator**: it keeps only
     the current `BookState` and the previously emitted `BBORow`, never a list of all events or
     all intermediate states. `mbo.book.replay` (returns a `list`) is for small/test inputs only —
     production code must consume `replay_stream` and sink output incrementally.
   - Call `memguard.check(tag)` after every chunk/session in any loop over the raw data; it reads
     the cgroup/container memory limit (not just host RAM) and raises `MemoryBudgetExceeded`
     before an OOM kill would occur, so a crash is visible and resumable instead of silent.
   - Never add a convenience function that returns "everything" as one in-memory object for the
     full dataset — bounded, chunked, or streaming by construction, not by caller discipline.
6. **Raw data immutability — ABSOLUTE RULE.** `data/databento_options_clean.parquet` is never
   opened for writing, overwritten, or modified in place. Any cleaning, new/derived column
   (parsed symbol fields, `contract_key`, log returns, BBO reconstruction, research-table
   features, ...) is written to a **new parquet file** under `data/derived/<step_name>.parquet`
   (or session-partitioned `data/derived/<step_name>/session_date=YYYY-MM-DD/*.parquet` for large
   outputs), produced by `scan(raw) -> transform -> sink(new_path)`. The raw file stays a
   byte-for-byte, re-runnable source of truth; every derived file states which raw file + which
   function version it was built from.

## Architecture

```
options/
├── config/
│   └── config.yaml                 # frozen run config (splits, costs, thresholds, seed)
├── data/
│   ├── databento_options_clean.parquet   # raw, read-only, never modified (Rule 6)
│   └── derived/                    # every cleaned/derived/new-column dataset: a new parquet
│       └── <step_name>.parquet     #   (or session_date=.../ partitions), never overwrites raw
├── src/
│   └── options_research/           # pure-function package, no mutable module state
│       ├── __init__.py
│       ├── config.py               # load_config(path) -> frozen Config (pydantic, immutable)
│       ├── memguard.py             # pure budget/limit functions + check() boundary guard
│       ├── io/
│       │   ├── lazyio.py           # scan_parquet/project/collect_stream — all pure, lazy-in/out
│       │   └── inventory.py        # describe_schema, sample_rows, null_counts (read-only)
│       ├── symbols.py              # parse_symbol, build_contract_key — pure string -> record
│       ├── contracts.py            # build_symbology_map, coverage_table — DataFrame -> DataFrame
│       ├── mbo/
│       │   └── book.py             # replay_stream: O(1)-memory generator, reduce(events) -> BBO rows
│       ├── stats/
│       │   ├── timeseries.py       # log_returns, adf_test, acf, hac_ols — pure, return-based
│       │   └── lopez_de_prado.py   # frac_diff, cusum_filter, purge_embargo_mask — pure
│       ├── research/
│       │   ├── returns.py          # return-based feature construction (never raw price)
│       │   └── grid.py             # build_decision_grid, asof_join_backward — pure, causal
│       ├── eda/
│       │   └── exhibits.py         # aggregate -> small DataFrame (plotting is a thin edge)
│       └── strategy/
│           ├── signals.py          # signal(returns, threshold) -> +1/0/-1, pure
│           ├── select.py           # select_contract(candidates, rules) -> contract_key, pure
│           ├── execute.py          # simulate_fill(order, quotes) -> Fill, pure
│           └── ledger.py           # fold_trades(events) -> ledger rows, pure
├── tests/                          # pytest, one test module per pure-function module
├── scripts/                        # thin driver scripts only: parse args, call pure functions, write output
├── outputs/
│   └── phaseNN/                    # facts_stepNN.json, data_dictionary.csv, CSVs per phase
├── requirements.txt
├── report.md                       # running results log, one section per phase, numbers only
└── todo.md
```

Rule of thumb for every new file: if it has a `class` with a method that mutates `self` across
calls, or a function with no return value that isn't an I/O boundary, it violates Rule 1 — refactor
to pure functions plus an explicit fold/reduce over state.

**Status: scaffolded and implemented** — `config/config.yaml`, `pytest.ini`
(`pythonpath = src`), and the full `src/options_research/` tree above exist with working pure
functions (`config`, `memguard`, `io.lazyio`, `io.inventory`, `symbols`, `contracts`,
`mbo.book` functional order-book replay, `stats.timeseries`, `stats.lopez_de_prado`,
`research.returns`, `research.grid`, `eda.exhibits`, `strategy.signals/select/execute/ledger`)
plus `tests/` covering symbols, timeseries, López de Prado tools, MBO replay and strategy
functions — 23/23 passing. **Phase 1 has now been run** against the real
`data/databento_options_clean.parquet` via `scripts/phase01_inventory.py`, writing
`outputs/phase01/data_dictionary.csv` and `outputs/phase01/facts_step01.json` (see Data Facts
below for the real numbers). **`report.md`** is the running, human-readable results log — one
section per phase, updated as each phase completes; `todo.md` stays the task tracker/rulebook.

## Phase 0 — Environment
- [x] Create `.venv`, activate it, set PowerShell execution policy.
- [x] Write `requirements.txt` (unpinned) and install libraries.
- [ ] Verify key imports work (`polars`, `pyarrow`, `duckdb`, `pandas`, `databento`, `numba`,
      `exchange_calendars`, `statsmodels`, `py_vollib`) — drop/replace any that fail to build on
      Windows (e.g. `py_vollib`/`numba` native deps) and note it in repo memory.

## Phase 1 — Inventory & data dictionary (MBO playbook Section 1-2)
- [x] Load schema (DuckDB `DESCRIBE`) and row count — see Data Facts below. 35,045,335 rows.
- [x] Distinct `symbol` values (1,056); parsed via fixed regex (trailing variant token):
      21 futures, 985 options, 50 unknown — the 50 are real **spread/strategy symbols**
      (`FCEU.S.<exp1>.<exp2>.SPD` calendar spreads, `EUCO.O.<date>.<code>.<seq>` option
      strategies), correctly left unparsed rather than misparsed. List in
      `outputs/phase01/facts_step01.json`.
- [x] Null/sentinel counts (full streaming scan): `price` 11,249 nulls (= exactly the `R`/reset
      action count — nulls on reset events, expected), `size` 5 nulls, all other columns 0 nulls.
- [x] `ts_recv` monotonicity check (ordered by `sequence` per session/publisher): 58,925
      reversals (~0.17% of rows) — flag for the Phase 3 cleaning log, do not "fix" them.
- [x] `action`/`side` value counts: action `A`=17,515,881 `C`=17,516,289 `R`=11,249 `T`=996
      `F`=867 `M`=53 (almost no in-place modifies — book updates are add/cancel pairs); side
      `A`=17,549,004 `B`=17,484,953 `N`=11,378 (N ≈ R count, as expected on reset events).
- [x] `publisher_id`/`channel_id`/`session_date`: publisher 101 dominates (35,045,179 rows) vs
      103 (156 rows); **channel_id 23 (19,869,718) and 79 (15,175,617) = the "2 folders"** from
      the Eurex playbook; **22 distinct sessions**, 2026-03-09 → 2026-04-08, matching the
      playbook's "22 sessions" exactly (incl. 2026-04-06, the playbook's flagged partial/holiday
      day — exclude in Phase 3/5 `config.yaml`).
- [x] Data dictionary CSV written: `outputs/phase01/data_dictionary.csv`.
- [x] Route confirmation: MBO data, so bid/ask must be reconstructed (not present as columns).
- [x] Write `facts_step01.json` → `outputs/phase01/facts_step01.json`.

## Phase 2 — Contract identity & symbol parsing (MBO playbook Section 3)
- [x] Parsed `symbol` into product/expiry/right/strike on the DISTINCT
      `(session_date, publisher_id, instrument_id, symbol)` table (4,440 rows, not raw rows);
      stable `contract_key`; 50 unparsed (spread/strategy) symbols listed in
      `outputs/phase02/unparsed_symbols.csv`. Enriched map sunk to
      `data/derived/phase02_symbology_map.parquet` (never the raw file — Rule 6).
- [x] Date-aware map built and validated: **0** instrument_ids/symbols collide within the same
      day, but **1,039** `instrument_id` values are reused across different days for different
      symbols — proves `instrument_id` alone is day-local only, exactly as the playbook warns.
- [x] Coverage table (`outputs/phase02/coverage_table.csv`): product `EUCO`, **985 contracts**,
      **13 expiries**, 2,174 calls / 2,051 puts; **21** `FCEU` futures; strikes `1.09`–`1.265`.
- [x] Option→underlying-future mapping **hypothesis** built (nearest future expiring on/after
      the option expiry) in `outputs/phase02/option_to_future_mapping_hypothesis.csv` — labelled
      a hypothesis, NOT verified; actual testing against development-session return correlation
      is deferred to Phase 5+ once the session split is frozen (no split exists yet).

## Phase 3 — Decode/clean audit (MBO playbook Section 4)
- [ ] Cleaning log (`cleaning_log.csv`): nulls, negative prices (spread instruments), timestamp
      reversals, `ts_event` vs `ts_recv` gaps, flags 8/4, duplicates, sequence gaps, partial days.
- [ ] Three annotated real data-issue examples with plots + distortion explanation.

## Phase 4 — Build top-of-book (BBO) from MBO (MBO playbook Section 5)
- [x] Per-day, per-instrument order-book replay (A/C/M/R/T/F) in feed order → emit BBO rows only
      on change; separate `trades` table for T/F.
- [x] Validate book: smoke-check a few contracts, ensure BBO changes are emitted only on a real
      best-bid/best-ask change, and verify return stats on the reconstructed mid.
- [x] Expand the smoke-validated session run to all sessions for the front future (session-sized
      streaming chunks, not one monolithic full-dataset list build).
- [x] The project environment is consistent with the `numba` dependency and the MBO replay path is
      validated against the working environment; no fallback was required after restoring the
      working installation.

## Phase 5 — Splits, universe selection & causal research table (MBO playbook Sections 6, 8.1)
- [x] Freeze chronological session split (whole sessions) with purge/embargo; save `config.yaml`
      + `frozen_config.sha256`.
- [x] Implement the Phase 5 causal research-table builder in `src/options_research/research/grid.py`
      and the bounded driver in `scripts/phase05_research.py`.
- [x] Profile development sessions only; rank candidate contracts; pick the bounded research
      universe that is later used in EDA and hypothesis testing.
- [x] Build 1-minute decision grid per contract; backward-only `join_asof` (by contract/session)
      against the reconstructed BBO and the mapped underlying future; compute `log(K/S)`, `dte`,
      `quote_age`, `entry_eligible`.
- [x] Report grid coverage (n_grid vs n_with_quote) per session; before/after coverage table.

## Phase 6 — EDA (development only)
- [x] Coverage, strike/expiry distributions, calls vs puts, concentration.
- [x] Spread distributions (absolute/bps) by time of day, moneyness, expiry bucket; tick/premium
      ratio check (ticks may already be large relative to low option premiums).
- [x] Entry-filter pass-rate analysis.
- [x] Write 3 findings / 2 limitations / 1 testable hypothesis.

## Phase 7 — Option behavior & hypothesis (MBO playbook Section 8)
- [x] Price vs strike exhibits, option-mid vs underlying-return regressions (contemporaneous and
      lagged), OLS with clustered/HAC errors, return ACF.
- [x] Freeze a falsifiable hypothesis + pass/fail criteria (`hypothesis.json`) before touching
      validation/test data.

## Phase 8 — Backtester (MBO playbook Section 9)
- [x] Implement signal → contract selection → execution → ledger state machine per the frozen
      rules (DTE window, moneyness band, spread/size filters, holding period, fees/slippage,
      futures-roll guard on the lookback window).
- [x] Run on development + validation only.

## Phase 9 — Validation, benchmarks, robustness, test-once (MBO playbook Section 10)
- [x] PnL decomposition, equity curve/drawdown, day-block bootstrap, benchmarks (cash,
      underlying-direction, optimistic mid).
- [x] Robustness checks (cost cases, delay, freshness) on dev+validation only.
- [x] Open test split exactly once after freezing config; report final retain/reject decision.

## Phase 10 — Verification suite
- [x] Unit tests: causality (feature ts <= decision ts, fill >= decision+delay), no future-quote
      joins, contract fixed while held, hand-reproduced example ledger PnL to the cent.
- [x] 3 real development trades hand-calculated and asserted against code output.

## Phase 11 — Report
- [x] 6–10 page report using only numbers from `facts_step*.json`/CSVs/figures (cite sources).
- [x] README with run order, dependency versions, config, seeds, AI-use statement.

---
### Data Facts (observed, from `databento_options_clean.parquet`)

```
--- SCHEMA ---
      column_name column_type null   key default extra
0         ts_recv   TIMESTAMP  YES  None    None  None
1        ts_event   TIMESTAMP  YES  None    None  None
2        ts_index   TIMESTAMP  YES  None    None  None
3           price      DOUBLE  YES  None    None  None
4           rtype     INTEGER  YES  None    None  None
5    publisher_id     INTEGER  YES  None    None  None
6   instrument_id     INTEGER  YES  None    None  None
7          action     VARCHAR  YES  None    None  None
8            side     VARCHAR  YES  None    None  None
9            size     INTEGER  YES  None    None  None
10     channel_id     INTEGER  YES  None    None  None
11       order_id     VARCHAR  YES  None    None  None
12          flags     INTEGER  YES  None    None  None
13    ts_in_delta     INTEGER  YES  None    None  None
14       sequence     INTEGER  YES  None    None  None
15         symbol     VARCHAR  YES  None    None  None
16   session_date     VARCHAR  YES  None    None  None
--- ROW COUNT ---
Total Rows: 35,045,335
```

**Route decision:** this schema (per-order `order_id`, `action`/`side`, `channel_id`, `flags`,
`ts_in_delta`, `sequence`, no bid/ask columns, ~35.0M rows) is **MBO (market-by-order)**, not
pre-aggregated top-of-book — and the row count matches the **Databento MBO feed signature**
(`prompt/eurex_options_strategy_playbook(1).md`, "44 daily files = 22 sessions x 2 folders, about
35M records") almost exactly. **The local playbook remains the implementation guide**, not the generic
Kaggle v2 / OPRA playbook. Top-of-book bid/ask must be **reconstructed from the MBO stream**
(Phase "Build top-of-book (BBO) from MBO" in the local playbook) — it is not a ready-made column.

### Open questions — resolved
1. ~~OPRA vs MBO feed vs other~~ → **Databento MBO options feed**, confirmed by schema + row count match and the notebook cross-check.
2. ~~Aligned underlying feed?~~ → Not yet confirmed; need to check `symbol` values for futures
   (`FCEU...`) vs options (`EUCO...`) prefixes per the local playbook's symbol convention.
3. ~~Decimal vs fixed-point prices?~~ → `price` is `DOUBLE`, i.e. **already decimal** (not int64
   fixed-point) — confirm no further scaling needed, and check for sentinel values anyway.
4. ~~Multi-session / `session_date` column?~~ → **Yes**, `session_date` (VARCHAR) already present
   in the file — sessions do not need to be derived from timestamps.

### Still to verify in Phase 1
- Distinct `symbol` prefixes/patterns (confirm futures vs options products, parse convention).
- `publisher_id`/`channel_id` distinct values (maps to the "44 files = 22 sessions × 2 folders"
  structure — are the two folders two channels/publishers?).
- Null/sentinel counts on `price`, `order_id`, `side` (side may be `A`/`B`/`N`; action may be
  `A`/`C`/`M`/`R`/`T`/`F` per the local MBO playbook).
- Whether `ts_recv` is monotonic per file/instrument (no sort needed) as the local playbook expects.
- `order_id` dtype is VARCHAR — confirm numeric range before any `dense_rank` factorization for a
  numba/BBO-rebuild loop.