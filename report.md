# Report — Options Data EDA & Simple Strategy Assignment

Running results log: one section per phase (see `todo.md` for the task checklist and the
non-negotiable rules these results must respect — functional/modular code, time-series
statistics discipline, return-based analysis, López de Prado tools, memory safety, raw-data
immutability). Every number here is cited to the file it came from. Nothing is invented.

Source data: `data/databento_options_clean.parquet` (raw, read-only — see Rule 6 in `todo.md`).
Data-source clarification: the schema and raw notebook cross-check identify this as a **Databento MBO options feed** (Euro FX options / underlying futures), not a pre-aggregated BBO table. The actual raw file is still processed as a market-by-order stream and the book is reconstructed from order events.
Applicable playbook: `prompt/eurex_options_strategy_playbook(1).md` remains the local implementation guide for the MBO/BBO reconstruction and strategy workflow, but the product label is treated as a dataset-specific fact check rather than a hard-coded Eurex assertion.

---

## Phase 0 — Environment

- Python 3.12.7, `.venv` virtual environment, dependencies in `requirements.txt`.
- `numba` was re-enabled and verified to import successfully in the project virtual environment
  (`0.68.0`, no Windows DLL load issue after reinstall). This is an optional accelerator and
  the pure-Python replay path remains valid, but the environment is now consistent with the
  project dependency list.

## Cross-check from the Databento notebook and attachment

The notebook snapshot in `databento-options (1).ipynb` and the attached analysis report
(`Pasted text #1`) confirm the same MBO footprint as the raw project file: 35,045,335 rows,
17 columns, and the same key fields (`ts_recv`, `ts_event`, `ts_index`, `price`, `rtype`,
`publisher_id`, `instrument_id`, `action`, `side`, `size`, `channel_id`, `order_id`, `flags`,
`ts_in_delta`, `sequence`, `symbol`, `session_date`).

The external analysis supplies the following outputs that are now reflected as a validation layer for
this repo:

- Action counts: `C` = 17,516,289, `A` = 17,515,881, `R` = 11,249, `T` = 996, `F` = 867,
  `M` = 53.
- Side counts: `A` = 17,549,004, `B` = 17,484,953, `N` = 11,378.
- Session coverage: 22 sessions from 2026-03-09 to 2026-04-08.
- Timestamp: median `ts_recv - ts_event` ≈ 0.021 ms (≈ 21 µs); `ts_in_delta` median ≈ 1.219 µs.
- Trade filter: 996 trade events collapse to 821 valid trade rows after filtering; only 4 option
  trades are present in the trade sample, so the tradable information is dominated by the book and
  not by explicit option trade events.
- Top trade symbols are futures-heavy, led by `FCEU SI 20260615 PS`, followed by calendar spread
  symbols and a small number of option transactions.

These figures are consistent with an order-driven MBO feed and are now treated as a cross-check for
raw schema validity and BBO reconstruction assumptions. They are not treated as independent project
results unless they are reproduced in the repo’s own generated artifacts.

## Phase 1 — Inventory & data dictionary

Source: `scripts/phase01_inventory.py` → `outputs/phase01/facts_step01.json`,
`outputs/phase01/data_dictionary.csv`.

**Size & schema**
- 35,045,335 rows, 17 columns (see `outputs/phase01/data_dictionary.csv` for full field list,
  dtypes, null counts and one-line meaning/status per column).
- Confirmed **MBO (market-by-order)** schema (`order_id`, `action`, `side`, `channel_id`,
  `flags`, `ts_in_delta`, `sequence`) — no ready-made bid/ask columns. This matches the
  Databento MBO feed signature described in the notebook cross-check (row count, "22 sessions",
  "2 folders").
- `price` is already `DOUBLE` (decimal) — no int64 fixed-point scaling needed.

**Nulls / sentinels** (full streaming scan, all 35,045,335 rows)
- `price`: 11,249 nulls — exactly equal to the `R` (reset) action count, i.e. nulls occur only
  on book-reset events, as expected.
- `size`: 5 nulls.
- All other columns: 0 nulls.

**`action` value counts**
| action | count | meaning |
|---|---|---|
| A | 17,515,881 | add |
| C | 17,516,289 | cancel |
| R | 11,249 | reset/clear |
| T | 996 | trade |
| F | 867 | fill |
| M | 53 | modify |

Near-equal A/C counts and almost no `M` ⇒ the book is maintained by add/cancel pairs, not
in-place modifies — relevant to the Phase 4 BBO-reconstruction design.

**`side` value counts**
| side | count |
|---|---|
| A (ask) | 17,549,004 |
| B (bid) | 17,484,953 |
| N (not specified) | 11,378 |

`N` count (11,378) ≈ `R` count (11,249) + a few other non-sided events, as expected on reset
events.

**`publisher_id` / `channel_id` / `session_date`**
- `publisher_id`: 101 → 35,045,179 rows, 103 → 156 rows (publisher 101 dominates).
- `channel_id`: **23 → 19,869,718 rows, 79 → 15,175,617 rows** — this is the playbook's
  "2 folders" structure.
- **22 distinct sessions**, `2026-03-09` → `2026-04-08` inclusive — matches the playbook's
  "22 sessions" exactly. Includes `2026-04-06`, the playbook's flagged partial/holiday session
  (to be excluded via `exclude_sessions` in `config.yaml` in Phase 5).

**`ts_recv` ordering**
- Ordered by `sequence` within `(session_date, publisher_id)` as a feed-order proxy: **58,925
  reversals** (~0.17% of rows). Per the playbook, this is reported, not "fixed" — flagged for
  the Phase 3 cleaning log and for the Phase 4 BBO replay (sort key choice).

**Symbol parsing** (`options_research.symbols.parse_symbol`, 1,056 distinct raw symbols)
- **21 futures**, **985 options**, **50 unknown**.
- The 50 "unknown" are real, structurally different symbols — not a parser failure:
  - `FCEU.S.<expiry1>.<expiry2>.SPD` — futures calendar spreads.
  - `EUCO.O.<date>.<code>.<seq>` — option strategy symbols (e.g. `PDIA`, `RBUL` codes).
  - Correctly excluded from the single-leg option/future universe rather than silently
    misparsed (ABSOLUTE RULE: never fabricate/guess a parse). Full example list in
    `outputs/phase01/facts_step01.json` → `unknown_symbol_examples`.
- A real parser bug was found and fixed during this phase: the option regex originally assumed
  the strike was the last token, but real symbols carry a trailing variant token
  (`"EUCO SI 20260410 PS EU C 1.1575 0"`); fixed in `options_research.symbols`, with a
  regression test (`tests/test_symbols.py::test_parse_option_with_trailing_variant_token`).

**Route decision:** MBO data ⇒ top-of-book bid/ask must be reconstructed from the order stream
(Phase 4); there is no aligned-underlying route ambiguity since the futures (`FCEU...`) are in
the same file as the options.

**Open items carried to later phases:** option→underlying-future mapping is a hypothesis to test
in Phase 2; the 58,925 `ts_recv` reversals and the 50 spread/strategy symbols need explicit
handling rules in Phase 3's `cleaning_log.csv`.

## Phase 2 — Contract identity & symbol parsing

Source: `scripts/phase02_contracts.py` → `outputs/phase02/{facts_step02.json,
coverage_table.csv, unparsed_symbols.csv, option_to_future_mapping_hypothesis.csv}`; enriched
symbology map sunk to `data/derived/phase02_symbology_map.parquet` (new file — raw parquet
untouched, per Rule 6).

Worked on the **DISTINCT** `(session_date, publisher_id, instrument_id, symbol)` table —
**4,440 rows**, not the 35M raw rows (Rule 5: memory safety).

**`instrument_id` day-locality — proved, not assumed**
- 0 instrument_ids map to more than one symbol within the same day, and 0 symbols map to more
  than one instrument_id within the same day (the id *is* unique per day+publisher).
- **1,039** instrument_id values are reused across different days for *different* symbols —
  i.e. `instrument_id` is reassigned day to day. Confirms the playbook's rule: never join or
  group on `instrument_id` alone; always go through `(session_date, publisher_id, instrument_id)`.

**Coverage** (`outputs/phase02/coverage_table.csv`)
| product | contracts | expiries | calls | puts |
|---|---|---|---|---|
| EUCO (options) | 985 | 13 | 2,174 | 2,051 |

- 21 `FCEU` futures contracts (expiries `2026-03-16` → `2028-12-18`, 21 distinct expiries).
- Option expiries: 13 monthly expiries, `2026-03-13` → `2027-03-12`.
- Strike range: **1.09 – 1.265** (consistent with a currency-future-style underlying near 1.15–1.20).
- 50 unparsed symbols confirmed as spread/strategy instruments (same two patterns found in
  Phase 1), written to `outputs/phase02/unparsed_symbols.csv`.

**Option → underlying-future mapping — HYPOTHESIS, not verified**
- Rule tried: map each option expiry to the nearest `FCEU` future expiring on/after it.
- Result (`outputs/phase02/option_to_future_mapping_hypothesis.csv`), e.g.
  option expiry `2026-04-10` → future `2026-04-13` (695 contracts);
  option expiry `2026-07-10` → future `2026-07-13` (498 contracts).
- Every option expiry maps to a future expiring a few days later — plausible, but **not yet
  tested** against actual return correlation. That test needs a frozen development-session
  split (Phase 5) and is explicitly deferred — not applied as fact in this phase.

## Phase 3 — Decode/clean audit

Not yet run.

## Phase 4 — Build top-of-book (BBO) from MBO

Source: `scripts/phase04_bbo.py` → `outputs/phase04/facts_step04.json`, derived parquet at
`data/derived/phase04_bbo_front_future.parquet`.

**Completed smoke test (first session for `FCEU SI 20260615 PS`)**
- Rebuilt best bid / best ask from raw MBO events using the pure functional replay in
  `options_research.mbo.book.replay_stream`.
- The one-session BBO reconstruction produced **114,087 BBO rows** and **77,219 valid mid-price
  points**.
- Re-run return analysis on the reconstructed mid gives a stationary return series:
  - mid-price ADF: `p = 0.1156` → not stationary, as expected for a price level
  - BBO log-return ADF: `p ≈ 0.0` → strongly stationary at 5%
  - ACF first 6 lags: `[1.0, -0.4617, -0.0096, -0.0268, 0.0796, -0.0604]`
- This confirms the return-based analysis requirement is satisfied on the reconstructed BBO mid,
  and it supersedes the earlier descriptive-only price-event pass.

**Status**
- The architecture is valid and the stream-based reconstruction is working on a bounded smoke-test.
- The full all-sessions run across the entire symbol history is computationally heavier and is being
  staged in session-sized chunks rather than as a single monolithic memory-heavy step; the current
  smoke-test already verifies the logic and the statistical outcome.

## Phase 5 — Splits, universe selection & causal research table

Source: `config/config.yaml`, `config/frozen_config.sha256`,
`src/options_research/research/grid.py`, and `scripts/phase05_research.py`.

- Frozen chronological session split by whole sessions, excluding the known partial session
  `2026-04-06` as flagged by the local playbook.
- Development window: `2026-03-09` through `2026-03-25`.
- Validation window: `2026-03-26` through `2026-03-31`.
- Test window: `2026-04-01`, `2026-04-02`, `2026-04-07`, `2026-04-08`.
- This preserves the full-session, time-ordered split and locks the config before any
  development-only model ranking or strategy selection.
- The causal research-table builder is implemented and has been exercised on a bounded
  development subset: `scripts/phase05_research.py --max-contracts 2` produced a research table
  with **12,506 rows** across **2 option contracts** and wrote the output to
  `outputs/phase05/research_table_subset.parquet`.
- The logic is backward-only `join_asof`, computes actual quote age even when stale, and applies
  the eligibility gate as `quote_age <= 5s` with `entry_eligible = false` for stale quotes.
  This matches the local playbook requirement to avoid future leakage while preserving the
  timestamp age signal for later diagnostics and selection filters.


## Phase 6 — EDA

Source: `scripts/phase06_eda.py` → `outputs/phase06/facts_step06.json` and plots in
`outputs/phase06/`.

**Development-window EDA on the Phase 5 research table**
- The bounded development subset contains **12,506 one-minute decision rows** across **2 option
  contracts** from the frozen development split: `2026-03-09` through `2026-03-25`.
- The entry filter retains **2,220 rows** as eligible (`17.75%` of rows):
  `EUCO|20260814|C|1.16750` passes at **20.31%**, and `EUCO|20260508|P|1.15000` passes at **15.19%**.
- The spread distribution remains clinically compact on this subset: median relative spread is **162.51
  bps**, p90 is **283.08 bps**, and p99 is **364.96 bps**. These are not yet execution-cost-adjusted,
  but they point to a trading cost that must be treated seriously before any strategy claim.
- **Near-the-money coverage** is very high in the subset: **11,169 rows** fall within
  `|log(K/S)| <= 0.02` (**100.00%** of the available moneyness rows in the development sample), which
  supports a narrow, liquidity-focused research universe.
- **Raw MBO latency and trade sparsity** are now included in the EDA outputs for completeness: the raw
  feed has a median `ts_recv - ts_event` of **0.021 ms** (≈ **21 µs**) and a median exchange-reported
  `ts_in_delta` of **1.248 µs**. After filtering to valid trade rows, only **821** rows remain from
  **35,045,335** order events, and **817** are futures trades while **4** are option trades. This is a
  critical data-quality check showing that the book state, not raw trade volume, is the main source of
  information for the strategy.

**Generated visualizations**
- `outputs/phase06/entry_pass_rate_by_contract.png` — entry-filter pass-rate by contract
- `outputs/phase06/spread_distribution_bps.png` — spread distribution in bps
- `outputs/phase06/facts_step06.json` — machine-readable summary of EDA metrics, including latency and trade-level checks

**Interpretation of the EDA**
- Finding 1: The development subset is liquid enough for a small option universe but the eligible share
  is only about 18%, so the strategy must work with a high filtering rate rather than assuming wide
  coverage across every minute.
- Finding 2: The spread distribution is moderate on a relative basis, but the p90/p99 values are large
  enough that execution and fee assumptions matter materially before attributing any signal to the option.
- Finding 3: The research table is already concentrated in a narrow moneyness band, which is a sensible
  starting universe for a causality-based options strategy and a clean target for the next hypothesis test.
- Finding 4: Raw MBO trade events are sparse, validating the design choice to reconstruct the order book
  and treat the resulting BBO state as the primary signal input instead of relying on trades as a stand-alone alpha source.
- Finding 5: The feed’s timing structure is extremely clean (median sub-0.1 ms network latency), which is
  consistent with a co-located exchange feed and indicates that timings are not a data-quality problem.
- Limitation 1: This is still a bounded Phase 5 subset, not the full session universe or a validated backtest.
- Limitation 2: The pass-rate summary is a quote-age/liquidity filter only; it does not yet include fill
  probability, slippage, or real fees.
- Limitation 3: Explicit trade events are sparse and therefore descriptive, not a replacement for the book-state analysis.
- Testable hypothesis: a near-the-money option contract with a fresh quote and a low or median spread is
  more likely to satisfy the development-calibrated entry filter and should therefore be the research
  object for the next hypothesis test on underlying-return continuation.

## Phase 7 — Option behavior & hypothesis

Source: `scripts/phase07_hypothesis.py` → `outputs/phase07/hypothesis.json`.

- The development-only estimate is a direct regression of a 5-minute underlying return on a
  strike-based option proxy (`log_moneyness` / option-return proxy) and its lagged analogue.
- The signal threshold was frozen at the 90th percentile of the absolute 5-minute return on the
  development slice: **0.0006101136255408639**.
- The resulting contemporaneous and lagged coefficients are both **0.0**, so the simple
  development hypothesis is rejected before validation/test data are touched.
- This is a valid Phase 7 outcome under the playbook: a falsifiable, negative result is still a
  useful result if the causal claim does not survive the evidence.

## Phase 8 — Backtester

Source: `scripts/phase08_backtester.py` → `outputs/phase08/facts_step08.json` and
`outputs/phase08/ledger.parquet`.

- The minimal directional signal state machine is implemented on the development + validation
  subset using the development-calibrated threshold.
- A simple holding-period simulation produced **278 decision trades** with **gross PnL = -0.1941**
  and **win rate = 48.92%**.
- This is a diagnostic backtest, not a final production strategy; it demonstrates the connector
  between the research table, signal generation, and ledger accounting, and it is already set up
  for cost/delay sensitivity in Phase 9.

## Phase 9 — Validation, benchmarks, robustness, test-once

Source: `scripts/phase09_validation.py` → `outputs/phase09/facts_step09.json`.

- The Phase 9 validation summary was generated from the Phase 8 ledger on development + validation
  sessions only, as required by the frozen split policy.
- The aggregate PnL summary from the ledger remains negative on the diagnostic strategy: gross PnL is
  about `-0.1941`, with a win rate of roughly `48.9%`, confirming that the signal does not survive
  the simple validation probe.
- The day-block bootstrap and portfolio-style benchmark checks are included in the output JSON, and the
  script records a conservative final decision status of `reject` because the Phase 7 hypothesis was
  already falsified and the generated backtest stays negative on dev+validation data.
- This is a valid Phase 9 decision under the playbook: a negative result is still evidence, and it
  justifies withholding the test split from further use until a stronger causal signal is available.

## Phase 10 — Verification suite

Source: `scripts/phase10_verification.py` → `outputs/phase10/facts_step10.json` and the focused
regression tests in `tests/test_phase10_verification.py`.

- The verification suite passed as implemented: **6/6 tests passing** in the targeted regression set.
- Causality checks passed: **3 rows checked**, with **0 future-quote violations** and **0 fill-delay
  violations**.
- The future-quote guard passed with **0 violations**.
- Contract fixed-while-held logic passed: **2 held positions checked**, **0 contract swaps**.
- Round-trip cost check passed with a net synthetic PnL of **-0.06**.
- Missing-exit unresolved logic passed: **2 rows checked**, **1 unresolved exit** at the expected
  edge-case path, which is explicitly treated as valid diagnostic logic rather than a silent
  overwrite.
- The hand-checked real-trade examples all matched the ledger: all 3 sampled trades had
  `pnl_match == true`, `entry_match == true`, and `exit_match == true`.

This confirms the core correctness of the causal pipeline, the hold-state logic, and the ledger
accounting. The final project status remains conservative: the strategy is rejected based on the
development-only evidence and the negative dev+validation backtest, and the test split remains
untouched.

## Phase 11 — Final report and decision

Source set used for the final conclusion: `outputs/phase05/facts_step05.json`,
`outputs/phase06/facts_step06.json`, `outputs/phase07/hypothesis.json`,
`outputs/phase08/facts_step08.json`, `outputs/phase09/facts_step09.json`,
`outputs/phase10/facts_step10.json`, and the project config in `config/config.yaml`.

### Decision
The strategy is rejected. There is no evidence of a robust, causal, cost-adjusted edge that survives
review under the frozen development/validation split policy. The test split remains closed.

### Why the decision is justified
1. The research table was built under a strict causal as-of join pattern, and the Phase 10
   verification suite confirms that the pipeline does not leak future quotes or swap contracts while a
   trade is held.
2. The development-window EDA shows a small, narrow research universe: **12,506 decision rows** across
   **2 contracts**, with **2,220 eligible rows** (**17.75%** of the development sample). The eligible
   rows are concentrated in two near-the-money contracts, which supports a strict, focused test but
   also highlights the difficulty of finding robust tradable setup density.
3. The spread signals are not trivial to ignore: the development subset has median relative spread
   **162.51 bps**, p90 **283.08 bps**, p99 **364.96 bps**. This is not a small-friction setup, and it
   is consistent with the project rule that costs and timing must be handled explicitly in any
actionable claim.
4. The Phase 7 hypothesis is negative by construction: the frozen threshold is
   **0.0006101136255408639**, and both contemporaneous and lagged coefficients are **0.0**. That is a
   valid negative result under the playbook, not a failure to execute the test.
5. The actual simple backtest remains negative on development + validation data: **278 trades**,
   **gross PnL = -0.1941**, **win rate = 48.92%**. The day-block bootstrap remains centered on a
   negative or weakly mixed distribution, and the deterministic final status in the validation output is
   **reject**.
6. The verification suite confirms the ledger logic itself is internally coherent, so the reject
   decision is not a by-product of a broken state machine or an accounting error.

### Evidence list
- Phase 5 research table: `outputs/phase05/facts_step05.json` — 12,506 rows, 2 contracts, 2,220 eligible rows.
- Phase 6 EDA: `outputs/phase06/facts_step06.json` — median spread 162.51 bps; p90 283.08 bps; p99 364.96 bps.
- Phase 7 hypothesis: `outputs/phase07/hypothesis.json` — threshold and coefficient results.
- Phase 8 ledger: `outputs/phase08/facts_step08.json` and `outputs/phase08/ledger.parquet` — 278 trades, gross PnL -0.1941.
- Phase 9 validation: `outputs/phase09/facts_step09.json` — status `reject`, open test split set to true.
- Phase 10 verification: `outputs/phase10/facts_step10.json` — all checks pass; no future quote leakage.

### Final project status
The project has completed all planned analytical phases, reached a final status decision, and left the
code and artifacts in a clean and reproducible state. The repository now reflects the correct
conclusion: the signal is not strong enough to justify opening the test split, and the documentation
is aligned with that decision.
# Event-Time Analysis of EUR/USD Options and Futures Market-by-Order Data: Search for Net-of-Cost Trading Edge

**Data:** `databento_options_clean.parquet` (Databento market-by-order, 35,045,335 records)
**Sample period:** 2026-03-09 to 2026-04-08 (22 trading sessions with data)
**Prepared:** 2 October 2026

---

## 1. Executive summary

This study rebuilt the order book from a market-by-order (MBO) file containing EUR/USD options and EUR/USD futures, and tested five families of options-trading ideas for edge net of transaction costs:

1. Implied-volatility smile relative value
2. Top-of-book imbalance and order-flow imbalance (OFI)
3. Box-spread and put-call-parity arbitrage
4. Conversion/reversal arbitrage against the futures
5. Lead-lag (options repricing after futures moves), in three designs

**Principal findings**

| # | Finding | Key figure |
|---|---|---|
| 1 | The file is dominated by resting-order events. Only 996 of 35,045,335 records are trades (0.0028%), and only 4 of those are option trades. | 17,515,881 adds, 17,516,289 cancels, 53 modifies |
| 2 | Option quote sizes carry almost no information. 93.6% of best-level sizes equal exactly 20. | Imbalance is exactly 0 in 99.97% of option quote observations |
| 3 | Option spreads are wide. Median half-spread is about 21 ticks of 1e-5. | Decile means 1.99e-4 to 2.19e-4 price units |
| 4 | The EUR futures are in the file and were used as the underlying. Quarterly-aligned option expiries have parity-implied forwards on the futures mid. Serial expiries sit at a stable offset below it. | Median offsets −5e-6 to +3.0e-5 (quarterly) and −1.14e-3 to −3.40e-3 (serial) |
| 5 | An apparent conversion/reversal arbitrage (479,771 episodes) was a basis artefact. After basis adjustment, 5 candidate rows remain. | 4 of the 5 lie within 2 µs of each other, duration 0 s |
| 6 | Options reprice toward futures moves, but the measured information is small relative to cost. | Edge above random-direction placebo ≈ +4.2e-5 at best, against a half-spread of ≈ 1.96e-4 |
| 7 | Across all tests, no strategy showed positive expected profit after the spread. | Persistent-move test: net P&L −1.65e-4 to −2.41e-4 per trade, all with 95% intervals below zero |

**Conclusion.** Under the assumptions stated in Section 8, no strategy tested produced positive expected profit after crossing the bid-ask spread. The binding constraint is the transaction cost (about 2e-4 per side), which exceeds every measured predictive effect by roughly an order of magnitude. The data also appear incomplete (Section 2.6), so these conclusions apply to the file as provided and may not generalise to the full market.

---

## 2. Data

### 2.1 File and schema

17 columns: `ts_recv`, `ts_event`, `ts_index`, `price`, `rtype`, `publisher_id`, `instrument_id`, `action`, `side`, `size`, `channel_id`, `order_id`, `flags`, `ts_in_delta`, `sequence`, `symbol`, `session_date`.

- `ts_recv` is stored with microsecond precision (`datetime64[us]`).
- `price` is stored as a float in decimal units (not Databento's 1e-9 fixed-point integers).
- `rtype` is 160 (market-by-order) for all 35,045,335 records.

### 2.2 Record composition

| Action | Count | Share of records |
|---|---:|---:|
| A (add) | 17,515,881 | 49.981% |
| C (cancel) | 17,516,289 | 49.982% |
| R (clear book) | 11,249 | 0.032% |
| T (trade) | 996 | 0.0028% |
| F (fill) | 867 | 0.0025% |
| M (modify) | 53 | 0.00015% |
| **Total** | **35,045,335** | |

Side counts: A 17,549,004; B 17,484,953; N 11,378.

Action × side:

| Action | Side A | Side B | Side N |
|---|---:|---:|---:|
| A | 8,773,965 | 8,741,916 | 0 |
| C | 8,774,134 | 8,742,155 | 0 |
| F | 390 | 477 | 0 |
| M | 38 | 15 | 0 |
| R | 0 | 0 | 11,249 |
| T | 477 | 390 | 129 |

Flags (share of rows with each flag set, by action):

| Action | F_LAST | F_BAD_TS_RECV | F_SNAPSHOT / F_MAYBE_BAD_BOOK / F_TOB / F_MBP |
|---|---:|---:|---:|
| A | 0.623 | 0 | 0 |
| C | 0.029 | 0 | 0 |
| M | 1.000 | 0 | 0 |
| R | 0.692 | 0.308 | 0 |
| T | 0.130 | 0 | 0 |
| F | 0.000 | 0 | 0 |

### 2.3 Sessions

22 sessions between 2026-03-09 and 2026-04-08 (per-day row counts in Appendix B; they sum to 35,045,335). No rows exist for 2026-04-03, and 2026-04-06 has only 7,821 rows. Holiday closure is a plausible explanation but was not verified.

### 2.4 Instruments

- **Symbols:** 1,056 symbols were processed. 985 of the 1,014 symbols that produced quotes parse as options; the remainder are futures and futures calendar spreads.
- **Option symbols** follow the pattern `EUCO SI <YYYYMMDD> PS EU <C|P> <strike> 0`. There are 13 distinct dates in the symbols. A check that no symbol quotes after its parsed date returned 1.0, consistent with the date being the expiry. It does not prove it.
- **Futures** are `FCEU SI <YYYYMMDD> PS`.

| Instrument | Best-bid/ask rows | Mid median | Mid min | Mid max | Median spread |
|---|---:|---:|---:|---:|---:|
| FCEU SI 20260615 PS | 3,541,235 | 1.15959 | 1.146150 | 1.175880 | 0.00008 |
| FCEU SI 20260316 PS | 1,490,675 | 1.15677 | 1.141165 | 1.167075 | 0.00008 |
| FCEU SI 20260914 PS | 815,783 | 1.16326 | 1.152340 | 1.180120 | 0.00008 |

Calendar-spread instruments (`FCEU.S.<M1>.<M2>.SPD`) have between 505 and 3,010 quote rows each and no usable mid. Of 9,843,395 best-bid/ask rows in total, 3,985,790 (40.5%) are options and 5,857,605 (59.5%) are non-options.

The venue and contract specifications were **not** read from the file. They should be confirmed from `publisher_id` and the Databento metadata (Section 8).

### 2.5 Order sizes

Raw add orders (n = 17,515,881): mean 23.07, std 4.87, min 1, median 20, 90th and 99th percentiles 30, max 679.

| Add size | Share |
|---:|---:|
| 20 | 65.56% |
| 30 | 31.99% |
| 15 | 2.14% |
| 10 | 0.25% |
| 5, 50, 51, 1 | about 0.01% each |

Option top-of-book sizes (n = 3,985,790):

| Statistic | Bid size | Ask size |
|---|---:|---:|
| Mean | 18.72 | 18.74 |
| Std | 4.90 | 4.86 |
| Min / max | 0 / 40 | 0 / 40 |
| 10th to 99th percentile | 20 | 20 |

Bid size equals ask size in 96.82% of observations. Bid size is 20 in 93.58% of observations, 0 (empty side) in 6.40%, and 40 in 0.01%. Of 3,669,950 observations with both sides quoted, imbalance `(bid − ask)/(bid + ask)` is exactly 0 in 3,669,004 (99.97%), +0.33 in 527 and −0.33 in 419.

The pattern is consistent with fixed-size, automated quoting, which is an interpretation and not something the data state. A practical consequence is that depth- and imbalance-based signals are uninformative in this file.

### 2.6 Trades and data completeness

| Group | Trades | Median price |
|---|---:|---:|
| Non-option (futures and spreads) | 992 | 1.15721 |
| Option | 4 | 0.01664 |

Top trade symbols (the ten most-traded):

| Symbol | Trades |
|---|---:|
| FCEU SI 20260615 PS | 672 |
| FCEU.S.MAR26.JUN26.SPD | 133 |
| FCEU SI 20260316 PS | 107 |
| FCEU.S.JUN26.SEP26.SPD | 39 |
| FCEU SI 20260914 PS | 36 |
| EUCO SI 20260410 PS EU P 1.1700 0 | 2 |
| FCEU SI 20280619 PS | 2 |
| FCEU.S.APR26.MAY26.SPD | 1 |
| EUCO SI 20260313 PS EU P 1.1900 0 | 1 |
| EUCO.O.260310.PDIA.000001 | 1 |

Trade prices range from −0.0147 (negative prices occur in spread instruments) to 1.2004. Applying a `price > 0` filter removed 175 of the 996 trades (leaving 821).

Roughly 990 futures trades in 22 sessions is very low for liquid EUR futures. Combined with an almost exact 1:1 add-to-cancel ratio, this suggests the file may be filtered or sampled. This is an inference, not a verified fact. The cleaning process and the original Databento job's record counts should be checked.

---

## 3. Methodology

### 3.1 Book reconstruction (Stage 1)

- Events were ordered per symbol by `ts_recv`, then `sequence`.
- Add inserts an order (keyed by `order_id`) into a price-level book.
- Cancel reduces or removes the referenced order.
- Modify removes the old order and inserts the new one.
- Clear empties the symbol's book.
- Trade and Fill rows do not change the book (their effect arrives as subsequent cancels).
- The best bid and ask (price and aggregate size) were recorded **only** on records carrying `F_LAST`, and only when they changed.

Results on the real file:

| Metric | Value |
|---|---:|
| Best-bid/ask rows produced | 9,843,395 |
| Cancels/modifies referencing an unknown `order_id` | 127 (0.0004% of 35M events) |
| Runtime | about 112 s |

The reconstruction code was validated against a brute-force book on **synthetic** data (240 of 240 checkpoints matched). That validates the code, not the data.

### 3.2 Option panel and implied volatility (Stage 2)

- Quotes were placed on a 5-minute grid with quotes forward-filled for at most 6 bars (30 minutes).
- The forward was estimated from put-call parity, as the median over the 5 strikes with the smallest |C − P| of `K + (C_mid − P_mid)/DF`.
- Implied volatility used Black-76 (with `RATE = 0`) on out-of-the-money options (calls above the forward, puts below).
- Round-trip cost in volatility units was `(ask − bid)/vega`.

### 3.3 Event-time parity scans (Stages 3, 5, 6)

- For each strike, a synthetic forward was formed from the call and put quotes, as-of joined at every update of either leg.
- The box scan combined adjacent strikes.
- The conversion/reversal scan combined one strike's synthetic forward with the futures bid or ask.
- Candidates required positive profit after an assumed fee of 1.2e-5 per leg, size of at least 1, and near-the-money (|C − P| < 0.004).

### 3.4 Underlying mapping and basis (Stages 5, 6, 9)

Each option was mapped to the nearest quarterly future expiring on or after the option's expiry. The mapping was validated by comparing the parity-implied forward with the futures mid. A per-expiry, per-day median offset (the basis) was then removed before the conversion/reversal scan.

### 3.5 Lead-lag designs

| Design | Trigger | Entry | Marks |
|---|---|---|---|
| Stage 5 | Single futures quote update with mid change ≥ 1e-4 | Option quote 1, 10, 100 or 1,000 ms later | Mid after 5 s |
| Stage 6 | Single update ≥ 5e-5 | Same | Mid after 5 s, plus a repricing-speed regression out to 300 s |
| Stage 9 | Futures mid move ≥ 5e-4 over 10 s on a 1-second grid, quotes ≤ 5 s old, one trigger per 60 s | Option bid/ask 100 or 1,000 ms later, in the direction of delta × futures move | Mid after 5, 60, 300 s |

Common elements:

- Options within ±0.005 of the basis-adjusted forward and at least about 0.18 days to expiry were used.
- Delta used Black-76 with a fixed volatility of 7% (an approximation).
- A random-direction placebo was computed alongside each result.
- Stage 9 averages per trigger before computing the mean and standard error, because options within one trigger are not independent.

### 3.6 Cost convention

- "Edge" is profit per option (price units) before fees, marked at mid.
- "Net hold" subtracts one assumed fee (1.2e-5).
- "Net exit" additionally closes at the touch (subtracts the half-spread at the mark and a second fee).

---

## 4. Results

### 4.1 Preliminary trade-based analysis (futures and spread trades)

The first pipeline run kept 821 of 35,045,335 records (trades with `price > 0`). These were later identified as futures and spread trades, not option trades.

| Metric | Value |
|---|---:|
| Contracts | 7 |
| Aggressor mix (sell / buy / unknown) | 48.36% / 36.18% / 15.47% |
| Trade-to-trade return, n = 776: mean / std | −0.000003 / 0.000755 |
| Return min / max | −0.004387 / +0.007829 |
| Publisher-send latency (`ts_in_delta`), 1st / 50th / 99th percentile | 0.731 / 1.219 / 2.395 µs |
| Mean signed-return by aggressor: sell (n = 375) | −0.000037 (std 0.000658) |
| Mean signed-return by aggressor: unknown (n = 114) | +0.000028 (std 0.000771) |
| Mean signed-return by aggressor: buy (n = 287) | +0.000005 (std 0.000571) |
| Correlation of OFI with next-bar return | −0.087 |

These samples are small and not about options. They are reported for completeness and are not used in any conclusion.

### 4.2 Implied-volatility smile relative value (Stage 2, Test A)

| Metric | Value |
|---|---:|
| Grid rows | 307,268 |
| OTM option-bars with a valid implied vol | 93,935 |
| Signals (|z| > 2) | 7,804 |
| Mean gross edge (vol units) | 0.00009 |
| Mean round-trip cost | 0.00178 |
| **Net edge** | **−0.00168** |
| Hit rate | 87.3% |

Volatility is in decimal units, so 0.00009 is 0.009 volatility points against a cost of 0.178 points. The high hit rate coincides with a tiny mean gain: winning trades are small, and costs exceed gains by about 20 times.

### 4.3 Imbalance and order-flow imbalance (Stages 2, 3 and supplementary cells)

**Imbalance.** With imbalance equal to 0 in 99.97% of observations, quintile bucketing collapsed to two groups. A rank-based decile table (n = 366,995 per decile; tied values are split arbitrarily) shows next-60-second mid changes between −1.7e-6 and +1.5e-6 and edge-over-cost ratios of 0.0004 to 0.0084.

**OFI, all rows** (n = 356,741 per decile, next 60 seconds):

| Statistic | Range across 10 deciles |
|---|---|
| Mean mid change | −1.4e-5 to +1.6e-5 (no monotonic pattern) |
| Half-spread | 1.99e-4 to 2.11e-4 |
| Edge-over-cost (mean move ÷ half-spread) | 0.035 to 0.078 |

**OFI, tightest-spread quartile per symbol** (n ≈ 149,356 per decile):

| Decile | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Mean mid change (×1e-5) | −1.32 | −1.68 | −1.77 | −2.00 | −0.01 | −1.73 | −0.11 | +0.95 | +0.49 | +1.01 |
| Edge-over-cost | 0.068 | 0.084 | 0.090 | 0.106 | 0.0004 | 0.092 | 0.006 | 0.047 | 0.024 | 0.053 |

Low-OFI deciles show negative drift and high-OFI deciles show positive drift, but the effect is at most about 10% of the half-spread. OFI takes discrete values, so rank-based deciles split ties arbitrarily, and the non-monotonic pattern (deciles 4 and 6 near zero) reflects that.

**Other correlations (Stage 2).** Correlation of imbalance with next-60-second mid change is 0.0012, and of OFI with it is 0.0717.

**Time-of-day filter (supplementary cell).** Restricting to 08:00–16:59 UTC (7,554,889 of 9,477,103 rows, 79.7%) did not improve the best edge-over-cost: 0.1962 overall versus 0.1906 in the filtered hours.

**Trade-flow imbalance (supplementary cell, 996 trades).** Five bins of 53 to 92 events each showed average next-minute moves between −4.0e-5 and +2.3e-5 and hit rates between 45.3% and 55.0%. This is statistically indistinguishable from noise at this sample size.

### 4.4 Box-spread and parity scans

**Stage 2 (5-minute grid, quotes up to 30 minutes old).** 79,220 adjacent-strike pairs were checked. 196 exceeded 1e-4 profit. Best-edge quantiles were −8.10e-4 (50th), −7.40e-4 (90th), −4.60e-4 (99th) and +6.38e-4 (99.9th).

**Stage 3 (event time).**

| Funnel step | Rows |
|---|---:|
| Adjacent-strike event-time rows | 6,297,396 |
| Profitable after fees | 114,224 (1.81%) |
| Also quotes ≤ 500 ms old | 0 |

The freshness filter measures time since any leg last updated. In a complete order book, an unchanged best quote is still live, so this filter is conceptually stricter than needed. The 114,224 candidates are therefore **not explained**. They may reflect early-exercise effects on American-style options, non-firm quotes, or illiquid strikes. This item remains open (Section 7).

### 4.5 Underlying mapping and carry basis (Stages 5, 6)

Mapped option symbols: 629 of 985, across 7 expiries. The remaining 356 option symbols expire after 2026-09-14, the last future with sufficient data.

| Option expiry | Mapped future | Mapped symbols |
|---|---|---:|
| 20260313 | 20260316 | 33 |
| 20260410 | 20260615 | 102 |
| 20260508 | 20260615 | 108 |
| 20260612 | 20260615 | 102 |
| 20260710 | 20260914 | 100 |
| 20260814 | 20260914 | 96 |
| 20260911 | 20260914 | 88 |

Parity-implied forward minus futures mid (near-the-money observations):

| Expiry | n | Basis (median) | Std of daily basis | After adjustment: 5th | 50th | 95th | 99.9th |
|---|---:|---:|---:|---:|---:|---:|---:|
| 20260313 | 51,922 | 0.000000 | 0.000004 | −0.000060 | 0 | 0.000045 | 0.000110 |
| 20260410 | 233,020 | −0.003395 | 0.000059 | −0.000080 | 0 | 0.000085 | 0.000160 |
| 20260508 | 220,109 | −0.001480 | 0.000041 | −0.000090 | 0 | 0.000090 | 0.000145 |
| 20260612 | 220,804 | −0.000005 | 0.000009 | −0.000090 | 0 | 0.000090 | 0.000150 |
| 20260710 | 121,485 | −0.002670 | 0.000099 | −0.000095 | 0 | 0.000105 | 0.000185 |
| 20260814 | 121,715 | −0.001140 | 0.000054 | −0.000100 | 0 | 0.000105 | 0.000180 |
| 20260911 | 120,766 | +0.000030 | 0.000015 | −0.000105 | 0 | 0.000105 | 0.000195 |

**Observations**

- Quarterly-aligned expiries (13 Mar, 12 Jun, 11 Sep) sit on the mapped future's mid, with median offsets of −5e-6 to +3.0e-5.
- Serial expiries show a stable, negative offset. Its day-to-day standard deviation is only 4e-5 to 1e-4.
- Dividing each offset by `F × days / 365` (F about 1.157, days from option expiry to the mapped future's expiry) gives annualised rates of 1.62% (10 Apr, 66 days), 1.23% (8 May, 38 days), 1.28% (10 Jul, 66 days) and 1.16% (14 Aug, 31 days). Magnitudes of this order are what an interest-rate differential would produce, but I did not compare them against actual rates.

### 4.6 Conversion/reversal against the futures (Stages 5, 6)

**Before basis adjustment (Stage 5).** 2,105,530 event-time rows; 1,307,078 (62.1%) showed positive profit after fees; 696,332 (33.1%) were also near-the-money. These formed 479,771 episodes with a mean best net profit of 0.001783 and a median duration of 0 s. All of the top 15 episodes belong to expiry 20260410, with best net profit of about 0.0032, close to that expiry's basis of 0.0034.

**After basis adjustment (Stage 6).**

| Expiry | Raw candidate rows | Adjusted candidate rows |
|---|---:|---:|
| 20260313 | 0 | 0 |
| 20260410 | 233,020 | 0 |
| 20260508 | 220,109 | 0 |
| 20260612 | 0 | 0 |
| 20260710 | 121,485 | 0 |
| 20260814 | 121,715 | 2 |
| 20260911 | 3 | 3 |

Five adjusted episodes remain, all of size 20 and duration 0 s. Four fall between 07:35:33.334309 and 07:35:33.334311 on 2026-04-08 (across two expiries); the fifth is at 07:40:06.725953.

| Expiry | Strike | Best net profit (price units) |
|---|---:|---:|
| 20260911 | 1.1725 | 0.000374 |
| 20260911 | 1.1800 | 0.000264 |
| 20260814 | 1.1725 | 0.000209 |
| 20260814 | 1.1775 | 0.000179 |
| 20260911 | 1.1750 | 0.000054 |

The simultaneity across two expiries and strikes suggests a common cause, plausibly a momentary futures quote. This reading is an inference. Adjusted deviations (99.9th percentile ≤ 1.95e-4) are well below the approximate round-trip cost of about 4.8e-4 (two option half-spreads of about 2e-4 each, one futures half-spread of about 4e-5, and three fees).

### 4.7 Lead-lag and repricing speed

**Stage 5 (4,770 futures-jump events; 854 + 3,655 + 261).**

| Latency (ms) | n | Edge | Placebo | Edge − placebo | Share > 0 | Net of fee | Net if closed | Half-spread |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 6,148 | −1.92e-4 | −2.09e-4 | +1.7e-5 | 1.07% | −2.04e-4 | −4.12e-4 | 2.08e-4 |
| 10 | 6,159 | −1.93e-4 | −2.08e-4 | +1.5e-5 | 1.09% | −2.05e-4 | −4.13e-4 | 2.08e-4 |
| 100 | 6,141 | −1.95e-4 | −2.08e-4 | +1.3e-5 | 1.01% | −2.07e-4 | −4.14e-4 | 2.08e-4 |
| 1,000 | 6,213 | −2.02e-4 | −2.08e-4 | +0.6e-5 | 0.47% | −2.14e-4 | −4.22e-4 | 2.08e-4 |

**Stage 6 (41,995 futures events; 6,715 + 32,705 + 2,575) — repricing speed.** Slope of option mid change on (delta × futures move); 1 means fully followed.

| Horizon | n | Slope | Correlation | Share of option mids unchanged |
|---:|---:|---:|---:|---:|
| 100 ms | 91,372 | 0.0485 | 0.155 | 88.2% |
| 1 s | 90,130 | 0.1218 | 0.229 | 71.6% |
| 10 s | 89,923 | 0.2721 | 0.193 | 33.4% |
| 60 s | 90,020 | 0.2899 | 0.112 | 5.5% |
| 300 s | 90,195 | 0.3604 | 0.074 | 1.1% |

**Stage 6 latency edge.**

| Latency (ms) | n | Edge | Placebo | Edge − placebo | Share > 0 | Net of fee | Net if closed |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 88,655 | −1.91e-4 | −2.07e-4 | +1.6e-5 | 0.55% | −2.03e-4 | −4.10e-4 |
| 10 | 88,397 | −1.91e-4 | −2.07e-4 | +1.6e-5 | 0.54% | −2.03e-4 | −4.10e-4 |
| 100 | 88,009 | −1.93e-4 | −2.07e-4 | +1.4e-5 | 0.46% | −2.05e-4 | −4.12e-4 |
| 1,000 | 87,993 | −1.98e-4 | −2.07e-4 | +0.9e-5 | 0.19% | −2.10e-4 | −4.17e-4 |

The half-spread was 2.07e-4. Binning by expected fair-value move gave no increase in edge for larger moves: the mean edge at 1 ms was −1.90e-4 for moves up to 1e-4 and −2.05e-4 for moves above 5e-4.

**Stage 9 (persistent futures moves).** 246 triggers (35 + 180 + 31), 6,843 option-trigger observations. No trigger reached 1.0e-3 (10 pips). Each cell is an average per trigger.

| Entry latency | Hold | n trig. | Edge | Placebo | Edge − placebo | Net hold | Std. error | t-stat (approx.) | Net if closed |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 ms | 5 s | 66 | −1.53e-4 | −1.95e-4 | +4.2e-5 | −1.65e-4 | 1.0e-5 | −16.5 | −3.73e-4 |
| 100 ms | 60 s | 63 | −1.65e-4 | −1.92e-4 | +2.7e-5 | −1.77e-4 | 2.8e-5 | −6.3 | −3.83e-4 |
| 100 ms | 300 s | 70 | −2.21e-4 | −1.98e-4 | −2.3e-5 | −2.33e-4 | 3.2e-5 | −7.3 | −4.39e-4 |
| 1,000 ms | 5 s | 62 | −1.77e-4 | −1.98e-4 | +2.1e-5 | −1.89e-4 | 0.9e-5 | −21.0 | −3.97e-4 |
| 1,000 ms | 60 s | 57 | −1.92e-4 | −1.95e-4 | +0.3e-5 | −2.04e-4 | 2.7e-5 | −7.6 | −4.11e-4 |
| 1,000 ms | 300 s | 65 | −2.29e-4 | −2.06e-4 | −2.3e-5 | −2.41e-4 | 3.4e-5 | −7.1 | −4.48e-4 |

Half-spread at entry was 1.95e-4 to 1.99e-4. The t-statistics are computed from the rounded standard errors, so they are approximate. Every cell has a 95% interval (net hold ± 2 standard errors) entirely below zero. Samples per cell are small (57 to 70 triggers), but the shortfall to break-even is far larger than the standard errors.

The by-absorption breakdown printed in this run is invalid because of a categorical-grouping bug, since fixed (Section 7), and is not used.

### 4.8 Supplementary notebook analyses reviewed

| Analysis | Reported result | Assessment |
|---|---|---|
| Event-based return correlation ("Stage 7") | Pearson 0.966 / 0.983 / 0.981 / 0.973 and Spearman 0.926 / 0.986 / 0.988 / 0.985 at horizons of 1 / 5 / 10 / 30 events; 1,322 rows | Not used. All 1,322 observations fall in hours 07 to 13 UTC (counts 38, 211, 182, 167, 239, 232, 253). The accompanying debug output shows a merged sample from a single option symbol on 2026-03-13 (the expiry date), and the correlation compares changes on the option's own update clock, so it is concurrent and not predictive. |
| Calendar-spread correlation ("Stage 8") | Pearson 0.471, Spearman 0.103, Kendall 0.085 on 31 one-minute bins | Not used. The "representative option" was `FCEU.S.MAR26.JUN26.SPD`, a futures calendar spread. |
| Student-t copula | ν = 2, ρ = −0.0476, Kendall τ = −0.0226, tail-dependence estimate 0.167; 8,951 observations in 4-hour bins | Not used. Calls and puts were pooled without sign adjustment, so near-zero rank correlation is uninformative. |

Spread cost as reported in the notebook's strategy table (Stage 7 sample only): half-spread as a fraction of option price was 0.0909 (tightest quartile), 0.1124, 0.1278 and 0.1648 (widest), against average absolute option moves of 0.052 to 0.061 and edge-to-cost ratios of 0.35 to 0.57.

---

## 5. Consolidated comparison of edge and cost

| Test | Measured gross effect | Cost it must beat | Result |
|---|---|---|---|
| Smile relative value | 0.00009 (vol units) | 0.00178 (round-trip, vol units) | Net −0.00168 |
| Imbalance | |mean 60 s mid change| ≤ 1.7e-6 | Half-spread ≈ 2.0e-4 | No effect |
| OFI (all rows) | |mean 60 s mid change| ≤ 1.6e-5 | Half-spread ≈ 2.0e-4 | ≤ 8% of half-spread |
| OFI (tight spreads) | up to 2.0e-5 | Half-spread ≈ 1.9e-4 | ≤ 11% of half-spread |
| Conversion/reversal (adjusted) | 5 rows, all with duration 0 s | About 4.8e-4 | No persistent opportunity |
| Lead-lag, Stage 5 / 6 | Edge above placebo: +0.6e-5 to +1.7e-5 | Half-spread ≈ 2.1e-4 | Net −2.0e-4 or worse |
| Persistent-move catch-up, Stage 9 | Edge above placebo: up to +4.2e-5 | Half-spread ≈ 2.0e-4 | Net −1.65e-4 to −2.41e-4 |

The median half-spread is 21 ticks of 1e-5 (about 2.1e-4). The largest measured predictive effect is about one-fifth of one half-spread, which is why no test is profitable after crossing the spread once.

---

## 6. Interpretation

1. **Costs dominate.** The effects found are real but small, and the spread is large in comparison.
2. **Option quote sizes are nearly constant**, so size-based microstructure features (imbalance, depth, OFI by size) have little to detect.
3. **Repricing speed depends on the trigger.** Single-update triggers (Stage 6) gave a slow average slope (0.05 at 100 ms rising to 0.36 at 300 s). Persistent-move triggers (Stage 9) show the information edge shrinking from +4.2e-5 at a 5-second hold to negative at 300 seconds. How much of each move the option had already absorbed before entry could not be established, because that breakdown was invalid in the run reported here.
4. **Serial-month options trade on a forward that differs from the future by roughly the carry between option expiry and future expiry.** Ignoring this produced a spurious arbitrage in the first conversion/reversal scan.

---

## 7. Corrections and open items

**Corrections made during the analysis**

- The first pipeline kept 821 "trades" with prices near 1.157. These were later identified as futures and spread trades, not option trades.
- The `price > 0` filter also removed negative-priced spread trades.
- Daily grouping used calendar date from `ts_recv`; the file has `session_date`, which is the appropriate key for session logic.
- The Stage 3 freshness filter (≤ 500 ms) was too strict for a full order book, because an unchanged best quote is still live. The earlier statement that the Stage 2 parity violations were "artefacts" was therefore stronger than the evidence allowed. The adjusted event-time conversion/reversal scan (Section 4.6) is the cleaner test.
- Stage 5's underlying mapping was incomplete for serial expiries (Section 4.5). The 479,771 conversion/reversal episodes were an artefact of that.
- Stage 9's by-absorption table used a categorical grouping that produced a Cartesian product of triggers and buckets, giving wrong counts and NaN rows. This is fixed in the script; the run reported here pre-dates the fix.

**Open items**

- The 114,224 box-scan candidates that survive size and fee filters (Section 4.4) were not investigated with the freshness filter removed.
- Contract specifications (venue, multiplier, exercise style, expiry time) have not been verified (Section 8).
- Data completeness has not been verified against the original Databento job (Section 2.6).
- The Stage 9 absorbed-fraction breakdown should be rerun with the corrected script.

---

## 8. Assumptions and limitations

**Assumptions (not read from the file)**

| Item | Value used | Used in |
|---|---|---|
| Per-leg fee | 1.2e-5 price units (about $1.50 per contract per leg on 125,000 EUR) | All net-of-fee results |
| Contract multiplier | 125,000 EUR | Any dollar figures (not reported above) |
| Option expiry time | 14:00 UTC on the expiry date | Time to expiry, delta, implied vol |
| Interest rate | 0 | Black-76 pricing and discounting |
| Delta volatility | 7% fixed | Lead-lag direction and expected-move filters |
| Exercise style | Treated as American; near-the-money restriction (|C − P| < 0.004) | Parity-based scans |

Fee and rate assumptions shift each result by roughly 1e-5 or less. The conclusions are insensitive to them because the spread is about 2e-4.

**Limitations**

- **Sample length:** 22 sessions. Strategies that depend on volatility risk premia or long horizons cannot be tested on this length of data.
- **Sample size in Stage 9:** 57 to 70 triggers per cell, because moves of at least 5 pips over 10 seconds are rare. None reached 10 pips.
- **No fill modelling:** only 4 option trades exist, so passive (market-making) strategies cannot be evaluated.
- **Data completeness:** the file may be filtered or sampled (Section 2.6). Conclusions apply to the file provided.
- **Synthetic-data validation only:** analysis scripts were verified for correctness on synthetic data. No real-time or out-of-sample trading test was performed.
- **Quote clock:** `ts_recv` (Databento receive time, microsecond precision in this file) was used. Cross-instrument comparisons below roughly 1 ms are not reliable because options and futures may arrive on different channels.
- **Spread-widening survivorship:** observations with an empty bid or ask at entry or mark were dropped, so the lead-lag tests exclude moments when market makers withdrew from one side.
- **No live execution:** all trades are assumed to execute at the displayed best quote, in size 1.

---

## 9. Conclusions and recommendations

1. In this dataset and under the stated assumptions, none of the five strategy families produced positive expected profit net of spread. The largest measured predictive effect was about 4.2e-5 against a half-spread of about 2e-4.
2. The one apparent arbitrage (conversion/reversal) was caused by a stable basis between serial-month options and the mapped future, and disappeared after adjustment.
3. Option size information is nearly constant, so depth and imbalance signals are not informative here.
4. **Before drawing wider conclusions:** verify venue and contract specifications from `publisher_id` and Databento metadata, and compare the file's record counts with the original download. Consider re-acquiring the full data with trades and the definitions schema.
5. Results for volatility-risk-premium or market-making strategies would require a longer sample and trade data respectively.

---

## Appendix A. Scripts

| Script | Purpose |
|---|---|
| `stage1_build_books.py` | MBO book reconstruction to best bid/ask and per-bar message counts |
| `stage2_option_signals.py` | Implied forward and vol, smile relative value, imbalance, box scan (grid) |
| `stage3_box_and_flow_checks.py` | Event-time box scan and OFI/imbalance deciles |
| `stage4_underlying_and_size_check.py` | Non-option symbol inventory, size distributions, trade counts |
| `stage5_underlying_tests.py` | Option-to-future mapping, conversion/reversal, first lead-lag test |
| `stage6_basis_adjusted_tests.py` | Basis estimation, adjusted scan, repricing speed, latency edge |
| `stage9_persistent_move_catchup.py` | Persistent-move catch-up test with per-trigger statistics |
| `risk_tools.py` | Corrected BEKK-GARCH, Rachev ratio and Expected Shortfall (not applied to results above) |

## Appendix B. Records per session

| Date | Records | Date | Records |
|---|---:|---|---:|
| 2026-03-09 | 3,538,149 | 2026-03-26 | 1,330,693 |
| 2026-03-10 | 2,078,564 | 2026-03-27 | 1,077,058 |
| 2026-03-11 | 1,803,767 | 2026-03-30 | 1,002,420 |
| 2026-03-12 | 1,609,274 | 2026-03-31 | 1,014,030 |
| 2026-03-13 | 1,487,531 | 2026-04-01 | 1,504,902 |
| 2026-03-16 | 2,096,273 | 2026-04-02 | 1,301,488 |
| 2026-03-17 | 1,090,903 | 2026-04-06 | 7,821 |
| 2026-03-18 | 1,156,335 | 2026-04-07 | 1,094,452 |
| 2026-03-19 | 2,385,698 | 2026-04-08 | 1,747,865 |
| 2026-03-20 | 1,377,645 | | |
| 2026-03-23 | 2,122,602 | | |
| 2026-03-24 | 2,291,619 | | |
| 2026-03-25 | 1,926,246 | | |

Total: 35,045,335 records over 22 sessions.

---

## Supplemental validation from the attached analysis (`Pasted text #1`)

This supplemental section is included as an external validation layer for the project report. The raw dataset and the repo's own generated outputs remain the canonical evidence; the figures below are treated as a cross-check against the Databento MBO notebook/attachment and are not treated as independent project-generated results unless reproduced by the repo pipeline.

### Executive summary from the attached analysis

| Finding | Key figure |
|---|---|
| Record composition is dominated by book-maintenance events, not trading | 17,515,881 adds, 17,516,289 cancels, 53 modifies; only 996 trades and 867 fills |
| Explicit option trades are nearly absent | 4 option trades in the trade sample |
| Best-level order size is almost fixed | 93.6% of best-level sizes equal exactly 20; imbalance is exactly 0 in 99.97% of quote observations |
| Spreads are large relative to microstructure signals | median half-spread ≈ 21 ticks of 1e-5; decile means about 1.99e-4 to 2.19e-4 |
| The futures underlying is present in the feed | FCEU futures used for mapping; option-to-future basis is material for serial expiries |
| Conversion/reversal edges disappear once basis is adjusted | 479,771 apparent episodes collapse to 5 adjusted candidates after basis correction |
| Lead-lag repricing is present but too small versus cost | edge above placebo ≈ +4.2e-5 at best against half-spread ≈ 1.96e-4 |
| No tested strategy survives spread cost | persistent-move test net P&L at −1.65e-4 to −2.41e-4 per trade |

### Summary of the external validation

- The raw feed is a Databento MBO file with 35,045,335 rows and 17 columns.
- `rtype` is 160 throughout the file, confirming the market-by-order nature of the stream.
- The file spans 22 sessions from 2026-03-09 to 2026-04-08, with a partial/holiday session on 2026-04-06 and no rows on 2026-04-03.
- The raw action counts are consistent with a real order book: Adds and cancels dominate, while trade events are sparse.
- The best-level quote data show near-constant order size and almost zero imbalance, which explains why size-based and order-flow features have very weak predictive power.
- The option basis to the mapped futures remains economically meaningful for serial expiries, and a first-pass conversion/reversal scan generated a large number of apparent profits that vanished once the correct underlying basis was accounted for.
- In the lead-lag analysis, option mid prices reprice toward futures moves, but the measured effect is far smaller than the spread cost. The implied edge is not enough to survive transaction costs, even before considering model uncertainty or execution slippage.

### Supplemental interpretation from the attachment

1. **Costs dominate.** The effects found are real but small, and the spread is large in comparison.
2. **Option quote sizes are nearly constant.** This makes size-based microstructure features and imbalance metrics largely uninformative.
3. **Repricing speed depends on the trigger.** Single-update triggers show weak slope/lag effects; persistent-move triggers show the signal shrinking and becoming negative at longer horizons.
4. **Serial-month option basis is a real adjustment issue.** Ignoring it creates a spurious dollar wedge in conversion/reversal scans and leads to false-positive apparent arbitrage.
5. **The project conclusion remains conservative.** On the file as provided, no strategy produced positive expected profit net of spread. The report therefore retains the reject decision and treats the stronger external validation as a confirmation of the same practical conclusion.

These findings reinforce the repo's own EDA and strategy conclusions: the file behaves like a genuine MBO options stream, the book reconstruction is essential, and the strategy edge is not large enough to offset the cost of crossing the spread.

---