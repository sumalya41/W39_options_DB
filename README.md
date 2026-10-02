<div align="center">

# 📊 Databento Options Research Project

**A bounded, causal research project for Euro FX options on the raw MBO feed**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)](https://python.org)
[![Data](https://img.shields.io/badge/Data-Databento%20MBO-6C5CE7)](https://databento.com)
[![Seed](https://img.shields.io/badge/Seed-42-00B894)](#seeds-and-determinism)
[![Status](https://img.shields.io/badge/Outcome-Strategy%20Rejected%20✓-E17055)](#validation-status)
[![Tests](https://img.shields.io/badge/Tests-6%20passed-brightgreen?logo=pytest)](#validation-status)

</div>

---

> [!IMPORTANT]
> *This repository is intentionally structured to **reject a weak signal** rather than to force a strategy into the test split.*

A bounded, causal research project for a simple Databento MBO options strategy on the raw Euro FX options feed. The project follows a frozen session-split workflow, keeps raw data immutable, and uses return-based logic throughout.

---

## 🎯 Project Goal

Evaluate whether a simple near-the-money option signal based on short-horizon underlying return continuation has a valid causal edge.

> **The final decision was to reject the strategy after development and validation evidence, leaving the test split untouched.**

---

## 📦 Data-Source Clarification

The raw parquet (`data/databento_options_clean.parquet`) and the notebook-based schema check in `databento-options (1).ipynb` confirm the same MBO footprint:

<details>
<summary><b>Click to expand MBO footprint fields</b></summary>

| Field | Field | Field | Field |
|:------|:------|:------|:------|
| `ts_recv` | `ts_event` | `ts_index` | `price` |
| `rtype` | `publisher_id` | `instrument_id` | `action` |
| `side` | `size` | `channel_id` | `order_id` |
| `flags` | `ts_in_delta` | `sequence` | `symbol` |
| `session_date` | | | |

</details>

The feed is a Databento MBO options dataset (Euro FX options / futures) rather than a ready-made BBO feed; **the book must be reconstructed from order events.**

---

## 🔐 Raw Data and Policy

| Policy | Detail |
|:-------|:-------|
| 📁 **Raw dataset** | `data/databento_options_clean.parquet` |
| 🔒 **Immutability** | Raw data are read-only and never modified in place |
| 📂 **Derived outputs** | Written under `data/derived/` and `outputs/` only |
| ⚙️ **Configuration** | Frozen configuration in `config/config.yaml`, operates by whole-session splits |

---
## 📂 Repository Structure
The codebase enforces a strict functional architecture. The core logic lives in src/options_research/ as pure functions with no mutable state, while scripts/ serves as a thin driver layer.
'''
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
'''

---
## 🏗️ Run Order

The project executes as a deterministic 10-phase pipeline:

| Phase | Script | Description |
|:-----:|:-------|:------------|
| `01` | `scripts/phase01_inventory.py` | Inventory, schema checks, symbol parsing, null counts |
| `02` | `scripts/phase02_contracts.py` | Contract identity, option/future mapping hypothesis, coverage table |
| `03` | `scripts/phase03_cleaning_audit.py` | Cleaning-log audit for partial sessions, reversals, data issues |
| `04` | `scripts/phase04_bbo.py` | Replay MBO stream into top-of-book output |
| `05` | `scripts/phase05_research.py` | Causal decision-grid construction and research table |
| `06` | `scripts/phase06_eda.py` | EDA summary, spread distribution, entry-pass analysis |
| `07` | `scripts/phase07_hypothesis.py` | Freeze and evaluate development hypothesis |
| `08` | `scripts/phase08_backtester.py` | Minimal strategy backtest on development + validation |
| `09` | `scripts/phase09_validation.py` | Validation, benchmark, robustness, and final decision |
| `10` | `scripts/phase10_verification.py` | Causality and ledger verification checks |

---

## ⚙️ Configuration Notes

The canonical config is `config/config.yaml`.

> The development threshold is frozen from the development-return distribution and is **not** hard-coded in the final backtest logic.

### Session Splits

| Split | Range |
|:------|:------|
| 🚫 `exclude_sessions` | `2026-04-06` *(partial/holiday session)* |
| 🔵 `splits.development` | `2026-03-09` → `2026-03-25` |
| 🟡 `splits.validation` | `2026-03-26` → `2026-03-31` |
| 🔴 `splits.test` | `2026-04-01` → `2026-04-08` *(unopened)* |

### Signal & Strategy Parameters

| Parameter | Value |
|:----------|:------|
| `decision_grid_minutes` | `1` |
| `signal.lookback_minutes` | `5` |
| `max_abs_log_moneyness` | `0.02` |
| `max_rel_spread` | `0.05` |
| `quote_max_age_seconds` | `5` |
| `underlying_max_age_seconds` | `5` |
| `holding_minutes` | `15` |
| `initial_cash` | `100,000` |
| `seed` | `42` |

---

## 🧪 Validation Status

### Targeted Verification

```
pytest tests/test_phase10_verification.py -q → 6 passed ✅
```

### Project Outcome

| Phase | Result | Status |
|:------|:-------|:------:|
| Phase 7 — Signal | Rejected as non-surviving on development data | ❌ |
| Phase 8 — Backtest | Remained negative on development + validation data | ❌ |
| **Final Decision** | **Reject the strategy · Test split unopened** | 🛑 |

---

## 🔒 Seeds and Determinism

- **Seed:** `42`
- The frozen config and all phase scripts use deterministic logic without hidden mutable state.
- Any development-only threshold or test gate must be fixed **before** validation/test windows are touched.

---

## 🤖 AI-Use Statement

> This project used AI assistance as a coding implementation, but every analytical decision and numerical conclusion was checked against generated project artifacts  The final report uses only values originating from the generated facts, figures, and ledger outputs in the project. No numerical claim in the report was invented beyond those generated artifacts.

---

## 📦 Dependency Versions

<details>
<summary><b>Click to expand full dependency list</b> (<code>pip freeze</code>)</summary>

| Package | Version | Package | Version |
|:--------|:--------|:--------|:--------|
| `databento` | 0.87.0 | `databento-dbn` | 0.70.0 |
| `duckdb` | 1.5.6 | `exchange_calendars` | 4.13.2 |
| `hypothesis` | 6.168.3 | `matplotlib` | 3.11.2 |
| `numba` | 0.68.0 | `numpy` | 2.5.3 |
| `pandas` | 3.0.6 | `polars` | 1.44.2 |
| `pyarrow` | 25.0.1 | `pydantic` | 2.13.5 |
| `PyYAML` | 6.0.3 | `scipy` | 1.18.1 |
| `seaborn` | 0.13.2 | `statsmodels` | 0.15.0 |
| `pytest` | 9.1.1 | `tqdm` | 4.70.1 |
| `psutil` | 7.2.2 | `py-vollib` | 1.0.12 |

</details>

---

## 📜 License

This project is licensed under the **MIT License**.

```
MIT License

Copyright (c) 2026 Databento Options Research Project

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

<div align="center">

*Built with rigor. Rejected with evidence. Test split unopened.* 🎯

</div>
