from __future__ import annotations

import json

import polars as pl

from options_research.strategy.verification import (
    check_real_development_trade_examples,
    round_trip_pnl,
    verify_causality,
    verify_contract_stays_fixed,
    verify_missing_exit_unresolved,
    verify_no_future_quote_in_asof_join,
)


def test_causality_check_passes_for_valid_window():
    df = pl.DataFrame(
        {
            "decision_ts": [10, 20, 30],
            "quote_obs_ts": [9, 19, 29],
            "fill_ts": [12, 23, 33],
        }
    )
    result = verify_causality(df, decision_ts_col="decision_ts", quote_ts_col="quote_obs_ts", fill_ts_col="fill_ts", delay_seconds=1)
    assert result["passed"] is True
    assert result["future_quote_violations"] == 0
    assert result["fill_delay_violations"] == 0


def test_future_quote_guard_blocks_future_observations():
    df = pl.DataFrame({"decision_ts": [10, 20], "future_obs_ts": [8, 19]})
    result = verify_no_future_quote_in_asof_join(df, decision_ts_col="decision_ts", future_ts_col="future_obs_ts")
    assert result["passed"] is True
    assert result["future_quote_violations"] == 0


def test_contract_lock_while_held():
    df = pl.DataFrame(
        {
            "trade_id": [1, 1],
            "contract_key": ["EUCO|20260316|C|1.15000", "EUCO|20260316|C|1.15000"],
        }
    )
    result = verify_contract_stays_fixed(df)
    assert result["passed"] is True
    assert result["contract_swaps"] == 0


def test_round_trip_loses_spread_plus_fee():
    pnl = round_trip_pnl(2.20, 2.15, fee=0.01)
    assert abs(pnl - (-0.06)) < 1e-12


def test_missing_exit_stays_unresolved():
    df = pl.DataFrame({"exit_ts": [None, "2026-03-09 08:40:00"], "unresolved": [True, False]})
    result = verify_missing_exit_unresolved(df)
    assert result["passed"] is True
    assert result["unresolved_rows"] == 1


def test_real_phase8_trade_examples_are_reproduced():
    facts = json.loads(open("outputs/phase08/facts_step08.json", "r", encoding="utf-8").read())
    result = check_real_development_trade_examples(facts["sample_rows"])
    assert result["passed"] is True
    assert len(result["checks"]) == 3
