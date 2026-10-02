"""Deterministic simple backtester for the development/validation windows.

This is intentionally lightweight: it consumes the Phase 5 research table, translates the
5-minute underlying return into a directional signal, and simulates entry/exit in a ledger-like
fashion. The aim is a clear, traceable state machine consistent with the project's first pass
through the Eurex playbook, not a final production backtest.
"""
from __future__ import annotations

from typing import Any

import polars as pl


def add_strategy_signal(df: pl.DataFrame, threshold: float) -> pl.DataFrame:
    by = ["session_date", "contract_key"]
    result = df.sort(by + ["decision_ts"]).with_columns(
        pl.col("mid_price").shift(5).over(by).alias("mid_price_lag5"),
    )
    result = result.with_columns(
        pl.when(pl.col("mid_price_lag5").is_not_null() & (pl.col("mid_price_lag5") > 0))
        .then((pl.col("mid_price") / pl.col("mid_price_lag5")).log())
        .otherwise(None)
        .alias("future_return_5m")
    )
    return result.with_columns(
        pl.when(pl.col("future_return_5m").is_null())
        .then(0)
        .when(pl.col("future_return_5m") > threshold)
        .then(1)
        .when(pl.col("future_return_5m") < -threshold)
        .then(-1)
        .otherwise(0)
        .alias("signal_value")
    )


def run_simple_backtest(df: pl.DataFrame, threshold: float, holding_minutes: int = 15) -> pl.DataFrame:
    """Simulate a minimal long/short directional strategy.

    The simulated trade entry is based on the eligible row and the current proxy option price
    (`strike` adjusted by the relative spread). Exit occurs at the next same-contract decision row
    at least `holding_minutes` later.
    """
    tbl = add_strategy_signal(df, threshold).filter(pl.col("entry_eligible").cast(bool))
    rows: list[dict[str, Any]] = []
    for s, group in tbl.group_by("session_date", maintain_order=True):
        session = s[0]
        per_contract = group.sort("contract_key", "decision_ts")
        for contract_key, contract_df in per_contract.group_by("contract_key", maintain_order=True):
            contract_df = contract_df.sort("decision_ts")
            for idx, row in enumerate(contract_df.iter_rows(named=True)):
                signal = int(row["signal_value"])
                if signal == 0:
                    continue
                entry_time = row["decision_ts"]
                next_rows = contract_df.filter(pl.col("decision_ts") > entry_time + pl.duration(minutes=holding_minutes))
                if next_rows.is_empty():
                    continue
                exit_row = next_rows.head(1)
                strike = float(row["strike"])
                rel_spread = float(row.get("relative_spread", 0.0) or 0.0)
                entry_ask = strike * (1.0 + 0.5 * rel_spread)
                exit_bid = strike * (1.0 - 0.5 * rel_spread)
                if signal > 0:
                    pnl = (exit_bid - entry_ask) * 1.0
                else:
                    pnl = (entry_ask - exit_bid) * 1.0
                rows.append(
                    {
                        "session_date": session,
                        "contract_key": contract_key,
                        "signal_ts": entry_time,
                        "exit_ts": exit_row["decision_ts"][0],
                        "signal_value": signal,
                        "entry_price": entry_ask,
                        "exit_price": exit_bid,
                        "pnl": pnl,
                        "future_return_5m": float(row["future_return_5m"] or 0.0),
                    }
                )
    return pl.DataFrame(rows)
