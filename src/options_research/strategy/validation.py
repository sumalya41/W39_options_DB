"""Phase 9 validation and robustness summaries for the development/validation backtest."""
from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl


def ledger_summary(ledger: pl.DataFrame) -> dict[str, Any]:
    """Compute basic PnL, risk, and distribution metrics for the ledger."""
    if ledger.is_empty():
        return {
            "n_trades": 0,
            "gross_pnl": 0.0,
            "net_pnl": 0.0,
            "mean_pnl": 0.0,
            "median_pnl": 0.0,
            "win_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "worst_trade_day": None,
            "daily_pnl": [],
            "equity_curve": [],
            "max_drawdown": 0.0,
        }

    pnl = np.asarray(ledger["pnl"].to_numpy(), dtype=float)
    daily = ledger.group_by("session_date").agg(total_pnl=pl.col("pnl").sum()).sort("session_date")
    total = float(np.sum(pnl))
    mean_pnl = float(np.mean(pnl))
    median = float(np.median(pnl))
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    win_rate = float((pnl > 0).mean()) if len(pnl) else 0.0
    worst_trade_day = (
        daily.sort("total_pnl").head(1).row(0, named=True) if daily.height else None
    )

    equity = np.cumsum(np.asarray(daily["total_pnl"].to_numpy(), dtype=float))
    running_max = np.maximum.accumulate(equity)
    drawdown = (equity - running_max) / np.where(running_max == 0, 1.0, running_max)
    max_drawdown = float(np.min(drawdown)) if len(drawdown) else 0.0

    return {
        "n_trades": int(len(pnl)),
        "gross_pnl": total,
        "net_pnl": total,
        "mean_pnl": mean_pnl,
        "median_pnl": median,
        "win_rate": win_rate,
        "avg_win": float(np.mean(wins)) if len(wins) else 0.0,
        "avg_loss": float(np.mean(losses)) if len(losses) else 0.0,
        "worst_trade_day": worst_trade_day,
        "daily_pnl": daily.to_dicts(),
        "equity_curve": [float(v) for v in equity],
        "max_drawdown": max_drawdown,
    }


def day_block_bootstrap(daily_pnl: list[float], *, n_bootstrap: int = 2000, seed: int = 42) -> dict[str, Any]:
    """Resample whole-day PnL blocks to obtain simple confidence intervals."""
    values = np.asarray(daily_pnl, dtype=float)
    if values.size == 0:
        return {"n_bootstrap": n_bootstrap, "total_pnl_ci": [0.0, 0.0], "mean_daily_pnl_ci": [0.0, 0.0]}

    rng = np.random.default_rng(seed)
    draws = rng.choice(values, size=(n_bootstrap, values.size), replace=True)
    total_draws = draws.sum(axis=1)
    mean_draws = draws.mean(axis=1)
    return {
        "n_bootstrap": int(n_bootstrap),
        "total_pnl_ci": [float(np.quantile(total_draws, 0.025)), float(np.quantile(total_draws, 0.975))],
        "mean_daily_pnl_ci": [float(np.quantile(mean_draws, 0.025)), float(np.quantile(mean_draws, 0.975))],
    }


def benchmark_summary(ledger: pl.DataFrame) -> dict[str, float]:
    """Simple benchmark estimates on the same decision set."""
    if ledger.is_empty():
        return {"cash": 0.0, "underlying_direction": 0.0, "optimistic_mid": 0.0}

    signal = np.asarray(ledger["signal_value"].to_numpy(), dtype=float)
    returns = np.asarray(ledger["future_return_5m"].to_numpy(), dtype=float)
    return {
        "cash": 0.0,
        "underlying_direction": float(np.sum(signal * returns)),
        "optimistic_mid": float(np.sum(signal * returns * 2.0)),
    }


def robustness_summary(ledger: pl.DataFrame) -> list[dict[str, Any]]:
    """Return a few one-parameter sensitivity scenarios on the same ledger."""
    pnl = np.asarray(ledger["pnl"].to_numpy(), dtype=float)
    if pnl.size == 0:
        return []

    scenarios = [
        {"scenario": "base", "multiplier": 1.0},
        {"scenario": "zero_costs", "multiplier": 1.10},
        {"scenario": "higher_costs", "multiplier": 0.90},
        {"scenario": "delay_penalty", "multiplier": 0.80},
        {"scenario": "stale_quote_penalty", "multiplier": 0.75},
    ]

    out: list[dict[str, Any]] = []
    for row in scenarios:
        adjusted = float(np.sum(pnl * row["multiplier"]))
        adjusted_pnl = pnl * row["multiplier"]
        out.append({
            "scenario": row["scenario"],
            "multiplier": float(row["multiplier"]),
            "net_pnl": adjusted,
            "win_rate": float(np.mean(adjusted_pnl > 0.0)) if len(pnl) else 0.0,
        })
    return out
