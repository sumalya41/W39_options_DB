"""EDA aggregations: DataFrame in, small summary DataFrame out. Plotting itself (a true
side effect) belongs in a thin driver script, never in these functions.
"""
from __future__ import annotations

import polars as pl


def spread_distribution(df: pl.DataFrame, bid_col: str = "best_bid", ask_col: str = "best_ask") -> pl.DataFrame:
    mid = (pl.col(bid_col) + pl.col(ask_col)) / 2
    spread = pl.col(ask_col) - pl.col(bid_col)
    rel_spread_bps = (spread / mid * 1e4).alias("rel_spread_bps")
    return (
        df.filter(pl.col(bid_col) > 0, pl.col(ask_col) > pl.col(bid_col))
        .with_columns(rel_spread_bps)
        .select(pl.col("rel_spread_bps").quantile(q) for q in (0.25, 0.5, 0.75, 0.9, 0.99))
    )


def coverage_by_day(df: pl.DataFrame, date_col: str = "session_date") -> pl.DataFrame:
    return df.group_by(date_col).agg(n_rows=pl.len()).sort(date_col)


def coverage_summary(df: pl.DataFrame, date_col: str = "session_date") -> pl.DataFrame:
    return (
        df.group_by(date_col)
        .agg(rows=pl.len(), eligible=pl.col("entry_eligible").sum())
        .with_columns(pass_rate=(pl.col("eligible") / pl.col("rows")).round(4))
        .sort(date_col)
    )


def spread_bps_summary(df: pl.DataFrame, spread_col: str = "relative_spread") -> pl.DataFrame:
    if spread_col not in df.columns:
        raise ValueError(f"Column {spread_col!r} not found; expected a relative-spread ratio or equivalent.")
    return (
        df.filter(pl.col(spread_col).is_not_null(), pl.col(spread_col) >= 0)
        .with_columns((pl.col(spread_col) * 1_0000).alias("spread_bps"))
        .select(
            p25=pl.col("spread_bps").quantile(0.25),
            p50=pl.col("spread_bps").quantile(0.50),
            p75=pl.col("spread_bps").quantile(0.75),
            p90=pl.col("spread_bps").quantile(0.90),
            p99=pl.col("spread_bps").quantile(0.99),
        )
    )


def entry_pass_rate_by_contract(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("contract_key")
        .agg(rows=pl.len(), eligible=pl.col("entry_eligible").sum())
        .with_columns(pass_rate=(pl.col("eligible") / pl.col("rows")).round(4))
        .sort("rows", descending=True)
    )


def moneyness_summary(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.filter(pl.col("log_moneyness").is_not_null())
        .with_columns(abs_log_moneyness=pl.col("log_moneyness").abs())
        .group_by_dynamic("decision_ts", every="1d", closed="both")
        .agg(avg_abs_log_moneyness=pl.col("abs_log_moneyness").mean())
    )
