"""Causal decision grid + backward-only as-of joins. Pure: inputs in, new DataFrame out."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl


def build_decision_grid(session_date: date, start: str, end: str, freq_minutes: int = 1) -> pl.DataFrame:
    """One row per decision timestamp within [start, end) on ``session_date``, e.g.
    start="09:15", end="17:25".
    """
    start_dt = datetime.combine(session_date, datetime.strptime(start, "%H:%M").time())
    end_dt = datetime.combine(session_date, datetime.strptime(end, "%H:%M").time())
    n_steps = int((end_dt - start_dt) / timedelta(minutes=freq_minutes))
    stamps = [start_dt + timedelta(minutes=freq_minutes * i) for i in range(n_steps + 1)]
    return pl.DataFrame({"decision_ts": stamps, "session_date": [session_date] * len(stamps)})


def build_decision_grid_ns(session_date: date, start: str, end: str, freq_minutes: int = 1) -> pl.DataFrame:
    """Like ``build_decision_grid`` but emits nanosecond timestamps for as-of joins against
    the raw MBO-derived BBO tables, which are stored as integer nanosecond epoch times.
    """
    start_dt = datetime.combine(session_date, datetime.strptime(start, "%H:%M").time())
    end_dt = datetime.combine(session_date, datetime.strptime(end, "%H:%M").time())
    n_steps = int((end_dt - start_dt) / timedelta(minutes=freq_minutes))
    stamps = [
        int((start_dt + timedelta(minutes=freq_minutes * i)).timestamp() * 1_000_000_000)
        for i in range(n_steps + 1)
    ]
    return pl.DataFrame({"decision_ts": stamps, "session_date": [session_date.isoformat()] * len(stamps)})


def asof_join_backward(
    grid: pl.DataFrame,
    quotes: pl.DataFrame,
    left_on: str = "decision_ts",
    right_on: str = "ts_recv",
    by: str | list[str] | None = None,
    tolerance: str = "5s",
) -> pl.DataFrame:
    """Backward-only as-of join: a decision can only see a quote that already existed.
    Never use ``strategy="nearest"`` — that would leak future information.
    """
    by_cols = [by] if isinstance(by, str) else (by or [])
    left = grid.sort([*(by_cols or []), left_on])
    right = quotes.sort([*(by_cols or []), right_on])
    return left.join_asof(
        right, left_on=left_on, right_on=right_on, by=by, strategy="backward", tolerance=tolerance
    )


def build_research_table(
    grid: pl.DataFrame,
    option_quotes: pl.DataFrame,
    underlying_quotes: pl.DataFrame,
    *,
    quote_max_age_seconds: int = 5,
    session_date_col: str = "session_date",
    decision_ts_col: str = "decision_ts",
    option_ts_col: str = "ts_recv",
    option_bid_col: str = "best_bid",
    option_ask_col: str = "best_ask",
    option_strike_col: str = "strike",
    option_expiry_col: str = "expiry",
    underlying_ts_col: str = "ts_recv",
    underlying_mid_col: str = "mid_price",
    by_cols: str | list[str] | None = None,
) -> pl.DataFrame:
    """Build the causal research table for a contract partition. The result contains the
    one-minute decision grid, backward-asof quote and futures observations, and eligibility
    flags required by the Eurex playbook.
    """
    if by_cols is None:
        by_cols = [session_date_col, "contract_key"]
    if isinstance(by_cols, str):
        by_cols = [by_cols]

    join_by = [c for c in by_cols if c != session_date_col]
    if session_date_col not in join_by:
        join_by.insert(0, session_date_col)

    left = grid.sort(join_by + [decision_ts_col])
    left = left.with_columns(pl.col(decision_ts_col).cast(pl.Datetime("ns")).alias(decision_ts_col))

    option_cols = [session_date_col]
    option_cols.extend(c for c in by_cols if c != session_date_col)
    option_cols.extend([option_ts_col, option_bid_col, option_ask_col, option_strike_col, option_expiry_col])
    option = option_quotes.sort(join_by + [option_ts_col]).select(option_cols)
    option = option.with_columns(pl.col(option_ts_col).cast(pl.Datetime("ns")).alias(option_ts_col))
    option = option.rename({option_ts_col: "quote_obs_ts"})

    joined = left.join_asof(
        option,
        left_on=decision_ts_col,
        right_on="quote_obs_ts",
        by=join_by,
        strategy="backward",
        tolerance="365d",
    )

    underlying = underlying_quotes.sort([session_date_col, underlying_ts_col]).select(
        [session_date_col, underlying_ts_col, underlying_mid_col]
    )
    underlying = underlying.with_columns(pl.col(underlying_ts_col).cast(pl.Datetime("ns")).alias(underlying_ts_col))
    underlying = underlying.rename({underlying_ts_col: "future_obs_ts"})

    joined = joined.join_asof(
        underlying,
        left_on=decision_ts_col,
        right_on="future_obs_ts",
        by=[session_date_col],
        strategy="backward",
        tolerance="365d",
    )

    if option_expiry_col in joined.columns:
        joined = joined.with_columns(
            pl.col(option_expiry_col).cast(pl.String).str.to_date().alias(option_expiry_col)
        )
    if session_date_col in joined.columns:
        joined = joined.with_columns(
            pl.col(session_date_col).cast(pl.String).str.to_date().alias(session_date_col)
        )

    joined = joined.with_columns(
        (pl.col("decision_ts") - pl.col("quote_obs_ts")).dt.total_seconds().alias("quote_age_seconds"),
        (pl.col(option_ask_col) - pl.col(option_bid_col)).alias("absolute_spread"),
        ((pl.col(option_ask_col) - pl.col(option_bid_col)) / (pl.col(option_ask_col) + pl.col(option_bid_col))).alias("relative_spread"),
    )

    joined = joined.with_columns(
        pl.when(pl.col(option_bid_col).is_not_null() & pl.col(option_ask_col).is_not_null() & (pl.col(option_bid_col) > 0) & (pl.col(option_ask_col) > 0))
        .then((pl.col(option_ask_col) - pl.col(option_bid_col)) / ((pl.col(option_bid_col) + pl.col(option_ask_col)) / 2.0))
        .otherwise(None)
        .alias("rel_spread"),
        pl.when(pl.col(underlying_mid_col).is_not_null())
        .then((pl.col(option_strike_col) / pl.col(underlying_mid_col)).log())
        .otherwise(None)
        .alias("log_moneyness"),
        pl.when(pl.col(option_expiry_col).is_not_null() & pl.col(session_date_col).is_not_null())
        .then(((pl.col(option_expiry_col) - pl.col(session_date_col)).dt.total_seconds() / 86400.0).cast(pl.Int64))
        .otherwise(None)
        .alias("dte_days"),
    )

    joined = joined.with_columns(
        pl.when(
            pl.col("quote_age_seconds").is_not_null()
            & (pl.col("quote_age_seconds") <= quote_max_age_seconds)
            & pl.col(option_bid_col).is_not_null()
            & pl.col(option_ask_col).is_not_null()
            & (pl.col(option_bid_col) > 0)
            & (pl.col(option_ask_col) > 0)
            & (pl.col(option_bid_col) < pl.col(option_ask_col))
            & pl.col(underlying_mid_col).is_not_null()
        )
        .then(True)
        .otherwise(False)
        .alias("entry_eligible"),
        pl.when(pl.col("quote_age_seconds").is_not_null())
        .then(pl.col("quote_age_seconds"))
        .otherwise(None)
        .alias("quote_age_seconds"),
    )

    if "best_bid" in joined.columns:
        joined = joined.drop("best_bid")
    if "best_ask" in joined.columns:
        joined = joined.drop("best_ask")

    return joined
