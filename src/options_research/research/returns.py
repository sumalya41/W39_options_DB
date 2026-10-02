"""Return-based feature construction (Rule 3): every feature derived from a price series is
a log return, never a raw level. LazyFrame in, new LazyFrame out — no mutation.
"""
from __future__ import annotations

from typing import Sequence

import numpy as np
import polars as pl


def simple_return(prices: Sequence[float]) -> np.ndarray:
    """Simple gross return: (P_t / P_{t-1}) - 1."""
    arr = np.asarray(prices, dtype=float)
    if arr.size < 2:
        return np.empty(0, dtype=float)
    return arr[1:] / arr[:-1] - 1.0


def cumulative_return(returns: Sequence[float]) -> np.ndarray:
    """Compounded return from a sequence of simple returns."""
    arr = np.asarray(returns, dtype=float)
    if arr.size == 0:
        return np.empty(0, dtype=float)
    if arr.size == 1:
        return arr.copy()
    return np.cumprod(1.0 + arr) - 1.0


def with_log_return(lf: pl.LazyFrame, price_col: str, out_col: str = "log_return", by: str | None = None) -> pl.LazyFrame:
    """Adds ``out_col = log(price_col / price_col.shift(1))``, optionally partitioned by ``by``
    (e.g. contract_key or session_date) so returns never bridge an unrelated series.
    """
    if by is None:
        expr = (pl.col(price_col) / pl.col(price_col).shift(1)).log()
    else:
        expr = (pl.col(price_col) / pl.col(price_col).shift(1).over(by)).log()
    return lf.with_columns(expr.alias(out_col))


def with_simple_return(lf: pl.LazyFrame, price_col: str, out_col: str = "simple_return", by: str | None = None) -> pl.LazyFrame:
    """Adds simple gross return, optionally partitioned by ``by``."""
    if by is None:
        expr = (pl.col(price_col) / pl.col(price_col).shift(1)) - 1.0
    else:
        expr = (pl.col(price_col) / pl.col(price_col).shift(1).over(by)) - 1.0
    return lf.with_columns(expr.alias(out_col))


def with_cumulative_return(lf: pl.LazyFrame, simple_return_col: str, out_col: str = "cumulative_return") -> pl.LazyFrame:
    """Adds the cumulative compounded return across a simple-return series."""
    expr = (1.0 + pl.col(simple_return_col)).cumprod().sub(1.0)
    return lf.with_columns(expr.alias(out_col))


def with_log_moneyness(lf: pl.LazyFrame, strike_col: str, underlying_col: str, out_col: str = "log_moneyness") -> pl.LazyFrame:
    """log(K / S) — a ratio, already scale-free; still not a raw price level regression input."""
    return lf.with_columns((pl.col(strike_col) / pl.col(underlying_col)).log().alias(out_col))
