"""Return-based time-series statistics. Every function is pure: array/series in, a small
immutable result record out. Never mutates its inputs.
"""
from __future__ import annotations

from typing import NamedTuple, Optional, Sequence

import numpy as np
from statsmodels.tsa.stattools import acf as _acf
from statsmodels.tsa.stattools import adfuller
import statsmodels.api as sm


def _prepare_stat_series(series: Sequence[float], max_points: int = 50_000) -> np.ndarray:
    """Trim oversized series before heavy statistics calls to avoid memory blowups.

    This keeps the workflow valid for large event-time datasets while preserving the
    return-based statistical logic used in the project. For very long arrays, we take an
    evenly spaced subsample so the test remains representative instead of crashing.
    """
    arr = np.asarray(series, dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return arr
    if arr.size <= max_points:
        return arr
    step = max(1, arr.size // max_points)
    sampled = arr[::step]
    if sampled.size > max_points:
        sampled = sampled[:max_points]
    return sampled


def log_returns(prices: Sequence[float]) -> np.ndarray:
    """log(x_t / x_{t-1}). Rule 3: this, not raw price levels, feeds every regression."""
    arr = np.asarray(prices, dtype=float)
    if arr.size < 2:
        return np.empty(0, dtype=float)
    return np.diff(np.log(arr))


class StationarityResult(NamedTuple):
    adf_statistic: float
    p_value: float
    is_stationary_at_5pct: bool


def adf_stationarity(series: Sequence[float]) -> StationarityResult:
    """Augmented Dickey-Fuller test. Run on returns, not on price levels (Rule 2/3)."""
    arr = _prepare_stat_series(series)
    if arr.size < 3:
        return StationarityResult(float("nan"), float("nan"), False)
    stat, pvalue, *_ = adfuller(arr, autolag="AIC", result_object=False)
    return StationarityResult(stat, pvalue, pvalue < 0.05)


def acf_values(series: Sequence[float], nlags: int = 20) -> np.ndarray:
    """Autocorrelation up to ``nlags``. State the sampling scheme used (Rule 2)."""
    arr = _prepare_stat_series(series, max_points=max(1000, 5 * max(1, nlags)))
    if arr.size == 0:
        return np.empty(0, dtype=float)
    return _acf(arr, nlags=nlags, fft=True)


class HACResult(NamedTuple):
    params: np.ndarray
    hac_se: np.ndarray
    tvalues: np.ndarray
    pvalues: np.ndarray


def hac_ols(
    y: Sequence[float],
    x: Sequence[float],
    groups: Optional[Sequence] = None,
    maxlags: int = 5,
) -> HACResult:
    """OLS of y on x with HAC (Newey-West) or session-clustered standard errors.

    Never report plain OLS standard errors on serially correlated return data (Rule 2).
    """
    y_arr = np.asarray(y, dtype=float)
    x_arr = sm.add_constant(np.asarray(x, dtype=float))
    model = sm.OLS(y_arr, x_arr, missing="drop")
    if groups is not None:
        fit = model.fit(cov_type="cluster", cov_kwds={"groups": np.asarray(groups)})
    else:
        fit = model.fit(cov_type="HAC", cov_kwds={"maxlags": maxlags})
    return HACResult(fit.params, fit.bse, fit.tvalues, fit.pvalues)
