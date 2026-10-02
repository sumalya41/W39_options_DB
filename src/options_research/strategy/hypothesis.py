"""Phase 7 hypothesis diagnostics.

These functions stay pure and deterministic: they transform a research-table DataFrame into a
small summary dict describing the option-vs-underlying relationship in the development window.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl


def _ols_summary(x: np.ndarray, y: np.ndarray) -> dict[str, Any]:
    X = np.column_stack([np.ones(len(x)), x])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    pred = X @ beta
    resid = y - pred
    sse = float(resid.T @ resid)
    dof = max(len(y) - 2, 1)
    se = float(np.sqrt(sse / dof)) if dof > 0 else 0.0
    return {
        "intercept": float(beta[0]),
        "beta": float(beta[1]),
        "n": int(len(y)),
        "r2": float(1.0 - sse / max(np.sum((y - y.mean()) ** 2), 1e-12)),
        "std_error": se,
    }


def add_signal_features(df: pl.DataFrame) -> pl.DataFrame:
    """Add a 5-minute underlying return and a simple option-return proxy to the research table."""
    by = ["session_date", "contract_key"]
    result = df.sort(by + ["decision_ts"]).with_columns(
        pl.col("mid_price").shift(5).over(by).alias("mid_price_lag5"),
        pl.col("strike").shift(5).over(by).alias("strike_lag5"),
    )
    return result.with_columns(
        pl.when(pl.col("mid_price_lag5").is_not_null() & (pl.col("mid_price_lag5") > 0))
        .then((pl.col("mid_price") / pl.col("mid_price_lag5")).log())
        .otherwise(None)
        .alias("future_return_5m"),
        pl.when(pl.col("strike_lag5").is_not_null() & (pl.col("strike_lag5") > 0))
        .then((pl.col("strike") / pl.col("strike_lag5")).log())
        .otherwise(None)
        .alias("option_return_proxy_5m"),
    )


def build_hypothesis_summary(df: pl.DataFrame) -> dict[str, Any]:
    """Compute the development-only hypothesis and pass/fail criteria.

    We intentionally use simple, transparent statistics rather than a heavyweight model. The
    output is meant to freeze a testable claim before validation/test data are touched.
    """
    feature_df = add_signal_features(df).drop_nulls(subset=["future_return_5m", "option_return_proxy_5m"])
    if feature_df.is_empty():
        raise ValueError("No valid return observations available for the Phase 7 hypothesis test.")

    x = np.asarray(feature_df["future_return_5m"].to_numpy(), dtype=float)
    y = np.asarray(feature_df["option_return_proxy_5m"].to_numpy(), dtype=float)
    contemporaneous = _ols_summary(x, y)

    lagged = feature_df.sort(by=["session_date", "contract_key", "decision_ts"]).with_columns(
        pl.col("option_return_proxy_5m").shift(-5).over(["session_date", "contract_key"]).alias("option_return_proxy_5m_lagged")
    ).drop_nulls(subset=["future_return_5m", "option_return_proxy_5m_lagged"])
    x_lag = np.asarray(lagged["future_return_5m"].to_numpy(), dtype=float)
    y_lag = np.asarray(lagged["option_return_proxy_5m_lagged"].to_numpy(), dtype=float)
    lagged_model = _ols_summary(x_lag, y_lag)

    threshold = float(np.nanpercentile(np.abs(x), 90.0))
    pass_criteria = {
        "beta_positive": contemporaneous["beta"] > 0.0,
        "lagged_beta_positive": lagged_model["beta"] > 0.0,
        "n_obs_sufficient": len(x) >= 100,
        "threshold_positive": threshold > 0.0,
    }
    passed = all(pass_criteria.values())

    return {
        "n_observations": int(len(x)),
        "threshold_abs_5m_return_90pct": threshold,
        "contemporaneous_regression": contemporaneous,
        "lagged_regression": lagged_model,
        "pass_criteria": pass_criteria,
        "hypothesis_passed": passed,
        "hypothesis": (
            "A 5-minute futures return beyond the development-calibrated threshold is followed by "
            "a positive change in the option price proxy, with the sign of the relationship preserved "
            "in the lagged specification."
        ),
        "decision_rule": (
            "Retain the hypothesis only if the contemporaneous and lagged coefficients are both positive "
            "and the threshold is non-zero on development data; otherwise reject before validation/test."
        ),
    }
