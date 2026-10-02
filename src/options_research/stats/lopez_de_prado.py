"""López de Prado tools (Advances in Financial Machine Learning): fractional differentiation,
CUSUM event sampling, purge/embargo masking. All pure: array/sequence in, array out.
"""
from __future__ import annotations

from typing import Sequence

import numpy as np


def frac_diff_weights(d: float, thresh: float = 1e-4) -> np.ndarray:
    """Fixed-width weights for fractional differentiation, oldest-first truncated at ``thresh``."""
    weights = [1.0]
    k = 1
    while True:
        w_k = -weights[-1] / k * (d - k + 1)
        if abs(w_k) < thresh:
            break
        weights.append(w_k)
        k += 1
    return np.array(weights[::-1])


def frac_diff(series: Sequence[float], d: float, thresh: float = 1e-4) -> np.ndarray:
    """Fractionally differentiate a price series: stationary, but retains more memory than
    a plain first difference (Rule 4). Leading ``width-1`` values are NaN (insufficient window).
    """
    weights = frac_diff_weights(d, thresh)
    width = len(weights)
    arr = np.asarray(series, dtype=float)
    out = np.full(arr.shape, np.nan)
    for i in range(width - 1, len(arr)):
        out[i] = float(np.dot(weights, arr[i - width + 1 : i + 1]))
    return out


def cusum_filter(returns: Sequence[float], threshold: float) -> np.ndarray:
    """Symmetric CUSUM event sampler: returns the indices where cumulative up/down return
    exceeds ``threshold``, resetting after each event. Alternative to a fixed clock grid
    (Rule 4) — report both side by side.
    """
    s_pos, s_neg = 0.0, 0.0
    events: list[int] = []
    for i, r in enumerate(returns):
        s_pos = max(0.0, s_pos + r)
        s_neg = min(0.0, s_neg + r)
        if s_pos > threshold:
            s_pos = 0.0
            events.append(i)
        elif s_neg < -threshold:
            s_neg = 0.0
            events.append(i)
    return np.array(events, dtype=int)


def purge_embargo_mask(
    timestamps: Sequence[int],
    boundary_start: int,
    boundary_end: int,
    embargo: int,
) -> np.ndarray:
    """Boolean mask, True where a training timestamp must be **purged** because its outcome
    window overlaps the [boundary_start, boundary_end] split plus an embargo period after it.
    """
    ts = np.asarray(timestamps)
    return (ts >= boundary_start) & (ts <= boundary_end + embargo)
