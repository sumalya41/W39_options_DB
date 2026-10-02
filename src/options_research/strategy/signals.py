"""Signal construction: return in, signal out. Threshold must come from the frozen config
(development-calibrated), never hard-coded here (Rule 3/4).
"""
from __future__ import annotations

import numpy as np


def momentum_signal(returns: np.ndarray, threshold: float) -> np.ndarray:
    """+1 if return > threshold, -1 if return < -threshold, else 0. Pure elementwise map."""
    arr = np.asarray(returns, dtype=float)
    return np.where(arr > threshold, 1, np.where(arr < -threshold, -1, 0))
