import numpy as np

from options_research.stats.lopez_de_prado import cusum_filter, frac_diff, frac_diff_weights, purge_embargo_mask


def test_frac_diff_weights_shrink_and_truncate():
    w = frac_diff_weights(d=0.4, thresh=1e-3)
    assert w[-1] == 1.0  # last weight (most recent obs) is always 1
    assert abs(w[0]) < 1e-2


def test_frac_diff_leading_values_are_nan():
    series = np.cumsum(np.random.default_rng(0).normal(size=100)) + 100
    out = frac_diff(series, d=0.4)
    width = len(frac_diff_weights(0.4))
    assert np.all(np.isnan(out[: width - 1]))
    assert np.all(~np.isnan(out[width - 1 :]))


def test_cusum_filter_detects_trend():
    returns = [0.0, 0.001, 0.001, 0.001, 0.001, 0.001]
    events = cusum_filter(returns, threshold=0.003)
    assert len(events) >= 1


def test_purge_embargo_mask():
    ts = np.array([0, 5, 10, 15, 20, 25])
    mask = purge_embargo_mask(ts, boundary_start=10, boundary_end=15, embargo=5)
    assert list(mask) == [False, False, True, True, True, False]
