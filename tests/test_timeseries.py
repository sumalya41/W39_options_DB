import numpy as np

from options_research.stats.timeseries import acf_values, adf_stationarity, hac_ols, log_returns


def test_log_returns_basic():
    prices = [100.0, 101.0, 100.0]
    r = log_returns(prices)
    assert len(r) == 2
    assert np.isclose(r[0], np.log(101.0 / 100.0))


def test_log_returns_short_series():
    assert log_returns([100.0]).size == 0


def test_adf_on_white_noise_is_stationary():
    rng = np.random.default_rng(42)
    noise = rng.normal(size=500)
    result = adf_stationarity(noise)
    assert result.is_stationary_at_5pct


def test_acf_values_length():
    rng = np.random.default_rng(0)
    series = rng.normal(size=200)
    vals = acf_values(series, nlags=10)
    assert len(vals) == 11  # includes lag 0


def test_hac_ols_recovers_slope():
    rng = np.random.default_rng(1)
    x = rng.normal(size=300)
    y = 2.0 * x + rng.normal(scale=0.1, size=300)
    result = hac_ols(y, x)
    assert np.isclose(result.params[1], 2.0, atol=0.2)
