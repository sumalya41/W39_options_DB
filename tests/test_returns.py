import numpy as np

from options_research.research.returns import cumulative_return, simple_return


def test_simple_return_basic():
    values = [100.0, 110.0, 99.0]
    s = simple_return(values)
    assert np.allclose(s, np.array([0.10, -0.1]))


def test_cumulative_return_basic():
    ret = [0.10, -0.20, 0.05]
    cum = cumulative_return(ret)
    assert np.allclose(cum, np.array([0.10, -0.12, -0.076]))


def test_simple_return_short_series():
    assert simple_return([10.0]).size == 0
    assert cumulative_return([]).size == 0
