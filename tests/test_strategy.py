import numpy as np

from options_research.strategy.execute import Fill
from options_research.strategy.ledger import fold_fills
from options_research.strategy.select import ContractCandidate, select_contract
from options_research.strategy.signals import momentum_signal


def test_momentum_signal():
    out = momentum_signal(np.array([0.002, -0.002, 0.0]), threshold=0.001)
    assert list(out) == [1, -1, 0]


def test_select_contract_deterministic():
    candidates = [
        ContractCandidate("A", dte=10, log_moneyness=0.01, rel_spread=0.02),
        ContractCandidate("B", dte=14, log_moneyness=0.01, rel_spread=0.01),
    ]
    assert select_contract(candidates, target_dte=14, min_dte=7, max_dte=30, max_abs_log_moneyness=0.02) == "B"


def test_select_contract_none_when_no_candidate_eligible():
    candidates = [ContractCandidate("A", dte=100, log_moneyness=0.5, rel_spread=0.02)]
    assert select_contract(candidates, target_dte=14, min_dte=7, max_dte=30, max_abs_log_moneyness=0.02) is None


def test_fold_fills_accumulates_cash():
    fills = [
        Fill("A", "BUY", 1, 10.0, 1, False, None),
        Fill("A", "SELL", 1, 12.0, 2, False, None),
    ]
    ledger = fold_fills(fills, initial_cash=1000.0, multiplier=100.0, fee_per_side=0.65)
    assert ledger[-1].cash_after > 1000.0  # net profit after round trip
