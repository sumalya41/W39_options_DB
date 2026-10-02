"""Contract selection: candidates in, one contract key out (or None). Pure, deterministic
tie-breaking so the same inputs always choose the same contract.
"""
from __future__ import annotations

from typing import NamedTuple, Optional, Sequence


class ContractCandidate(NamedTuple):
    contract_key: str
    dte: int
    log_moneyness: float
    rel_spread: float


def select_contract(
    candidates: Sequence[ContractCandidate],
    target_dte: int,
    min_dte: int,
    max_dte: int,
    max_abs_log_moneyness: float,
) -> Optional[str]:
    """DTE window -> expiry nearest target -> |log(K/S)| minimized -> smaller spread ->
    contract_key (lexicographic) as the final deterministic tie-break.
    """
    eligible = [
        c
        for c in candidates
        if min_dte <= c.dte <= max_dte and abs(c.log_moneyness) <= max_abs_log_moneyness
    ]
    if not eligible:
        return None
    best = min(
        eligible,
        key=lambda c: (abs(c.dte - target_dte), abs(c.log_moneyness), c.rel_spread, c.contract_key),
    )
    return best.contract_key
