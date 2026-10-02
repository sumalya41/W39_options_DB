"""Ledger construction via fold, not a stateful class: a sequence of fills reduces to a
list of ledger rows plus running cash, computed functionally with ``functools.reduce``.
"""
from __future__ import annotations

from functools import reduce
from typing import NamedTuple, Sequence

from options_research.strategy.execute import Fill


class LedgerRow(NamedTuple):
    contract_key: str
    side: str
    qty: int
    price: float
    fees: float
    cash_after: float
    rejected: bool
    reject_reason: str | None


def _step(acc: tuple[list[LedgerRow], float], fill: Fill, multiplier: float, fee_per_side: float) -> tuple[list[LedgerRow], float]:
    rows, cash = acc
    if fill.rejected:
        return rows + [LedgerRow(fill.contract_key, fill.side, fill.qty, fill.price, 0.0, cash, True, fill.reject_reason)], cash
    notional = fill.price * fill.qty * multiplier
    fees = fee_per_side * fill.qty
    cash_after = cash - notional - fees if fill.side == "BUY" else cash + notional - fees
    return rows + [LedgerRow(fill.contract_key, fill.side, fill.qty, fill.price, fees, cash_after, False, None)], cash_after


def fold_fills(fills: Sequence[Fill], initial_cash: float, multiplier: float, fee_per_side: float) -> list[LedgerRow]:
    """Deterministic fold over fills in time order; no shared mutable ledger object."""
    rows, _ = reduce(lambda acc, f: _step(acc, f, multiplier, fee_per_side), fills, ([], initial_cash))
    return rows
