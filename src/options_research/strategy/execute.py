"""Order execution simulation: order + quote in, Fill out. No clock/state access here —
the caller supplies the decision/submit/fill timestamps explicitly.
"""
from __future__ import annotations

from typing import NamedTuple, Optional


class Order(NamedTuple):
    contract_key: str
    side: str  # "BUY" | "SELL"
    qty: int
    submit_ts: int
    deadline_ts: int


class Quote(NamedTuple):
    ts_recv: int
    bid: float
    ask: float
    bid_size: int
    ask_size: int


class Fill(NamedTuple):
    contract_key: str
    side: str
    qty: int
    price: float
    fill_ts: int
    rejected: bool
    reject_reason: Optional[str]


def simulate_fill(order: Order, quote: Optional[Quote], slippage: float = 0.0) -> Fill:
    """First usable quote at/after submit_ts, at/before deadline_ts, else a rejection."""
    if quote is None or quote.ts_recv < order.submit_ts or quote.ts_recv > order.deadline_ts:
        return Fill(order.contract_key, order.side, order.qty, float("nan"), order.deadline_ts, True, "late_or_no_quote")
    if quote.bid <= 0 or quote.ask <= quote.bid:
        return Fill(order.contract_key, order.side, order.qty, float("nan"), quote.ts_recv, True, "invalid_quote")
    price = quote.ask + slippage if order.side == "BUY" else quote.bid - slippage
    return Fill(order.contract_key, order.side, order.qty, price, quote.ts_recv, False, None)
