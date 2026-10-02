"""Functional MBO -> top-of-book (BBO) replay.

ABSOLUTE RULE compliance: ``apply_event`` takes a state and an event and returns a brand
new state; it never mutates ``state`` or ``event``. ``replay`` folds over a sequence of
events with ``apply_event`` (an explicit scan, not a stateful class) and emits one BBO row
per event where the best bid/ask actually changed.

Semantics are a simplified, documented approximation of the Eurex MBO action codes
(A=add, C=cancel, M=modify/replace, R=reset/clear, T/F=trade). [verify] against real
sample events before trusting quantitatively — see the Eurex playbook Section 5.
"""
from __future__ import annotations

from typing import Iterable, Iterator, Mapping, NamedTuple, Optional

from options_research import memguard

Side = str  # "A" (ask) | "B" (bid)


class OrderEvent(NamedTuple):
    ts_recv: int  # ns since epoch, for ordering/emission only
    action: str  # "A" | "C" | "M" | "R" | "T" | "F"
    order_id: str
    side: Optional[Side]
    price: Optional[float]
    size: Optional[int]


class _Order(NamedTuple):
    side: Side
    price: float
    size: int


class BookState(NamedTuple):
    """Immutable snapshot. Every transition returns a new instance."""

    orders: Mapping[str, _Order]
    bid_levels: Mapping[float, int]
    ask_levels: Mapping[float, int]


EMPTY_STATE = BookState(orders={}, bid_levels={}, ask_levels={})


def _add_level(levels: Mapping[float, int], price: float, delta_size: int) -> dict[float, int]:
    new_levels = dict(levels)
    new_total = new_levels.get(price, 0) + delta_size
    if new_total <= 0:
        new_levels.pop(price, None)
    else:
        new_levels[price] = new_total
    return new_levels


def _levels_for(state: BookState, side: Side) -> Mapping[float, int]:
    return state.bid_levels if side == "B" else state.ask_levels


def _with_levels(state: BookState, side: Side, levels: Mapping[float, int]) -> BookState:
    if side == "B":
        return state._replace(bid_levels=levels)
    return state._replace(ask_levels=levels)


def apply_event(state: BookState, event: OrderEvent) -> BookState:
    """Pure state transition. Unknown/trade actions pass the book through unchanged."""
    if event.action == "R":
        return EMPTY_STATE

    if event.action == "A" and event.side and event.price is not None and event.size:
        orders = dict(state.orders)
        orders[event.order_id] = _Order(event.side, event.price, event.size)
        levels = _add_level(_levels_for(state, event.side), event.price, event.size)
        return _with_levels(state._replace(orders=orders), event.side, levels)

    if event.action == "C" and event.order_id in state.orders:
        existing = state.orders[event.order_id]
        orders = dict(state.orders)
        del orders[event.order_id]
        levels = _add_level(_levels_for(state, existing.side), existing.price, -existing.size)
        return _with_levels(state._replace(orders=orders), existing.side, levels)

    if event.action == "M" and event.order_id in state.orders:
        existing = state.orders[event.order_id]
        removed_levels = _add_level(_levels_for(state, existing.side), existing.price, -existing.size)
        state_after_remove = _with_levels(state, existing.side, removed_levels)
        if event.side and event.price is not None and event.size:
            orders = dict(state_after_remove.orders)
            orders[event.order_id] = _Order(event.side, event.price, event.size)
            added_levels = _add_level(_levels_for(state_after_remove, event.side), event.price, event.size)
            return _with_levels(state_after_remove._replace(orders=orders), event.side, added_levels)
        orders = dict(state_after_remove.orders)
        orders.pop(event.order_id, None)
        return state_after_remove._replace(orders=orders)

    # T, F (trades) and anything else: book composition unchanged in this simplified model.
    return state


class BBORow(NamedTuple):
    ts_recv: int
    best_bid: Optional[float]
    best_bid_size: Optional[int]
    best_ask: Optional[float]
    best_ask_size: Optional[int]


def best_bid_ask(state: BookState) -> BBORow:
    bid = max(state.bid_levels) if state.bid_levels else None
    ask = min(state.ask_levels) if state.ask_levels else None
    return BBORow(
        ts_recv=0,
        best_bid=bid,
        best_bid_size=state.bid_levels.get(bid) if bid is not None else None,
        best_ask=ask,
        best_ask_size=state.ask_levels.get(ask) if ask is not None else None,
    )


def scan_states(events: Iterable[OrderEvent], initial: BookState = EMPTY_STATE) -> Iterator[BookState]:
    """Haskell-style ``scanl``: yields the initial state, then one state per event.

    A local loop variable is reassigned for iteration only — no object anywhere is
    mutated, and the function is referentially transparent in its inputs/outputs.
    Lazy (a generator): safe as long as the caller consumes it one state at a time and
    never does ``list(scan_states(...))`` on a large event stream (ABSOLUTE RULE: memory).
    """
    state = initial
    yield state
    for event in events:
        state = apply_event(state, event)
        yield state


def replay_stream(events: Iterable[OrderEvent], memcheck_every: int = 200_000) -> Iterator[BBORow]:
    """Streaming fold: O(1) memory in the number of events. Keeps only the current book
    state and the previously emitted BBO row — never a list of events or of past states.

    Use this (not ``replay``) for a full session/day of MBO data; sink each yielded row to
    parquet as it arrives. ``memcheck_every`` calls ``memguard.check`` periodically so an
    OOM condition raises here, before the Kaggle/OS kernel would be killed.
    """
    state = EMPTY_STATE
    prev_bbo: Optional[BBORow] = None
    for i, event in enumerate(events, start=1):
        state = apply_event(state, event)
        bbo = best_bid_ask(state)._replace(ts_recv=event.ts_recv)
        if prev_bbo is None or (bbo.best_bid, bbo.best_ask) != (prev_bbo.best_bid, prev_bbo.best_ask):
            yield bbo
        prev_bbo = bbo
        if memcheck_every and i % memcheck_every == 0:
            memguard.check("mbo.book.replay_stream")


def replay(events: Iterable[OrderEvent]) -> list[BBORow]:
    """Convenience wrapper over ``replay_stream`` for small/test inputs only. For a real
    session (millions of events) call ``replay_stream`` and sink/append incrementally —
    collecting the full result into one list here defeats the memory-safety guarantee.
    """
    return list(replay_stream(events))
