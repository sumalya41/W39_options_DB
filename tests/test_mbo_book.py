import inspect

from options_research.mbo.book import EMPTY_STATE, OrderEvent, apply_event, replay, replay_stream


def test_apply_event_does_not_mutate_input_state():
    before = EMPTY_STATE
    ev = OrderEvent(ts_recv=1, action="A", order_id="o1", side="B", price=100.0, size=10)
    after = apply_event(before, ev)
    assert before == EMPTY_STATE  # original untouched
    assert after.bid_levels == {100.0: 10}


def test_replay_stream_is_a_generator_not_a_list():
    assert inspect.isgenerator(replay_stream([]))


def test_replay_emits_bbo_on_change_only():
    events = [
        OrderEvent(1, "A", "o1", "B", 100.0, 10),
        OrderEvent(2, "A", "o2", "A", 101.0, 5),
        OrderEvent(3, "A", "o3", "B", 99.0, 20),  # worse bid, best bid unchanged
    ]
    rows = replay(events)
    assert len(rows) == 2  # row 3 does not change best bid/ask
    assert rows[0].best_bid == 100.0 and rows[0].best_ask is None
    assert rows[1].best_bid == 100.0 and rows[1].best_ask == 101.0


def test_cancel_removes_order_and_restores_prior_level():
    events = [
        OrderEvent(1, "A", "o1", "B", 100.0, 10),
        OrderEvent(2, "A", "o2", "B", 100.0, 5),
        OrderEvent(3, "C", "o2", None, None, None),
    ]
    rows = replay(events)
    assert rows[-1].best_bid_size == 10


def test_reset_clears_book():
    events = [
        OrderEvent(1, "A", "o1", "B", 100.0, 10),
        OrderEvent(2, "R", "", None, None, None),
    ]
    rows = replay(events)
    assert rows[-1].best_bid is None and rows[-1].best_ask is None
