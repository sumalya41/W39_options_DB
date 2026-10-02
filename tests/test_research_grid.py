import polars as pl

from options_research.research.grid import build_research_table


def test_build_research_table_backward_asof_and_eligibility():
    grid = pl.DataFrame(
        {
            "session_date": ["2026-03-09", "2026-03-09"],
            "contract_key": ["EUCO|20260316|C|1.15000", "EUCO|20260316|C|1.15000"],
            "decision_ts": [10_000_000_000, 70_000_000_000],
        }
    )
    option_quotes = pl.DataFrame(
        {
            "session_date": ["2026-03-09", "2026-03-09"],
            "contract_key": ["EUCO|20260316|C|1.15000", "EUCO|20260316|C|1.15000"],
            "ts_recv": [9_000_000_000, 40_000_000_000],
            "best_bid": [1.12, 1.13],
            "best_ask": [1.14, 1.15],
            "strike": [1.15, 1.15],
            "expiry": ["2026-03-16", "2026-03-16"],
        }
    )
    underlying_quotes = pl.DataFrame(
        {
            "session_date": ["2026-03-09", "2026-03-09"],
            "symbol": ["FCEU SI 20260316 PS", "FCEU SI 20260316 PS"],
            "ts_recv": [9_500_000_000, 40_500_000_000],
            "mid_price": [1.16, 1.17],
        }
    )

    tbl = build_research_table(
        grid=grid,
        option_quotes=option_quotes,
        underlying_quotes=underlying_quotes,
        quote_max_age_seconds=5,
        session_date_col="session_date",
        decision_ts_col="decision_ts",
        option_ts_col="ts_recv",
        option_bid_col="best_bid",
        option_ask_col="best_ask",
        underlying_ts_col="ts_recv",
        underlying_mid_col="mid_price",
        by_cols=["session_date", "contract_key"],
    )

    assert tbl["quote_age_seconds"].to_list() == [1, 30]
    assert tbl["entry_eligible"].to_list() == [True, False]
    assert tbl["log_moneyness"].to_list()[0] > -1.0
    assert tbl["dte_days"].to_list() == [7, 7]
