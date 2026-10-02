from datetime import date

import polars as pl

from options_research.contracts import (
    coverage_table,
    nearest_future_on_or_after,
    validate_day_local_instrument_id,
    with_contract_key,
    with_underlying_future_hypothesis,
)


def test_with_contract_key_adds_columns():
    df = pl.DataFrame({"symbol": ["EUCO SI 20260710 PS EU C 1.165", "garbage"]})
    out = with_contract_key(df)
    assert out["kind"].to_list() == ["option", "unknown"]
    assert out["contract_key"][0] == "EUCO|20260710|C|1.16500"


def test_coverage_table_counts_calls_and_puts():
    df = pl.DataFrame(
        {
            "symbol": [
                "EUCO SI 20260710 PS EU C 1.165",
                "EUCO SI 20260710 PS EU P 1.165",
                "EUCO SI 20260814 PS EU C 1.170",
            ]
        }
    )
    cov = coverage_table(df)
    row = cov.row(0, named=True)
    assert row["n_contracts"] == 3
    assert row["n_calls"] == 2
    assert row["n_puts"] == 1


def test_validate_day_local_instrument_id_detects_reuse_across_days():
    # Same instrument_id 1 maps to a different symbol on a different day -> day-local proof.
    distinct = pl.DataFrame(
        {
            "session_date": ["2026-01-01", "2026-01-02"],
            "publisher_id": [101, 101],
            "instrument_id": [1, 1],
            "symbol": ["EUCO SI 20260710 PS EU C 1.165", "EUCO SI 20260814 PS EU C 1.170"],
        }
    )
    result = validate_day_local_instrument_id(distinct)
    assert result["n_instrument_ids_reused_across_days_for_different_symbols"] == 1
    assert result["n_ids_with_multiple_symbols_same_day"] == 0


def test_nearest_future_on_or_after():
    expiries = [date(2026, 3, 16), date(2026, 6, 15), date(2026, 9, 14)]
    assert nearest_future_on_or_after(date(2026, 4, 1), expiries) == date(2026, 6, 15)
    assert nearest_future_on_or_after(date(2026, 12, 1), expiries) is None


def test_with_underlying_future_hypothesis():
    options_df = pl.DataFrame({"expiry": [date(2026, 4, 10), date(2026, 7, 10)]})
    out = with_underlying_future_hypothesis(options_df, [date(2026, 6, 15), date(2026, 9, 14)])
    assert out["underlying_future_expiry"].to_list() == [date(2026, 6, 15), date(2026, 9, 14)]
