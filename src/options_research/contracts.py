"""Contract-identity functions: DataFrame in, new DataFrame out. No mutation of inputs."""
from __future__ import annotations

from typing import Optional

import polars as pl

from options_research.symbols import ParsedSymbol, build_contract_key, parse_symbol


def with_contract_key(df: pl.DataFrame, symbol_col: str = "symbol") -> pl.DataFrame:
    """Adds `kind`, `product`, `expiry`, `right`, `strike`, `contract_key` columns.

    Pure: returns a new DataFrame; `df` is untouched.
    """
    parsed: list[ParsedSymbol] = [parse_symbol(s) for s in df[symbol_col].to_list()]
    return df.with_columns(
        kind=pl.Series([p.kind for p in parsed]),
        product=pl.Series([p.product for p in parsed]),
        expiry=pl.Series([p.expiry for p in parsed]),
        right=pl.Series([p.right for p in parsed]),
        strike=pl.Series([p.strike for p in parsed]),
        contract_key=pl.Series([build_contract_key(p) for p in parsed]),
    )


def unparsed_symbols(df: pl.DataFrame, symbol_col: str = "symbol") -> pl.DataFrame:
    enriched = with_contract_key(df, symbol_col)
    return enriched.filter(pl.col("kind") == "unknown").select(symbol_col).unique()


def coverage_table(df: pl.DataFrame) -> pl.DataFrame:
    """Sessions, underlyings, calls/puts, expirations, contracts — from an enriched frame."""
    enriched = with_contract_key(df) if "contract_key" not in df.columns else df
    return (
        enriched.filter(pl.col("kind") == "option")
        .group_by("product")
        .agg(
            n_contracts=pl.col("contract_key").n_unique(),
            n_expiries=pl.col("expiry").n_unique(),
            n_calls=(pl.col("right") == "C").sum(),
            n_puts=(pl.col("right") == "P").sum(),
        )
    )


def validate_day_local_instrument_id(
    distinct_df: pl.DataFrame,
    date_col: str = "session_date",
    publisher_col: str = "publisher_id",
    id_col: str = "instrument_id",
    symbol_col: str = "symbol",
) -> dict:
    """Proves/disproves that `instrument_id` is unique only within a day+publisher (Rule:
    never join on it alone). Takes the DISTINCT (date, publisher, instrument_id, symbol)
    table, not raw rows — pure, read-only, no mutation.
    """
    per_day_id = distinct_df.group_by(date_col, publisher_col, id_col).agg(
        n_symbols=pl.col(symbol_col).n_unique()
    )
    per_day_symbol = distinct_df.group_by(date_col, publisher_col, symbol_col).agg(
        n_ids=pl.col(id_col).n_unique()
    )
    ids_reused_across_days = (
        distinct_df.group_by(id_col, publisher_col)
        .agg(n_distinct_symbols_over_time=pl.col(symbol_col).n_unique(), n_days=pl.col(date_col).n_unique())
        .filter(pl.col("n_distinct_symbols_over_time") > 1)
    )
    return {
        "n_ids_with_multiple_symbols_same_day": int((per_day_id["n_symbols"] > 1).sum()),
        "n_symbols_with_multiple_ids_same_day": int((per_day_symbol["n_ids"] > 1).sum()),
        "n_instrument_ids_reused_across_days_for_different_symbols": ids_reused_across_days.height,
    }


def nearest_future_on_or_after(option_expiry, future_expiries: list) -> Optional[object]:
    """[verify] hypothesis: an option's underlying future is the one expiring on/after it.
    Pure: a date and a list of candidate dates in, the chosen date (or None) out.
    """
    candidates = sorted(e for e in future_expiries if e >= option_expiry)
    return candidates[0] if candidates else None


def with_underlying_future_hypothesis(
    options_df: pl.DataFrame, future_expiries: list
) -> pl.DataFrame:
    """Adds `underlying_future_expiry` per the nearest-on-or-after hypothesis. Labelled a
    hypothesis, not an observed fact — test it against development-session returns (Phase 5+)
    before relying on it.
    """
    expiries = options_df["expiry"].to_list()
    mapped = [nearest_future_on_or_after(e, future_expiries) for e in expiries]
    return options_df.with_columns(underlying_future_expiry=pl.Series(mapped))

