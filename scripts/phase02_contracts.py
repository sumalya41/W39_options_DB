"""Phase 2 driver: contract identity & symbol parsing against the real dataset.

Thin I/O-boundary script. Works only on the DISTINCT (session_date, publisher_id,
instrument_id, symbol) table (Rule 5: memory safety) — never on the 35M raw rows directly.
Writes the enriched symbology map to data/derived/ (Rule 6: raw data immutability).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

from options_research import memguard  # noqa: E402
from options_research.contracts import (  # noqa: E402
    coverage_table,
    unparsed_symbols,
    validate_day_local_instrument_id,
    with_contract_key,
    with_underlying_future_hypothesis,
)
from options_research.io import lazyio  # noqa: E402

DATA_PATH = ROOT / "data" / "databento_options_clean.parquet"
OUT_DIR = ROOT / "outputs" / "phase02"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    facts: dict = {"data_path": str(DATA_PATH)}

    print("Building DISTINCT (session_date, publisher_id, instrument_id, symbol)...")
    distinct = (
        lazyio.scan(DATA_PATH, columns=["session_date", "publisher_id", "instrument_id", "symbol"])
        .unique()
        .collect(engine="streaming")
    )
    facts["n_distinct_id_symbol_rows"] = distinct.height
    memguard.check("phase02.distinct")

    print("Parsing symbols -> contract_key on the distinct table (bounded size)...")
    enriched = with_contract_key(distinct)
    memguard.check("phase02.with_contract_key")

    print("Validating instrument_id is day-local (not a global contract key)...")
    facts["instrument_id_validation"] = validate_day_local_instrument_id(enriched)

    print("Coverage table (by product)...")
    cov = coverage_table(enriched)
    cov.write_csv(OUT_DIR / "coverage_table.csv")
    facts["coverage_by_product"] = cov.to_dicts()

    print("Unparsed symbols (spread/strategy)...")
    unparsed = unparsed_symbols(enriched)
    unparsed.write_csv(OUT_DIR / "unparsed_symbols.csv")
    facts["n_unparsed_symbols"] = unparsed.height

    print("Session/expiration/strike coverage...")
    options = enriched.filter(pl.col("kind") == "option")
    futures = enriched.filter(pl.col("kind") == "future")
    facts["n_sessions"] = enriched["session_date"].n_unique()
    facts["n_option_contracts"] = options["contract_key"].n_unique()
    facts["n_future_contracts"] = futures["contract_key"].n_unique()
    facts["future_expiries"] = sorted(e.isoformat() for e in futures["expiry"].unique().to_list())
    facts["option_expiries"] = sorted(e.isoformat() for e in options["expiry"].unique().to_list())
    strikes = options["strike"].drop_nulls()
    facts["strike_min"] = float(strikes.min())
    facts["strike_max"] = float(strikes.max())
    memguard.check("phase02.coverage")

    print("Option -> underlying future mapping hypothesis (nearest expiry on/after)...")
    future_expiries = futures["expiry"].to_list()
    options_mapped = with_underlying_future_hypothesis(options, future_expiries)
    mapping_summary = (
        options_mapped.group_by("expiry")
        .agg(underlying_future_expiry=pl.col("underlying_future_expiry").first(), n_contracts=pl.len())
        .sort("expiry")
    )
    mapping_summary.write_csv(OUT_DIR / "option_to_future_mapping_hypothesis.csv")
    facts["option_to_future_mapping_hypothesis"] = [
        {"option_expiry": r["expiry"].isoformat(), "underlying_future_expiry": (r["underlying_future_expiry"].isoformat() if r["underlying_future_expiry"] else None), "n_contracts": r["n_contracts"]}
        for r in mapping_summary.to_dicts()
    ]
    facts["mapping_note"] = (
        "HYPOTHESIS, not verified: nearest future expiring on/after the option expiry. "
        "Must be tested against development-session underlying/option return correlation "
        "once splits are frozen (Phase 5+); not applied as fact here."
    )

    print("Writing enriched symbology map to data/derived/ (never the raw file)...")
    out_path = lazyio.sink(enriched.lazy(), lazyio.derived_path("phase02_symbology_map"))
    facts["symbology_map_path"] = str(out_path)

    (OUT_DIR / "facts_step02.json").write_text(json.dumps(facts, indent=2, default=str))
    print(json.dumps(facts, indent=2, default=str))
    print(f"\nWrote outputs to {OUT_DIR} and {out_path}")


if __name__ == "__main__":
    main()
