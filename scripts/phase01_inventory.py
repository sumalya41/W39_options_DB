"""Phase 1 driver: inventory the real dataset and write data_dictionary.csv + facts_step01.json.

Thin I/O-boundary script (not part of options_research): calls pure/read-only functions from
the package, prints progress, writes two small output files. Columns are always projected
before any eager collect (Rule 5: memory safety) — no unprojected full-table materialization.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

from options_research import memguard  # noqa: E402
from options_research.io import inventory  # noqa: E402
from options_research.symbols import parse_symbol  # noqa: E402

DATA_PATH = ROOT / "data" / "databento_options_clean.parquet"
OUT_DIR = ROOT / "outputs" / "phase01"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    facts: dict = {"data_path": str(DATA_PATH)}

    print("Schema + row count...")
    schema_df = inventory.describe_schema(DATA_PATH)
    n_rows = inventory.row_count(DATA_PATH)
    facts["n_rows"] = int(n_rows)
    memguard.check("phase01.schema")

    print("Null counts (streaming)...")
    nulls = inventory.null_counts(DATA_PATH)
    null_counts = nulls.row(0, named=True)
    facts["null_counts"] = null_counts
    memguard.check("phase01.nulls")

    print("action/side/publisher_id/channel_id/session_date value counts...")
    action_counts = inventory.value_counts(DATA_PATH, "action", limit=20)
    side_counts = inventory.value_counts(DATA_PATH, "side", limit=20)
    publisher_counts = inventory.value_counts(DATA_PATH, "publisher_id", limit=20)
    channel_counts = inventory.value_counts(DATA_PATH, "channel_id", limit=20)
    session_counts = inventory.value_counts(DATA_PATH, "session_date", limit=100)
    facts["action_counts"] = dict(zip(action_counts["action"], action_counts["n"]))
    facts["side_counts"] = dict(zip(side_counts["side"], side_counts["n"]))
    facts["publisher_id_counts"] = dict(zip(publisher_counts["publisher_id"], publisher_counts["n"]))
    facts["channel_id_counts"] = dict(zip(channel_counts["channel_id"], channel_counts["n"]))
    facts["n_sessions"] = session_counts.height
    facts["session_dates"] = sorted(session_counts["session_date"].to_list())
    memguard.check("phase01.value_counts")

    print("Distinct symbols + parsing (futures vs options vs unknown)...")
    symbol_counts = inventory.value_counts(DATA_PATH, "symbol", limit=200_000)
    distinct_symbols = symbol_counts["symbol"].to_list()
    parsed = [parse_symbol(s) for s in distinct_symbols]
    kind_counts: dict[str, int] = {}
    unknown_examples: list[str] = []
    for p in parsed:
        kind_counts[p.kind] = kind_counts.get(p.kind, 0) + 1
        if p.kind == "unknown" and len(unknown_examples) < 20:
            unknown_examples.append(p.raw)
    facts["n_distinct_symbols"] = len(distinct_symbols)
    facts["symbol_kind_counts"] = kind_counts
    facts["unknown_symbol_examples"] = unknown_examples
    memguard.check("phase01.symbols")

    print("ts_recv monotonicity check (per session_date, publisher_id, ordered by sequence)...")
    narrow = (
        pl.scan_parquet(str(DATA_PATH))
        .select("session_date", "publisher_id", "sequence", "ts_recv")
        .collect(engine="streaming")
    )
    memguard.check("phase01.narrow_collect")
    narrow = narrow.sort(["session_date", "publisher_id", "sequence"])
    reversals = narrow.with_columns(
        ts_recv_diff=pl.col("ts_recv").diff().over(["session_date", "publisher_id"])
    )
    n_reversals = reversals.filter(pl.col("ts_recv_diff") < 0).height
    facts["ts_recv_reversals_by_sequence_order"] = int(n_reversals)
    memguard.check("phase01.monotonicity")

    # Data dictionary: schema + null counts + a one-line status/meaning per column.
    meaning = {
        "ts_recv": "receive timestamp (ns, UTC) — index timestamp, monotonic per file/channel only",
        "ts_event": "exchange/venue event timestamp (ns, UTC) — may lag/lead ts_recv",
        "ts_index": "[verify] purpose unconfirmed — not in Eurex playbook's documented schema",
        "price": "order/event price, already DOUBLE (decimal) — no int64 fixed-point scaling needed",
        "rtype": "Databento record type code — does not alone determine schema (observed, [verify] meaning)",
        "publisher_id": "publisher/venue id (int) — distinguishes the two source folders/channels",
        "instrument_id": "day-local instrument id — never join across days without the symbology map",
        "action": "MBO action: A/C/M/R/T/F expected (add/cancel/modify/reset/trade/fill)",
        "side": "A/B/N expected (ask/bid/not-specified) — N is not neutral/call-put",
        "size": "order size (int) in contract/lot units — lot size [verify] against Eurex spec",
        "channel_id": "feed channel id (int)",
        "order_id": "per-order identifier, stored as VARCHAR — confirm numeric range before any rank/factorize",
        "flags": "bitmask flags — only bits 8 (bad ts_recv) and 4 (maybe bad book) are error flags",
        "ts_in_delta": "clamped int32 — OPRA-specific meaning does not apply to this Eurex venue",
        "sequence": "per-channel monotonic sequence number — used here as the feed-order proxy",
        "symbol": "raw exchange symbol string — parsed via options_research.symbols.parse_symbol",
        "session_date": "trading session date (VARCHAR) — already present, no derivation needed",
    }
    rows = []
    for name, dtype in zip(schema_df["column_name"], schema_df["column_type"]):
        rows.append(
            {
                "column": name,
                "dtype": dtype,
                "null_count": null_counts.get(name),
                "status": "observed",
                "meaning": meaning.get(name, "[verify]"),
            }
        )
    pl.DataFrame(rows).write_csv(OUT_DIR / "data_dictionary.csv")

    (OUT_DIR / "facts_step01.json").write_text(json.dumps(facts, indent=2, default=str))
    print(json.dumps(facts, indent=2, default=str))
    print(f"\nWrote {OUT_DIR / 'data_dictionary.csv'} and {OUT_DIR / 'facts_step01.json'}")


if __name__ == "__main__":
    main()
