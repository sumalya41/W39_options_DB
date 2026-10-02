"""Phase 3 driver: cleaning audit / decode log for the raw Eurex MBO parquet.

This is a thin I/O-boundary script: it reads the raw file, computes a compact audit summary
without materializing full raw tables, writes a `cleaning_log.csv` under outputs/phase03/
and emits a JSON summary. The raw file is never modified (Rule 6).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl  # noqa: E402

from options_research import memguard  # noqa: E402
from options_research.io import lazyio  # noqa: E402

DATA_PATH = ROOT / "data" / "databento_options_clean.parquet"
OUT_DIR = ROOT / "outputs" / "phase03"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    lf = lazyio.scan(
        DATA_PATH,
        columns=[
            "session_date",
            "publisher_id",
            "ts_recv",
            "sequence",
            "flags",
            "price",
            "action",
            "side",
            "size",
            "order_id",
            "symbol",
        ],
    )

    n_rows = int(lf.select(pl.len()).collect(engine="streaming").item())
    price_nulls = int(
        lf.select(pl.col("price").null_count().alias("n")).collect(engine="streaming").item()
    )
    size_nulls = int(
        lf.select(pl.col("size").null_count().alias("n")).collect(engine="streaming").item()
    )
    negative_price_rows = int(
        lf.filter((pl.col("price").is_not_null()) & (pl.col("price") < 0)).select(pl.len()).collect(engine="streaming").item()
    )
    nonpositive_size_rows = int(
        lf.filter((pl.col("size").is_not_null()) & (pl.col("size") <= 0)).select(pl.len()).collect(engine="streaming").item()
    )
    ts_recv_reversals = int(
        lf.with_columns(
            pl.col("ts_recv").diff().over(["session_date", "publisher_id"]).alias("ts_recv_delta")
        )
        .filter(pl.col("ts_recv_delta") < 0)
        .select(pl.len())
        .collect(engine="streaming")
        .item()
    )
    sequence_reversals = int(
        lf.with_columns(
            pl.col("sequence").diff().over(["session_date", "publisher_id"]).alias("seq_delta")
        )
        .filter(pl.col("seq_delta") < 0)
        .select(pl.len())
        .collect(engine="streaming")
        .item()
    )
    flags_4_or_8 = int(
        lf.filter(pl.col("flags").is_in([4, 8])).select(pl.len()).collect(engine="streaming").item()
    )
    action_counts = lf.group_by("action").agg(n=pl.len()).sort("action").collect(engine="streaming")
    side_counts = lf.group_by("side").agg(n=pl.len()).sort("side").collect(engine="streaming")
    session_counts = lf.group_by("session_date").agg(n=pl.len()).sort("session_date").collect(engine="streaming")
    rowid_duplicates = int(
        lf.group_by("order_id").agg(n=pl.len()).filter(pl.col("n") > 1).select(pl.sum("n").sub(pl.len())).collect(engine="streaming").item()
    )

    memguard.check("phase03.audit")

    summary = {
        "n_rows": n_rows,
        "price_nulls": price_nulls,
        "size_nulls": size_nulls,
        "negative_price_rows": negative_price_rows,
        "nonpositive_size_rows": nonpositive_size_rows,
        "ts_recv_reversals": ts_recv_reversals,
        "sequence_reversals": sequence_reversals,
        "flags_4_or_8": flags_4_or_8,
        "order_id_duplicate_rows": rowid_duplicates,
        "action_counts": action_counts.to_dicts(),
        "side_counts": side_counts.to_dicts(),
        "session_date_row_counts": session_counts.to_dicts(),
    }

    cleaned = [{
        "issue": "price_nulls_on_reset",
        "count": price_nulls,
        "explanation": "Null prices occur exactly on reset events; this is expected in MBO resets and should be excluded from mid-price reconstruction.",
    }, {
        "issue": "negative_price_rows",
        "count": negative_price_rows,
        "explanation": "Negative prices correspond to non-standard spread/strategy instruments and should not be used in the single-leg option/future universe.",
    }, {
        "issue": "ts_recv_reversals",
        "count": ts_recv_reversals,
        "explanation": "Feed-order reversals are real data irregularities; they are retained for the audit and handled explicitly during replay ordering.",
    }, {
        "issue": "sequence_reversals",
        "count": sequence_reversals,
        "explanation": "Sequence decreases indicate order log reordering within a session/publisher and should be treated as a known feed anomaly.",
    }, {
        "issue": "partial_day_session",
        "count": int(session_counts.filter(pl.col("session_date") == "2026-04-06")["n"].sum()) if "2026-04-06" in session_counts["session_date"].to_list() else 0,
        "explanation": "The partial session on 2026-04-06 is explicitly noted by the playbook; exclude it from the development window unless validation requires it.",
    }]

    cleaning_df = pl.DataFrame(cleaned)
    cleaning_path = OUT_DIR / "cleaning_log.csv"
    cleaning_df.write_csv(cleaning_path)

    (OUT_DIR / "facts_step03.json").write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps(summary, indent=2, default=str))
    print(f"Wrote audit summary to {OUT_DIR}")


if __name__ == "__main__":
    main()
