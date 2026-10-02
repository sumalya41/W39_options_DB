"""Phase 7: freeze a falsifiable hypothesis from development-only research-table data."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl

from options_research.config import load_config
from options_research.strategy.hypothesis import build_hypothesis_summary

TABLE_PATH = ROOT / "outputs" / "phase05" / "research_table_subset.parquet"
OUT_DIR = ROOT / "outputs" / "phase07"


def main() -> None:
    cfg = load_config(ROOT / "config" / "config.yaml")
    df = pl.read_parquet(TABLE_PATH).with_columns(pl.col("session_date").cast(pl.String).alias("session_date"))
    dev = df.filter(pl.col("session_date").is_in(cfg.splits["development"]))
    if dev.is_empty():
        raise ValueError("No development rows available for Phase 7 hypothesis testing.")

    summary = build_hypothesis_summary(dev)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "hypothesis.json"
    out_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"Wrote Phase 7 hypothesis to {out_path}")


if __name__ == "__main__":
    main()
