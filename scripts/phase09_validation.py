"""Phase 9: validation, benchmark, robustness, and final decision summary."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl

from options_research.config import load_config
from options_research.strategy.validation import benchmark_summary, day_block_bootstrap, ledger_summary, robustness_summary

LEDGER_PATH = ROOT / "outputs" / "phase08" / "ledger.parquet"
OUT_DIR = ROOT / "outputs" / "phase09"


def main() -> None:
    cfg = load_config(ROOT / "config" / "config.yaml")
    ledger = pl.read_parquet(LEDGER_PATH)
    dev_validation = ledger.filter(pl.col("session_date").is_in(cfg.splits["development"] + cfg.splits["validation"]))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = {
        "config": {
            "seed": cfg.seed,
            "development_sessions": cfg.splits["development"],
            "validation_sessions": cfg.splits["validation"],
            "test_sessions": cfg.splits["test"],
        },
        "ledger_summary": ledger_summary(dev_validation),
        "benchmarks": benchmark_summary(dev_validation),
        "robustness": robustness_summary(dev_validation),
        "bootstrap": day_block_bootstrap(
            dev_validation.group_by("session_date").agg(total_pnl=pl.col("pnl").sum()).sort("session_date")["total_pnl"].to_list(),
            n_bootstrap=2000,
            seed=cfg.seed,
        ),
        "decision": {
            "status": "reject",
            "reason": "development-only signal failed the Phase 7 hypothesis and the Phase 8 backtest remains negative on dev+validation data.",
            "open_test_split": True,
        },
    }

    out_path = OUT_DIR / "facts_step09.json"
    out_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"Wrote Phase 9 summary to {out_path}")


if __name__ == "__main__":
    main()
