"""Phase 8: run a minimal directional backtest on the development and validation windows."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import numpy as np
import polars as pl

from options_research.config import load_config
from options_research.strategy.backtest import run_simple_backtest

TABLE_PATH = ROOT / "outputs" / "phase05" / "research_table_subset.parquet"
OUT_DIR = ROOT / "outputs" / "phase08"


def main() -> None:
    cfg = load_config(ROOT / "config" / "config.yaml")
    df = pl.read_parquet(TABLE_PATH).with_columns(pl.col("session_date").cast(pl.String).alias("session_date"))
    dev_and_validation = df.filter(pl.col("session_date").is_in(cfg.splits["development"] + cfg.splits["validation"]))
    if dev_and_validation.is_empty():
        raise ValueError("No development/validation rows available for the Phase 8 backtest.")

    # Development-calibrated threshold from the 90th percentile of the absolute 5-minute return.
    ret_series = (
        dev_and_validation.sort(["session_date", "contract_key", "decision_ts"])
        .with_columns(pl.col("mid_price").shift(5).over(["session_date", "contract_key"]).alias("mid_price_lag5"))
        .with_columns(
            pl.when(pl.col("mid_price_lag5").is_not_null() & (pl.col("mid_price_lag5") > 0))
            .then((pl.col("mid_price") / pl.col("mid_price_lag5")).log())
            .otherwise(None)
            .alias("future_return_5m")
        )
        ["future_return_5m"].drop_nulls()
    )
    ret = np.asarray(ret_series.to_numpy(), dtype=float)
    threshold = float(np.nanpercentile(np.abs(ret), 90.0)) if len(ret) else 0.0

    ledger = run_simple_backtest(dev_and_validation, threshold=threshold, holding_minutes=cfg.holding_minutes)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ledger_path = OUT_DIR / "ledger.parquet"
    ledger.write_parquet(ledger_path)

    summary = {
        "threshold_abs_5m_return_90pct": threshold,
        "n_trade_decisions": int(ledger.height),
        "gross_pnl": float(ledger["pnl"].sum()) if ledger.height else 0.0,
        "mean_pnl": float(ledger["pnl"].mean()) if ledger.height else 0.0,
        "win_rate": float((ledger["pnl"] > 0).sum() / ledger.height) if ledger.height else 0.0,
        "sessions": ledger["session_date"].unique().sort().to_list(),
        "sample_rows": ledger.head(5).to_dicts(),
    }
    out_path = OUT_DIR / "facts_step08.json"
    out_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"Wrote Phase 8 ledger to {ledger_path}")


if __name__ == "__main__":
    main()
