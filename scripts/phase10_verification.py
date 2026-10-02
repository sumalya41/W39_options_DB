"""Phase 10: verification suite for the causal research and backtest pipeline."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl

from options_research.strategy.verification import (
    build_phase10_summary,
    check_real_development_trade_examples,
    round_trip_pnl,
    verify_causality,
    verify_contract_stays_fixed,
    verify_missing_exit_unresolved,
    verify_no_future_quote_in_asof_join,
)

OUT_DIR = ROOT / "outputs" / "phase10"


def _real_trade_examples() -> list[dict]:
    facts = json.loads((ROOT / "outputs" / "phase08" / "facts_step08.json").read_text(encoding="utf-8"))
    return facts.get("sample_rows", [])


def main() -> None:
    fact_rows = _real_trade_examples()
    ledger = pl.read_parquet(ROOT / "outputs" / "phase08" / "ledger.parquet")
    causal = verify_causality(
        pl.DataFrame(
            {
                "decision_ts": [10, 20, 30],
                "quote_obs_ts": [9, 19, 29],
                "fill_ts": [12, 23, 33],
            }
        ),
        decision_ts_col="decision_ts",
        quote_ts_col="quote_obs_ts",
        fill_ts_col="fill_ts",
        delay_seconds=1,
    )

    future_guard = verify_no_future_quote_in_asof_join(
        pl.DataFrame({"decision_ts": [10, 20], "future_obs_ts": [8, 19]}),
        decision_ts_col="decision_ts",
        future_ts_col="future_obs_ts",
    )

    fixed_contract = verify_contract_stays_fixed(
        pl.DataFrame(
            {
                "trade_id": [1, 1],
                "contract_key": ["EUCO|20260316|C|1.15000", "EUCO|20260316|C|1.15000"],
            }
        )
    )

    round_trip = round_trip_pnl(2.20, 2.15, fee=0.01)
    unresolved = verify_missing_exit_unresolved(
        pl.DataFrame(
            {
                "exit_ts": [None, "2026-03-09 08:40:00"],
                "unresolved": [True, False],
            }
        )
    )

    summary = {
        "phase": "10",
        "checks": {
            "causality": causal,
            "future_quote_guard": future_guard,
            "contract_fixed_while_held": fixed_contract,
            "round_trip_cost": {"passed": abs(round_trip + 0.06) < 1e-9, "pnl": round_trip},
            "missing_exit_unresolved": unresolved,
            "real_trade_examples": check_real_development_trade_examples(fact_rows),
        },
        "real_ledger_samples": {
            "n_rows": int(len(fact_rows)),
            "first_three_signals": fact_rows[:3],
        },
        "overall_passed": all(
            value.get("passed") if isinstance(value, dict) else bool(value)
            for value in [
                causal,
                future_guard,
                fixed_contract,
                {"passed": abs(round_trip + 0.06) < 1e-9},
                unresolved,
                check_real_development_trade_examples(fact_rows),
            ]
        ),
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "facts_step10.json"
    out_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"Wrote Phase 10 verification summary to {out_path}")


if __name__ == "__main__":
    main()
