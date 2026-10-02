"""Phase 5 driver: build the causal research table for a small development-only subset.

This is deliberately bounded to a small candidate universe so it remains runnable on a local
workstation while still exercising the real Eurex logic: one-minute decision grid, backward-only
as-of joins, and the feature set required by the playbook. The raw parquet remains read-only.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import polars as pl

from options_research.config import load_config
from options_research.io import lazyio
from options_research.mbo.book import OrderEvent, replay_stream
from options_research.research.grid import build_decision_grid_ns, build_research_table

RAW_PATH = ROOT / "data" / "databento_options_clean.parquet"
SIMB_MAP_PATH = ROOT / "data" / "derived" / "phase02_symbology_map.parquet"
UNDERLYING_PATH = ROOT / "data" / "derived" / "phase04_bbo_front_future.parquet"
OUT_DIR = ROOT / "outputs" / "phase05"


def _ts_ns(value):
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if hasattr(value, "timestamp"):
        return int(value.timestamp() * 1_000_000_000)
    return int(value)


def iter_order_events_for_session(symbol: str, session_date: str):
    df = (
        lazyio.scan(
            RAW_PATH,
            columns=[
                "ts_recv",
                "symbol",
                "session_date",
                "publisher_id",
                "sequence",
                "order_id",
                "action",
                "side",
                "price",
                "size",
            ],
        )
        .filter((pl.col("symbol") == symbol) & (pl.col("session_date") == session_date))
        .sort(["publisher_id", "sequence", "ts_recv"])
        .collect(engine="streaming")
    )

    for row in df.iter_rows(named=True):
        yield OrderEvent(
            ts_recv=_ts_ns(row["ts_recv"]),
            action=str(row["action"] or ""),
            order_id=str(row["order_id"] or ""),
            side=str(row["side"]) if row["side"] is not None else None,
            price=float(row["price"]) if row["price"] is not None else None,
            size=int(row["size"]) if row["size"] is not None else None,
        )


def candidate_option_symbols(max_contracts: int = 5) -> list[str]:
    cfg = load_config(ROOT / "config" / "config.yaml")
    dev_sessions = cfg.splits["development"]
    meta = pl.read_parquet(SIMB_MAP_PATH).filter(pl.col("session_date").is_in(dev_sessions))
    counts = (
        meta.filter(pl.col("kind") == "option")
        .group_by("symbol")
        .agg(n_rows=pl.len())
        .sort("n_rows", descending=True)
        .head(max_contracts)
    )
    return counts["symbol"].to_list()


def build_option_bbo_for_symbols(symbols: list[str], sessions: list[str]) -> pl.DataFrame:
    rows: list[dict] = []
    symbol_meta = pl.read_parquet(SIMB_MAP_PATH).filter(pl.col("symbol").is_in(symbols))
    for session in sessions:
        for symbol in symbols:
            for bbo in replay_stream(iter_order_events_for_session(symbol, session), memcheck_every=200_000):
                if bbo.best_bid is None or bbo.best_ask is None:
                    continue
                rows.append(
                    {
                        "session_date": session,
                        "symbol": symbol,
                        "ts_recv": bbo.ts_recv,
                        "best_bid": bbo.best_bid,
                        "best_ask": bbo.best_ask,
                        "mid_price": (bbo.best_bid + bbo.best_ask) / 2.0,
                    }
                )

    bbo = pl.DataFrame(rows)
    if bbo.is_empty():
        return pl.DataFrame(schema={"session_date": pl.String, "symbol": pl.String, "ts_recv": pl.Int64, "best_bid": pl.Float64, "best_ask": pl.Float64, "mid_price": pl.Float64})

    return bbo.join(
        symbol_meta.select(["session_date", "symbol", "contract_key", "strike", "expiry"]),
        on=["session_date", "symbol"],
        how="left",
    )


def build_phase5_research_table(max_contracts: int = 5) -> pl.DataFrame:
    cfg = load_config(ROOT / "config" / "config.yaml")
    sessions = cfg.splits["development"]
    symbols = candidate_option_symbols(max_contracts=max_contracts)
    option_bbo = build_option_bbo_for_symbols(symbols, sessions)
    if option_bbo.is_empty():
        raise ValueError(f"No option BBO rows generated for development sessions {sessions!r}.")

    underlying = pl.read_parquet(UNDERLYING_PATH).filter(pl.col("session_date").is_in(sessions))
    grid_rows: list[dict] = []
    for session in sessions:
        session_date = __import__("datetime").date.fromisoformat(session)
        session_grid = build_decision_grid_ns(session_date, "09:00", "17:00", int(cfg.decision_grid_minutes))
        for contract_key in option_bbo.filter(pl.col("session_date") == session)["contract_key"].unique().to_list():
            for row in session_grid.iter_rows(named=True):
                grid_rows.append({"session_date": session, "contract_key": contract_key, "decision_ts": row["decision_ts"]})

    grid = pl.DataFrame(grid_rows)
    res = build_research_table(
        grid=grid,
        option_quotes=option_bbo,
        underlying_quotes=underlying,
        quote_max_age_seconds=cfg.quote_max_age_seconds,
        session_date_col="session_date",
        decision_ts_col="decision_ts",
        option_ts_col="ts_recv",
        option_bid_col="best_bid",
        option_ask_col="best_ask",
        option_strike_col="strike",
        option_expiry_col="expiry",
        underlying_ts_col="ts_recv",
        underlying_mid_col="mid_price",
        by_cols=["session_date", "contract_key"],
    )
    return res


def main() -> None:
    parser = __import__("argparse").ArgumentParser(description="Phase 5: build a causal research table on development sessions.")
    parser.add_argument("--max-contracts", type=int, default=5, help="Maximum number of option contracts to include in the subset research table.")
    parser.add_argument("--out-dir", type=str, default=str(OUT_DIR), help="Output directory for the Phase 5 artifacts.")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    table = build_phase5_research_table(max_contracts=args.max_contracts)
    table_path = out_dir / "research_table_subset.parquet"
    table.write_parquet(table_path)

    summary = {
        "rows": int(table.height),
        "contract_count": int(table["contract_key"].n_unique()) if "contract_key" in table.columns else 0,
        "coverage_by_contract": table.group_by("contract_key").agg(rows=pl.len(), eligible=pl.col("entry_eligible").sum()).sort("rows", descending=True).to_dicts(),
        "sample_rows": table.head(3).to_dicts(),
    }
    (out_dir / "facts_step05.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"Wrote subset research table to {table_path}")


if __name__ == "__main__":
    main()
