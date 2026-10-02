"""Phase 4 driver: reconstruct top-of-book (BBO) from raw MBO events and re-run return stats on the mid.

This is a thin I/O-boundary script. It reads only the raw parquet, uses the pure functional
order-book replay in options_research.mbo.book, and writes derived parquet + summary JSON to
`data/derived/` and `outputs/phase04/`. It never overwrites the raw file.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import matplotlib  # noqa: E402
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

from options_research import memguard  # noqa: E402
from options_research.io import lazyio  # noqa: E402
from options_research.mbo.book import OrderEvent, replay_stream  # noqa: E402
from options_research.stats import timeseries as ts  # noqa: E402

RAW_PATH = ROOT / "data" / "databento_options_clean.parquet"
OUT_DIR = ROOT / "outputs" / "phase04"
DEFAULT_SYMBOL = "FCEU SI 20260615 PS"


def _ts_ns(value):
    if value is None:
        return 0
    if isinstance(value, (int, np.integer)):
        return int(value)
    if hasattr(value, "timestamp"):
        return int(value.timestamp() * 1_000_000_000)
    return int(value)


def session_dates_for_symbol(symbol: str) -> list[str]:
    df = (
        lazyio.scan(RAW_PATH, columns=["session_date", "symbol"])
        .filter(pl.col("symbol") == symbol)
        .select("session_date")
        .unique()
        .collect(engine="streaming")
    )
    return df["session_date"].cast(str).to_list()


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
        if row["symbol"] is None:
            continue
        yield OrderEvent(
            ts_recv=_ts_ns(row["ts_recv"]),
            action=str(row["action"] or ""),
            order_id=str(row["order_id"] or ""),
            side=(str(row["side"]) if row["side"] is not None else None),
            price=float(row["price"]) if row["price"] is not None else None,
            size=int(row["size"]) if row["size"] is not None else None,
        )


def build_bbo_for_symbol(symbol: str, max_sessions: int | None = None) -> pl.DataFrame:
    rows: list[dict] = []
    for i, session_date in enumerate(session_dates_for_symbol(symbol), start=1):
        if max_sessions is not None and i > max_sessions:
            break
        events = iter_order_events_for_session(symbol, session_date)
        for bbo in replay_stream(events):
            rows.append(
                {
                    "session_date": session_date,
                    "symbol": symbol,
                    "ts_recv": bbo.ts_recv,
                    "best_bid": bbo.best_bid,
                    "best_bid_size": bbo.best_bid_size,
                    "best_ask": bbo.best_ask,
                    "best_ask_size": bbo.best_ask_size,
                    "mid_price": None if bbo.best_bid is None or bbo.best_ask is None else (bbo.best_bid + bbo.best_ask) / 2.0,
                }
            )
        memguard.check(f"phase04.{symbol}.{session_date}")
    if not rows:
        return pl.DataFrame(schema={"session_date": pl.String, "symbol": pl.String, "ts_recv": pl.Int64, "best_bid": pl.Float64, "best_bid_size": pl.Int64, "best_ask": pl.Float64, "best_ask_size": pl.Int64, "mid_price": pl.Float64})
    return pl.DataFrame(rows).sort(["session_date", "ts_recv"])


def compute_mid_returns(bbo_df: pl.DataFrame) -> tuple[np.ndarray, dict]:
    mid = np.asarray(bbo_df.filter(pl.col("mid_price").is_not_null())["mid_price"], dtype=float)
    rets = ts.log_returns(mid)
    adf = ts.adf_stationarity(rets)
    acf = ts.acf_values(rets, nlags=min(20, max(1, len(rets) - 1))) if len(rets) > 1 else np.array([1.0])
    return rets, {
        "n_mid_points": int(len(mid)),
        "n_return_points": int(len(rets)),
        "mid_price_adf": ts.adf_stationarity(mid)._asdict(),
        "log_return_adf": adf._asdict(),
        "acf_first_6_lags": acf[:6].tolist(),
    }


def plot_returns(bbo_df: pl.DataFrame, symbol: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    mid_df = bbo_df.filter(pl.col("mid_price").is_not_null()).sort("ts_recv")
    mid_vals = np.asarray(mid_df["mid_price"], dtype=float)
    times = np.asarray(mid_df["ts_recv"], dtype=np.int64)
    rets = ts.log_returns(mid_vals)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(times, mid_vals, lw=0.8)
    ax.set_title(f"Reconstructed BBO mid for {symbol}")
    ax.set_xlabel("ts_recv (ns)")
    ax.set_ylabel("mid")
    fig.tight_layout()
    fig.savefig(out_dir / "01_bbo_mid.png", dpi=120)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(times[1:], rets, lw=0.6, color="tab:orange")
    ax.set_title(f"BBO log returns for {symbol}")
    ax.set_xlabel("ts_recv (ns)")
    ax.set_ylabel("log return")
    fig.tight_layout()
    fig.savefig(out_dir / "02_bbo_mid_log_returns.png", dpi=120)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(rets, bins=80)
    ax.set_title(f"BBO log-return distribution for {symbol}")
    fig.tight_layout()
    fig.savefig(out_dir / "03_bbo_mid_return_hist.png", dpi=120)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 4: reconstruct BBO from the raw MBO feed.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="contract symbol to reconstruct")
    parser.add_argument("--out-dir", default=str(OUT_DIR), help="output directory for phase 04 artifacts")
    parser.add_argument("--max-sessions", type=int, default=None, help="optional cap to test one/few sessions before full run")
    args = parser.parse_args()

    symbol = args.symbol
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.max_sessions is not None:
        print(f"Testing Phase 4 on the first {args.max_sessions} session(s) only...")
    print(f"Reconstructing top-of-book for {symbol!r} from raw MBO events...")
    bbo_df = build_bbo_for_symbol(symbol, max_sessions=args.max_sessions)
    out_path = lazyio.derived_path("phase04_bbo_front_future")
    lazyio.sink(bbo_df.lazy(), out_path)
    print(f"Wrote reconstructed BBO parquet to {out_path}")

    rets, stat_summary = compute_mid_returns(bbo_df)
    print("Re-run return analysis on reconstructed BBO mid:")
    print(json.dumps(stat_summary, indent=2, default=str))

    plot_returns(bbo_df, symbol, out_dir)

    facts = {
        "source": str(RAW_PATH),
        "symbol": symbol,
        "bbo_rows": int(bbo_df.height),
        "valid_mid_rows": int(bbo_df.filter(pl.col("mid_price").is_not_null()).height),
        "return_stats": stat_summary,
        "note": (
            "This is the first reconstructed mid-price analysis from the raw MBO feed; "
            "it is the correct Phase 4 BBO pass and supersedes the earlier descriptive price-event EDA."
        ),
    }
    (out_dir / "facts_step04.json").write_text(json.dumps(facts, indent=2, default=str))
    print(f"Wrote summary facts to {out_dir / 'facts_step04.json'}")


if __name__ == "__main__":
    main()
