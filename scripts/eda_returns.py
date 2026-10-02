"""Exploratory EDA driver: visualize raw data coverage + return-based time-series stats.

PRELIMINARY / DESCRIPTIVE PASS: uses raw add/trade price events for the front (most active)
future contract as a proxy price series, because Phase 4's BBO reconstruction has not run yet.
Labelled as such in every output and in facts_eda_returns.json. Per the ABSOLUTE RULES in
todo.md: every regression-relevant series is a LOG RETURN (Rule 3), not a raw price level
(except the one explicitly-labelled price-level sanity plot); stationarity/ACF are checked
before any statistic is trusted (Rule 2); López de Prado fractional differentiation and a
CUSUM filter are shown alongside the plain return series (Rule 4); only projected, streaming
scans are used and memguard.check() runs after every heavy step (Rule 5); no raw file is ever
written to (Rule 6) — plots and facts go to outputs/, nothing touches data/derived/ here.
"""
from __future__ import annotations

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
from options_research.stats import lopez_de_prado as ldp  # noqa: E402
from options_research.stats import timeseries as ts  # noqa: E402

DATA_PATH = ROOT / "data" / "databento_options_clean.parquet"
SYMBOLOGY_PATH = ROOT / "data" / "derived" / "phase02_symbology_map.parquet"
OUT_DIR = ROOT / "outputs" / "eda_returns"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    facts: dict = {
        "data_path": str(DATA_PATH),
        "note": (
            "PRELIMINARY: built from raw add/trade price events on the front future contract, "
            "not the reconstructed BBO mid (Phase 4 not yet run). Descriptive/exploratory only."
        ),
    }

    print("Loading future symbols from the Phase 2 symbology map (derived, not raw)...")
    future_symbols = (
        pl.scan_parquet(str(SYMBOLOGY_PATH))
        .filter(pl.col("kind") == "future")
        .select("symbol")
        .unique()
        .collect()["symbol"]
        .to_list()
    )

    print("Finding the most active future symbol (row count)...")
    future_counts = (
        lazyio.scan(DATA_PATH, columns=["symbol", "session_date"])
        .filter(pl.col("symbol").is_in(future_symbols))
        .group_by("symbol")
        .agg(n=pl.len(), n_sessions=pl.col("session_date").n_unique())
        .sort("n", descending=True)
        .collect(engine="streaming")
    )
    front_symbol = future_counts["symbol"][0]
    facts["front_future_symbol"] = front_symbol
    facts["front_future_row_count"] = int(future_counts["n"][0])
    memguard.check("eda.future_counts")

    print(f"Building 1-minute last-price bars for {front_symbol!r}...")
    bars = (
        lazyio.scan(DATA_PATH, columns=["ts_recv", "symbol", "price", "action"])
        .filter(
            (pl.col("symbol") == front_symbol)
            & pl.col("price").is_not_null()
            & pl.col("action").is_in(["A", "T", "F"])
        )
        .sort("ts_recv")
        .with_columns(minute=pl.col("ts_recv").dt.truncate("1m"))
        .group_by("minute")
        .agg(price=pl.col("price").last())
        .sort("minute")
        .collect(engine="streaming")
    )
    facts["n_price_bars"] = bars.height
    memguard.check("eda.bars")

    prices = bars["price"].to_numpy()
    minutes = bars["minute"].to_list()

    print("Computing log returns (Rule 3) + stationarity/ACF (Rule 2)...")
    returns = ts.log_returns(prices)
    price_stationarity = ts.adf_stationarity(prices)
    return_stationarity = ts.adf_stationarity(returns)
    acf_vals = ts.acf_values(returns, nlags=20)
    facts["price_level_adf"] = price_stationarity._asdict()
    facts["log_return_adf"] = return_stationarity._asdict()
    facts["log_return_acf_first_6_lags"] = acf_vals[:6].tolist()

    print("Lopez de Prado: fractional differentiation + CUSUM filter...")
    frac = ldp.frac_diff(prices, d=0.4)
    frac_valid = frac[~np.isnan(frac)]
    frac_stationarity = ts.adf_stationarity(frac_valid)
    cusum_threshold = float(np.nanstd(returns) * 3)  # exploratory only, not dev-calibrated
    cusum_events = ldp.cusum_filter(returns, threshold=cusum_threshold)
    facts["frac_diff_d"] = 0.4
    facts["frac_diff_adf"] = frac_stationarity._asdict()
    facts["cusum_threshold_exploratory"] = cusum_threshold
    facts["cusum_n_events"] = int(len(cusum_events))
    memguard.check("eda.stats")

    print("Plotting (Agg backend, saved as PNG — no display)...")
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(minutes, prices, lw=0.6)
    ax.set_title(f"{front_symbol} — 1-min last price (DESCRIPTIVE ONLY, not a regression input)")
    ax.set_xlabel("time"); ax.set_ylabel("price")
    fig.tight_layout(); fig.savefig(OUT_DIR / "01_price_level_descriptive.png", dpi=120); plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(minutes[1:], returns, lw=0.5, color="tab:orange")
    ax.set_title(f"{front_symbol} — 1-min log returns (ADF p={return_stationarity.p_value:.4f})")
    ax.set_xlabel("time"); ax.set_ylabel("log return")
    fig.tight_layout(); fig.savefig(OUT_DIR / "02_log_returns.png", dpi=120); plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(returns, bins=80)
    ax.set_title("Log return distribution (1-min grid)")
    fig.tight_layout(); fig.savefig(OUT_DIR / "03_log_return_histogram.png", dpi=120); plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.stem(range(len(acf_vals)), acf_vals)
    ax.set_title("Log return ACF (event-time 1-min grid)")
    ax.set_xlabel("lag (minutes)"); ax.set_ylabel("autocorrelation")
    fig.tight_layout(); fig.savefig(OUT_DIR / "04_acf.png", dpi=120); plt.close(fig)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(minutes, frac, lw=0.6)
    ax.set_title(f"L\u00f3pez de Prado fractional differentiation (d=0.4), ADF p={frac_stationarity.p_value:.4f}")
    fig.tight_layout(); fig.savefig(OUT_DIR / "05_frac_diff.png", dpi=120); plt.close(fig)

    coverage = (
        lazyio.scan(DATA_PATH, columns=["session_date"])
        .group_by("session_date")
        .agg(n=pl.len())
        .sort("session_date")
        .collect(engine="streaming")
    )
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(coverage["session_date"].to_list(), coverage["n"].to_list())
    ax.set_title("Row count by session_date (all symbols, all events)")
    ax.tick_params(axis="x", rotation=60)
    fig.tight_layout(); fig.savefig(OUT_DIR / "06_session_coverage.png", dpi=120); plt.close(fig)

    (OUT_DIR / "facts_eda_returns.json").write_text(json.dumps(facts, indent=2, default=str))
    print(json.dumps(facts, indent=2, default=str))
    print(f"\nWrote plots + facts to {OUT_DIR}")


if __name__ == "__main__":
    main()
