"""Frozen, immutable run configuration. Pure data + one read-at-the-edge loader function."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict


class SignalConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    lookback_minutes: int
    threshold: Optional[float] = None


class CostsConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    fee_per_contract_per_side: Optional[float] = None
    slippage_price_units: float = 0.0


class MemoryConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    budget_fraction: float = 0.65


class LopezDePradoConfig(BaseModel):
    model_config = ConfigDict(frozen=True)
    frac_diff_d: float = 0.4
    frac_diff_thresh: float = 1e-4
    cusum_threshold: Optional[float] = None
    purge_embargo_minutes: int = 15


class Config(BaseModel):
    """Immutable: construct once, never mutate. Re-derive, don't reassign fields."""

    model_config = ConfigDict(frozen=True)

    seed: int
    tz_venue: str
    data_path: str
    exclude_sessions: list[str] = []
    splits: dict[str, Optional[list[str]]] = {}
    decision_grid_minutes: int = 1
    signal: SignalConfig
    max_abs_log_moneyness: float = 0.02
    max_rel_spread: float = 0.05
    min_size: int = 1
    quote_max_age_seconds: int = 5
    underlying_max_age_seconds: int = 5
    exec_delay_seconds: int = 1
    entry_deadline_seconds: int = 5
    holding_minutes: int = 15
    costs: CostsConfig
    multiplier: Optional[float] = None
    initial_cash: float = 100_000.0
    memory: MemoryConfig = MemoryConfig()
    lopez_de_prado: LopezDePradoConfig = LopezDePradoConfig()


def load_config(path: str | Path) -> Config:
    """The one I/O boundary in this module: read YAML, return an immutable Config."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return Config.model_validate(raw)
