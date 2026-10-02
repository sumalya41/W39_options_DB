"""Lazy polars I/O helpers. Every function takes a path/LazyFrame and returns a new
LazyFrame/DataFrame — never mutates, never collects eagerly unless named ``collect_*``.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Optional

os.environ.setdefault("POLARS_MAX_THREADS", "4")
import polars as pl

RAW_DATA_NAME = "databento_options_clean.parquet"  # the one file sink() refuses to overwrite


class RawDataWriteError(RuntimeError):
    pass


def scan(path: str | Path, columns: Optional[Iterable[str]] = None) -> pl.LazyFrame:
    """Lazy scan with optional column projection pushed down before any filter/join."""
    lf = pl.scan_parquet(str(path))
    if columns is not None:
        have = set(schema_names(lf))
        lf = lf.select([c for c in columns if c in have])
    return lf


def schema_names(lf: pl.LazyFrame) -> list[str]:
    try:
        return lf.collect_schema().names()
    except AttributeError:
        return lf.columns


def collect_stream(lf: pl.LazyFrame) -> pl.DataFrame:
    """Streaming collect across polars versions — the one place eager collection happens."""
    try:
        return lf.collect(engine="streaming")
    except TypeError:
        return lf.collect(streaming=True)


def derived_path(step_name: str, derived_root: str | Path = "data/derived") -> Path:
    """Standard location for any cleaned/new-column dataset (Rule 6): never the raw file."""
    return Path(derived_root) / f"{step_name}.parquet"


def sink(lf: pl.LazyFrame, path: str | Path, row_group_size: int = 1_000_000) -> Path:
    """Write a LazyFrame to parquet. Refuses to target the raw data file (Rule 6) —
    always write derived/cleaned output to a new path, e.g. via ``derived_path()``.
    """
    out = Path(path)
    if out.name == RAW_DATA_NAME:
        raise RawDataWriteError(
            f"Refusing to write to raw data file {out}. Use derived_path(step_name) instead."
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    try:
        lf.sink_parquet(str(tmp), compression="zstd", row_group_size=row_group_size)
    except Exception:
        collect_stream(lf).write_parquet(str(tmp), compression="zstd", row_group_size=row_group_size)
    os.replace(tmp, out)
    return out
