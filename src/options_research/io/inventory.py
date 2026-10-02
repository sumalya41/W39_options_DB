"""Read-only inventory functions: schema, sample rows, null counts. Each returns a new
DataFrame/dict; none mutate the source file or any passed-in object.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl


def describe_schema(path: str | Path) -> pl.DataFrame:
    con = duckdb.connect()
    return con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").pl()


def row_count(path: str | Path) -> int:
    con = duckdb.connect()
    return con.execute(f"SELECT count(*) AS n FROM read_parquet('{path}')").pl()["n"][0]


def sample_rows(path: str | Path, n: int = 5) -> pl.DataFrame:
    return pl.scan_parquet(str(path)).limit(n).collect()


def null_counts(path: str | Path, columns: list[str] | None = None) -> pl.DataFrame:
    lf = pl.scan_parquet(str(path))
    cols = columns or lf.collect_schema().names()
    exprs = [pl.col(c).null_count().alias(c) for c in cols]
    return lf.select(exprs).collect(engine="streaming")


def value_counts(path: str | Path, column: str, limit: int = 50) -> pl.DataFrame:
    return (
        pl.scan_parquet(str(path))
        .group_by(column)
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .limit(limit)
        .collect(engine="streaming")
    )
