"""Memory budget functions (Kaggle/low-RAM safe). Pure computations + one guard that raises.

No module-level mutable state: every function takes what it needs as arguments or reads
process/OS state through a narrow, named function (``container_limit_bytes``, ``rss``) so the
impurity is explicit and localized, never hidden inside a class.
"""
from __future__ import annotations

import gc
import math
import os
from typing import NamedTuple

import psutil


class MemoryBudgetExceeded(RuntimeError):
    pass


def container_limit_bytes() -> int:
    """cgroup v2, then v1; fall back to host RAM."""
    for p in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            v = open(p).read().strip()
            if v.isdigit() and int(v) < (1 << 60):
                return int(v)
        except OSError:
            pass
    return psutil.virtual_memory().total


def budget_bytes(frac: float = 0.65) -> int:
    return int(container_limit_bytes() * frac)


def rss() -> int:
    return psutil.Process(os.getpid()).memory_info().rss


def is_over_budget(current_rss: int, limit_bytes: int) -> bool:
    """Pure predicate — no process inspection, easy to unit test."""
    return current_rss > limit_bytes


def check(tag: str, frac: float = 0.65) -> None:
    """Call after every chunk/session. Raises before Kaggle's OOM killer would act."""
    limit = budget_bytes(frac)
    if is_over_budget(rss(), limit):
        gc.collect()
        if is_over_budget(rss(), limit):
            raise MemoryBudgetExceeded(
                f"[{tag}] RSS {rss() / 2**30:.1f} GiB > budget {limit / 2**30:.1f} GiB."
            )


class ChunkPlan(NamedTuple):
    chunk_rows: int


def chunk_rows_for(
    bytes_per_row: float,
    peak_factor: float = 8.0,
    frac_of_budget: float = 0.25,
    lo: int = 200_000,
    hi: int = 5_000_000,
) -> ChunkPlan:
    n = int(budget_bytes() * frac_of_budget / (max(bytes_per_row, 1.0) * peak_factor))
    return ChunkPlan(max(lo, min(hi, n)))


def plan_parts(
    n_rows: int, bytes_per_row: float, peak_factor: float = 6.0, frac_of_budget: float = 0.30
) -> int:
    need = n_rows * bytes_per_row * peak_factor
    return max(1, math.ceil(need / (budget_bytes() * frac_of_budget)))
