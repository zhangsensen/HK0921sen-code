"""Core configuration values for the HK factor discovery system."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional


DEFAULT_TIMEFRAMES: List[str] = [
    "1m",
    "2m",
    "3m",
    "5m",
    "10m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "1d",
]


@dataclass(frozen=True)
class CombinerConfig:
    """Configuration for the multi-factor combiner."""

    top_n: int = 20
    max_factors: int = 3
    max_combinations: Optional[int] = 1000
    min_sharpe: float = 0.0
    min_information_coefficient: float = 0.0


def timeframe_sort_key(timeframe: str) -> Iterable[int]:
    """Sort timeframes by unit and magnitude."""

    unit_order = {"m": 0, "h": 1, "d": 2}
    value = int("".join(ch for ch in timeframe if ch.isdigit()))
    unit = timeframe[-1]
    return unit_order.get(unit, 99), value
