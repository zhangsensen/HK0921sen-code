"""Hong Kong short-term factor discovery package."""
from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "DEFAULT_TIMEFRAMES",
    "HistoricalDataLoader",
    "SingleFactorExplorer",
    "MultiFactorCombiner",
    "DatabaseManager",
]


def __getattr__(name: str) -> Any:  # pragma: no cover - thin wrapper
    if name == "DEFAULT_TIMEFRAMES":
        return import_module("hk_factor_discovery.config").DEFAULT_TIMEFRAMES
    if name == "HistoricalDataLoader":
        return import_module("hk_factor_discovery.data_loader").HistoricalDataLoader
    if name == "SingleFactorExplorer":
        return import_module("hk_factor_discovery.phase1.explorer").SingleFactorExplorer
    if name == "MultiFactorCombiner":
        return import_module("hk_factor_discovery.phase2.combiner").MultiFactorCombiner
    if name == "DatabaseManager":
        return import_module("hk_factor_discovery.database").DatabaseManager
    raise AttributeError(name)
