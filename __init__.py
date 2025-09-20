"""Compatibility wrapper for the hk_factor_discovery package."""
from __future__ import annotations

from importlib import import_module
from typing import Any, Dict, Tuple

__all__ = [
    "DEFAULT_TIMEFRAMES",
    "HistoricalDataLoader",
    "OptimizedDataLoader",
    "SingleFactorExplorer",
    "ParallelFactorExplorer",
    "EnhancedBacktestEngine",
    "MultiFactorCombiner",
    "DatabaseManager",
    "FactorCache",
]

_LOOKUP: Dict[str, Tuple[str, str]] = {
    "DEFAULT_TIMEFRAMES": ("hk_factor_discovery.config", "DEFAULT_TIMEFRAMES"),
    "HistoricalDataLoader": ("hk_factor_discovery.data_loader", "HistoricalDataLoader"),
    "OptimizedDataLoader": ("hk_factor_discovery.data_loader_optimized", "OptimizedDataLoader"),
    "SingleFactorExplorer": ("hk_factor_discovery.phase1.explorer", "SingleFactorExplorer"),
    "ParallelFactorExplorer": (
        "hk_factor_discovery.phase1.parallel_explorer",
        "ParallelFactorExplorer",
    ),
    "EnhancedBacktestEngine": (
        "hk_factor_discovery.phase1.enhanced_backtest_engine",
        "EnhancedBacktestEngine",
    ),
    "MultiFactorCombiner": ("hk_factor_discovery.phase2.combiner", "MultiFactorCombiner"),
    "DatabaseManager": ("hk_factor_discovery.database", "DatabaseManager"),
    "FactorCache": ("hk_factor_discovery.utils.factor_cache", "FactorCache"),
}


def __getattr__(name: str) -> Any:  # pragma: no cover - thin wrapper
    try:
        module_name, attr_name = _LOOKUP[name]
    except KeyError as exc:  # pragma: no cover - preserve AttributeError semantics
        raise AttributeError(name) from exc
    module = import_module(module_name)
    return getattr(module, attr_name)


__all__ = sorted(__all__)
