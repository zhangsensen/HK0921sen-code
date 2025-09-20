"""Compatibility namespace for legacy phase1 imports."""
from __future__ import annotations

from importlib import import_module
from typing import Any, Dict, Tuple

__all__ = ["SingleFactorExplorer", "SimpleBacktestEngine", "EnhancedBacktestEngine", "ParallelFactorExplorer"]

_LOOKUP: Dict[str, Tuple[str, str]] = {
    "SingleFactorExplorer": ("hk_factor_discovery.phase1.explorer", "SingleFactorExplorer"),
    "SimpleBacktestEngine": ("hk_factor_discovery.phase1.backtest_engine", "SimpleBacktestEngine"),
    "EnhancedBacktestEngine": (
        "hk_factor_discovery.phase1.enhanced_backtest_engine",
        "EnhancedBacktestEngine",
    ),
    "ParallelFactorExplorer": (
        "hk_factor_discovery.phase1.parallel_explorer",
        "ParallelFactorExplorer",
    ),
}


def __getattr__(name: str) -> Any:  # pragma: no cover
    try:
        module_name, attr_name = _LOOKUP[name]
    except KeyError as exc:  # pragma: no cover
        raise AttributeError(name) from exc
    module = import_module(module_name)
    return getattr(module, attr_name)


__all__ = sorted(__all__)
