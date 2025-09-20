"""Phase 1 factor exploration toolkit."""
from .backtest_engine import SimpleBacktestEngine

try:  # pragma: no cover - optional heavy dependencies
    from .enhanced_backtest_engine import EnhancedBacktestEngine, create_enhanced_backtest_engine
except ModuleNotFoundError:  # pragma: no cover
    EnhancedBacktestEngine = None  # type: ignore[assignment]

    def create_enhanced_backtest_engine(*_: object, **__: object):  # type: ignore[override]
        raise ModuleNotFoundError("EnhancedBacktestEngine requires numpy/pandas")

try:  # pragma: no cover - optional heavy dependencies
    from .explorer import SingleFactorExplorer
except ModuleNotFoundError:  # pragma: no cover
    SingleFactorExplorer = None  # type: ignore[assignment]

from .parallel_explorer import ParallelFactorExplorer

__all__ = ["SimpleBacktestEngine", "ParallelFactorExplorer"]
if EnhancedBacktestEngine is not None:
    __all__.extend(["EnhancedBacktestEngine", "create_enhanced_backtest_engine"])
if SingleFactorExplorer is not None:
    __all__.append("SingleFactorExplorer")
