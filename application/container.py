"""Dependency container for discovery workflow services."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, Type, TypeVar

from database import DatabaseManager
from utils.cache import InMemoryCache
from utils.logging import get_logger
from .configuration import AppSettings

if TYPE_CHECKING:  # pragma: no cover - type hinting only
    from data_loader import HistoricalDataLoader
    from data_loader_optimized import OptimizedDataLoader
    from phase1.backtest_engine import SimpleBacktestEngine
    from phase1.enhanced_backtest_engine import EnhancedBacktestEngine
    from phase1.explorer import SingleFactorExplorer
    from phase1.parallel_explorer import ParallelFactorExplorer
    from phase2.combiner import MultiFactorCombiner
    from utils.factor_cache import FactorCache

T = TypeVar("T")


class ServiceContainer:
    """Simple service locator with lazy initialisation."""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self._instances: Dict[str, object] = {}
        self._logger = get_logger(__name__, settings.log_level)

    def _get(self, key: str, factory: Callable[[], T]) -> T:
        if key not in self._instances:
            self._instances[key] = factory()
        return self._instances[key]  # type: ignore[return-value]

    def resolve(self, typ: Type[T], factory: Callable[[], T]) -> T:
        return self._get(typ.__name__, factory)

    # ------------------------------------------------------------------
    def database(self) -> DatabaseManager:
        return self.resolve(DatabaseManager, lambda: DatabaseManager(self.settings.db_path))

    def data_loader(self) -> "HistoricalDataLoader":
        if self.settings.parallel_mode == "process":
            from data_loader_optimized import OptimizedDataLoader as LoaderType

            def factory() -> LoaderType:
                cache = InMemoryCache()
                cache_dir = None
                if self.settings.data_root is not None:
                    cache_dir = Path(self.settings.data_root) / ".optimized_cache"
                return LoaderType(
                    data_root=self.settings.data_root,
                    cache_backend=cache,
                    cache_ttl=self.settings.cache_ttl,
                    max_workers=self.settings.max_workers,
                    cache_dir=cache_dir,
                )

            return self.resolve(LoaderType, factory)

        from data_loader import HistoricalDataLoader as LoaderType

        def factory() -> LoaderType:
            cache = InMemoryCache()
            return LoaderType(
                data_root=self.settings.data_root,
                cache_backend=cache,
                cache_ttl=self.settings.cache_ttl,
            )

        return self.resolve(LoaderType, factory)

    def backtest_engine(self) -> "SimpleBacktestEngine":
        if self.settings.parallel_mode == "process":
            from phase1.enhanced_backtest_engine import EnhancedBacktestEngine

            return EnhancedBacktestEngine(self.settings.symbol)

        from phase1.backtest_engine import SimpleBacktestEngine

        return SimpleBacktestEngine(self.settings.symbol)

    def factor_cache(self) -> "FactorCache":
        from utils.factor_cache import FactorCache

        return self.resolve(FactorCache, FactorCache)

    def factor_explorer(self) -> "SingleFactorExplorer":
        if self.settings.parallel_mode == "process":
            from phase1.parallel_explorer import ParallelFactorExplorer as ExplorerType
            from phase1.enhanced_backtest_engine import create_enhanced_backtest_engine

            def factory() -> ExplorerType:
                loader = self.data_loader()
                cache = self.factor_cache()
                return ExplorerType(
                    self.settings.symbol,
                    data_loader=loader,  # type: ignore[arg-type]
                    factor_cache=cache,
                    backtest_engine_factory=create_enhanced_backtest_engine,
                    max_workers=self.settings.max_workers,
                    memory_limit_mb=self.settings.memory_limit_mb,
                    logger=self._logger,
                )

            return self.resolve(ExplorerType, factory)

        from phase1.explorer import SingleFactorExplorer as ExplorerType

        def factory() -> ExplorerType:
            loader = self.data_loader()
            engine = self.backtest_engine()
            return ExplorerType(
                self.settings.symbol,
                data_loader=loader,
                backtest_engine=engine,
            )

        return self.resolve(ExplorerType, factory)

    def factor_combiner(self, phase1_results: Dict[str, Dict[str, object]]) -> "MultiFactorCombiner":
        from phase2.combiner import MultiFactorCombiner as CombinerType

        combiner_config = getattr(self.settings, "combiner", None)
        if combiner_config is None:
            combiner_config = getattr(self.settings, "combiner_config", None)

        return CombinerType(
            symbol=self.settings.symbol,
            phase1_results=phase1_results,
            config=combiner_config,
            data_loader=self.data_loader(),
        )

    def logger(self):
        return self._logger


__all__ = ["ServiceContainer"]
