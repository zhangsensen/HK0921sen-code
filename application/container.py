"""Dependency container for discovery workflow services."""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, Type, TypeVar

from ..database import DatabaseManager
from ..utils.cache import InMemoryCache
from ..utils.logging import get_logger
from .configuration import AppSettings

if TYPE_CHECKING:  # pragma: no cover - type hinting only
    from ..data_loader import HistoricalDataLoader
    from ..phase1.backtest_engine import SimpleBacktestEngine
    from ..phase1.explorer import SingleFactorExplorer
    from ..phase2.combiner import MultiFactorCombiner

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
        from ..data_loader import OptimizedDataLoader as LoaderType

        def factory() -> LoaderType:
            cache = InMemoryCache()
            return LoaderType(
                data_root=self.settings.data_root,
                cache_backend=cache,
                cache_ttl=self.settings.cache_ttl,
                cache_dir=self.settings.data_cache_dir,
                max_workers=self.settings.loader_max_workers,
                enable_preload=self.settings.enable_preload,
                enable_disk_cache=self.settings.enable_disk_cache,
            )

        return self.resolve(LoaderType, factory)

    def backtest_engine(self) -> "SimpleBacktestEngine":
        from ..phase1.backtest_engine import SimpleBacktestEngine

        return SimpleBacktestEngine(self.settings.symbol)

    def factor_explorer(self) -> "SingleFactorExplorer":
        from ..phase1.explorer import SingleFactorExplorer as ExplorerType

        def factory() -> ExplorerType:
            loader = self.data_loader()
            engine = self.backtest_engine()
            return ExplorerType(
                self.settings.symbol,
                data_loader=loader,
                backtest_engine=engine,
                use_preload=self.settings.enable_preload,
                use_batch_loading=self.settings.enable_batch_loading,
            )

        return self.resolve(ExplorerType, factory)

    def factor_combiner(self, phase1_results: Dict[str, Dict[str, object]]) -> "MultiFactorCombiner":
        from ..phase2.combiner import MultiFactorCombiner as CombinerType

        return CombinerType(
            self.settings.symbol,
            phase1_results,
            data_loader=self.data_loader(),
        )

    def logger(self):
        return self._logger


__all__ = ["ServiceContainer"]
