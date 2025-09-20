"""Parallel factor exploration using multiple processes."""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from config import DEFAULT_TIMEFRAMES
from data_loader_optimized import OptimizedDataLoader
try:  # pragma: no cover - optional heavy dependencies
    from factors import FactorCalculator, all_factors
except ModuleNotFoundError:  # pragma: no cover - allows lightweight tests
    FactorCalculator = Any  # type: ignore[assignment]

    def all_factors() -> list:
        raise ModuleNotFoundError("pandas/numpy are required for the default factor set")
from utils.factor_cache import FactorCache, get_factor_cache
try:  # pragma: no cover - optional dependency
    from .enhanced_backtest_engine import EnhancedBacktestEngine, create_enhanced_backtest_engine
except ModuleNotFoundError:  # pragma: no cover - allows using stubs in tests
    EnhancedBacktestEngine = Any  # type: ignore[assignment]

    def create_enhanced_backtest_engine(symbol: str, **_: Any):  # type: ignore[override]
        raise ModuleNotFoundError("EnhancedBacktestEngine requires numpy/pandas")

_logger = logging.getLogger(__name__)


def _default_engine_factory(symbol: str) -> EnhancedBacktestEngine:
    return create_enhanced_backtest_engine(symbol)


def _current_memory_mb() -> float:
    try:  # pragma: no cover - psutil is optional
        import psutil  # type: ignore

        return psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    except Exception:  # pragma: no cover - fall back to resource
        try:
            import resource  # type: ignore

            usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform == "darwin":
                return usage / (1024 * 1024)
            return usage / 1024
        except Exception:
            return 0.0


def _format_result(
    symbol: str,
    timeframe: str,
    factor: FactorCalculator,
    backtest: Dict[str, Any],
) -> Dict[str, Any]:
    result = dict(backtest)
    result.setdefault("symbol", symbol)
    result.setdefault("timeframe", timeframe)
    result.setdefault("factor", factor.name)
    result.setdefault(
        "exploration_date",
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    )
    return result


def _worker_task(
    symbol: str,
    timeframe: str,
    factor: FactorCalculator,
    data: Any,
    engine_factory: Callable[[str], EnhancedBacktestEngine],
) -> tuple[str, Dict[str, Any]]:
    engine = engine_factory(symbol)
    signals = factor.generate_signals(symbol, timeframe, data)
    backtest = engine.backtest_factor(data, signals)
    key = f"{timeframe}_{factor.name}"
    return key, _format_result(symbol, timeframe, factor, backtest)


@dataclass
class _Task:
    timeframe: str
    factor: FactorCalculator
    data: Any
    signature: Optional[str]


class ParallelFactorExplorer:
    """Execute factor exploration in a :class:`ProcessPoolExecutor`."""

    def __init__(
        self,
        symbol: str,
        timeframes: Optional[Sequence[str]] = None,
        factors: Optional[Iterable[FactorCalculator]] = None,
        data_loader: Optional[OptimizedDataLoader] = None,
        factor_cache: Optional[FactorCache] = None,
        backtest_engine_factory: Callable[[str], EnhancedBacktestEngine] = _default_engine_factory,
        max_workers: Optional[int] = None,
        memory_limit_mb: Optional[int] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.symbol = symbol
        self.timeframes = list(timeframes) if timeframes is not None else list(DEFAULT_TIMEFRAMES)
        self.factors: List[FactorCalculator] = list(factors) if factors is not None else all_factors()
        self.data_loader = data_loader
        if self.data_loader is None:
            raise ValueError("data_loader must be provided for ParallelFactorExplorer")
        self.factor_cache = factor_cache or get_factor_cache()
        self._engine_factory = backtest_engine_factory
        self.max_workers = max_workers or max(1, (os.cpu_count() or 2) - 1)
        self.memory_limit_mb = memory_limit_mb
        self.logger = logger or _logger
        self._progress_logged = 0
        self._process_pool_supported = True

    # ------------------------------------------------------------------
    def _log_progress(self, completed: int, total: int) -> None:
        if total <= 0:
            return
        if completed == self._progress_logged:
            return
        self._progress_logged = completed
        percentage = (completed / total) * 100
        self.logger.info("并行探索进度 %s/%s (%.1f%%)", completed, total, percentage)

    def _check_memory(self) -> None:
        if not self.memory_limit_mb:
            return
        usage = _current_memory_mb()
        if usage > self.memory_limit_mb:
            self.logger.warning(
                "内存使用 %.1fMB 超过限制 %.1fMB", usage, float(self.memory_limit_mb)
            )

    def _compute_locally(self, timeframe: str, factor: FactorCalculator, data: Any) -> Dict[str, Any]:
        self.logger.debug("回退到本地执行 %s_%s", timeframe, factor.name)
        engine = self._engine_factory(self.symbol)
        signals = factor.generate_signals(self.symbol, timeframe, data)
        backtest = engine.backtest_factor(data, signals)
        return _format_result(self.symbol, timeframe, factor, backtest)

    def explore_single_factor(
        self, timeframe: str, factor: FactorCalculator, data: Any | None = None
    ) -> Dict[str, Any]:
        if data is None:
            data = self.data_loader.load(self.symbol, timeframe)
        return self._compute_locally(timeframe, factor, data)

    def _build_tasks(self) -> tuple[List[_Task], Dict[str, Any], int, Dict[str, Dict[str, Any]]]:
        tasks: List[_Task] = []
        dataset: Dict[str, Any] = {}
        results: Dict[str, Dict[str, Any]] = {}
        total = len(self.timeframes) * len(self.factors)
        completed = 0

        for timeframe in self.timeframes:
            data = self.data_loader.load(self.symbol, timeframe)
            dataset[timeframe] = data
            signature = FactorCache.compute_signature(data)
            for factor in self.factors:
                key = f"{timeframe}_{factor.name}"
                cached = self.factor_cache.get(self.symbol, timeframe, factor.name, signature)
                if cached is not None:
                    results[key] = cached
                    completed += 1
                    self._log_progress(completed, total)
                    continue
                tasks.append(_Task(timeframe, factor, data, signature))
        return tasks, dataset, total, results

    # ------------------------------------------------------------------
    def explore_all_factors(self) -> Dict[str, Dict[str, Any]]:
        self._progress_logged = 0
        tasks, dataset, total, results = self._build_tasks()
        completed = len(results)
        if not tasks:
            return results

        if not self._process_pool_supported:
            return self._execute_sequential(tasks, dataset, results, completed, total)

        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {
                    executor.submit(
                        _worker_task, self.symbol, task.timeframe, task.factor, task.data, self._engine_factory
                    ): task
                    for task in tasks
                }
                for future in as_completed(future_map):
                    task = future_map[future]
                    key = f"{task.timeframe}_{task.factor.name}"
                    try:
                        _, result = future.result()
                    except Exception as exc:
                        self.logger.error("并行任务失败 %s: %s", key, exc)
                        result = self._compute_locally(task.timeframe, task.factor, dataset[task.timeframe])
                    results[key] = result
                    if task.signature is not None:
                        self.factor_cache.set(
                            self.symbol, task.timeframe, task.factor.name, task.signature, result
                        )
                    completed += 1
                    self._log_progress(completed, total)
                    self._check_memory()
        except (NotImplementedError, PermissionError, OSError) as exc:
            self._process_pool_supported = False
            self.logger.warning("进程池不可用，回退到单进程执行: %s", exc)
            return self._execute_sequential(tasks, dataset, results, completed, total)
        return results

    async def explore_all_factors_async(self, batch_size: int | None = None) -> Dict[str, Dict[str, Any]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.explore_all_factors)

    # Convenience ------------------------------------------------------
    @property
    def cache_stats(self) -> Dict[str, int]:
        return self.factor_cache.stats.snapshot()

    @property
    def process_pool_available(self) -> bool:
        return self._process_pool_supported

    def _execute_sequential(
        self,
        tasks: List[_Task],
        dataset: Dict[str, Any],
        results: Dict[str, Dict[str, Any]],
        completed: int,
        total: int,
    ) -> Dict[str, Dict[str, Any]]:
        for task in tasks:
            key = f"{task.timeframe}_{task.factor.name}"
            try:
                result = self._compute_locally(task.timeframe, task.factor, dataset[task.timeframe])
            except Exception as exc:  # pragma: no cover - defensive guard
                self.logger.error("本地任务失败 %s: %s", key, exc)
                continue
            results[key] = result
            if task.signature is not None:
                self.factor_cache.set(self.symbol, task.timeframe, task.factor.name, task.signature, result)
            completed += 1
            self._log_progress(completed, total)
        return results


def create_parallel_explorer(symbol: str, **kwargs: Any) -> ParallelFactorExplorer:
    """Factory mirroring the single explorer helper."""

    return ParallelFactorExplorer(symbol, **kwargs)


__all__ = ["ParallelFactorExplorer", "create_parallel_explorer"]
