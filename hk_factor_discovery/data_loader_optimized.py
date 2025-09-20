"""Optimised data loader used by the parallel explorer."""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, Optional, Sequence, Tuple

from .data_loader import HistoricalDataLoader


class OptimizedDataLoader(HistoricalDataLoader):
    """Historical data loader with simple preloading helpers."""

    def __init__(
        self,
        *args,
        max_workers: Optional[int] = None,
        preload: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._executor = ThreadPoolExecutor(max_workers=max_workers or 4)
        self._preload_enabled = preload
        self._preloaded: Dict[Tuple[str, str], object] = {}
        self._lock = threading.Lock()
        self._stats = {"preload_hits": 0, "preload_misses": 0}

    # ------------------------------------------------------------------
    def load(self, symbol: str, timeframe: str) -> object:
        key = (symbol, timeframe)
        if self._preload_enabled:
            with self._lock:
                data = self._preloaded.pop(key, None)
            if data is not None:
                self._stats["preload_hits"] += 1
                return data
            self._stats["preload_misses"] += 1
        return super().load(symbol, timeframe)

    # ------------------------------------------------------------------
    def preload_timeframes(self, symbol: str, timeframes: Sequence[str]) -> None:
        if not self._preload_enabled or not timeframes:
            return

        futures = {
            self._executor.submit(super().load, symbol, timeframe): (symbol, timeframe)
            for timeframe in timeframes
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                data = future.result()
            except Exception:  # pragma: no cover - surface later
                continue
            with self._lock:
                self._preloaded[key] = data

    def batch_load(self, pairs: Iterable[Tuple[str, str]]) -> Dict[Tuple[str, str], object]:
        futures = {
            self._executor.submit(super().load, symbol, timeframe): (symbol, timeframe)
            for symbol, timeframe in pairs
        }
        results: Dict[Tuple[str, str], object] = {}
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception:  # pragma: no cover - surface to caller
                continue
        return results

    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._stats)

    def close(self) -> None:
        self._executor.shutdown(wait=True)


def create_optimized_loader(*args, **kwargs) -> OptimizedDataLoader:
    """Factory used by dependency injection in tests and CLI."""

    return OptimizedDataLoader(*args, **kwargs)


__all__ = ["OptimizedDataLoader", "create_optimized_loader"]
