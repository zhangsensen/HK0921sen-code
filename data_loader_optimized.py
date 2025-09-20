"""Optimised data loader used by the parallel explorer."""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence, Tuple

try:  # pragma: no cover - optional dependency guard
    import pandas as pd  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - surfaced via base class errors
    pd = None

from data_loader import HistoricalDataLoader


class OptimizedDataLoader(HistoricalDataLoader):
    """Historical data loader with simple preloading helpers."""

    def __init__(
        self,
        *args,
        max_workers: Optional[int] = None,
        preload: bool = True,
        cache_dir: Path | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        if pd is None:
            raise ModuleNotFoundError(
                "pandas is required to use OptimizedDataLoader. Install pandas to enable disk caching."
            )
        self._executor = ThreadPoolExecutor(max_workers=max_workers or 4)
        self._preload_enabled = preload
        self._preloaded: Dict[Tuple[str, str], object] = {}
        self._lock = threading.Lock()
        self._stats = {"preload_hits": 0, "preload_misses": 0, "disk_hits": 0, "disk_writes": 0}
        self.cache_dir = Path(cache_dir).expanduser() if cache_dir else None
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    def _cache_path(self, symbol: str, timeframe: str) -> Path:
        sanitized_symbol = symbol.replace("/", "_").replace(":", "_")
        return self.cache_dir / f"{sanitized_symbol}__{timeframe}.pkl"

    def _load_from_disk(self, symbol: str, timeframe: str):
        if not self.cache_dir:
            return None
        path = self._cache_path(symbol, timeframe)
        if not path.exists():
            return None
        if self.cache_ttl and self.cache_ttl > 0:
            age = time.time() - path.stat().st_mtime
            if age > self.cache_ttl:
                try:
                    path.unlink()
                except OSError:
                    pass
                return None
        try:
            data = pd.read_pickle(path)  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - corrupted cache
            try:
                path.unlink()
            except OSError:
                pass
            return None
        self._stats["disk_hits"] += 1
        return data

    def _store_to_disk(self, symbol: str, timeframe: str, data) -> None:
        if not self.cache_dir:
            return
        path = self._cache_path(symbol, timeframe)
        try:
            data.to_pickle(path)  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - ignore disk issues
            return
        self._stats["disk_writes"] += 1

    # ------------------------------------------------------------------
    def load(self, symbol: str, timeframe: str) -> object:
        key = (symbol, timeframe)
        if self.cache_dir:
            cached = self._load_from_disk(symbol, timeframe)
            if cached is not None:
                return cached
        if self._preload_enabled:
            with self._lock:
                data = self._preloaded.pop(key, None)
            if data is not None:
                self._stats["preload_hits"] += 1
                return data
            self._stats["preload_misses"] += 1
        result = super().load(symbol, timeframe)
        if self.cache_dir:
            self._store_to_disk(symbol, timeframe, result)
        return result

    # ------------------------------------------------------------------
    def preload_timeframes(self, symbol: str, timeframes: Sequence[str]) -> Dict[Tuple[str, str], object]:
        if not self._preload_enabled or not timeframes:
            return {}

        futures = {
            self._executor.submit(self.load, symbol, timeframe): (symbol, timeframe)
            for timeframe in timeframes
        }
        loaded: Dict[Tuple[str, str], object] = {}
        for future in as_completed(futures):
            key = futures[future]
            try:
                data = future.result()
            except Exception:  # pragma: no cover - surface later
                continue
            with self._lock:
                self._preloaded[key] = data
            loaded[key] = data
        return loaded

    def batch_load(self, pairs: Iterable[Tuple[str, str]]) -> Dict[Tuple[str, str], object]:
        futures = {
            self._executor.submit(self.load, symbol, timeframe): (symbol, timeframe)
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
