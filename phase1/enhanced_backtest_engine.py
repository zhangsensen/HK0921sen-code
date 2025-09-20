"""Enhanced backtest engine with lightweight caching."""
from __future__ import annotations

import hashlib
import pickle
import threading
from typing import Any, Dict, Iterable, Optional

from .backtest_engine import SimpleBacktestEngine


class EnhancedBacktestEngine(SimpleBacktestEngine):
    """Simple extension of :class:`SimpleBacktestEngine` adding caching."""

    def __init__(
        self,
        symbol: str,
        initial_capital: float = 100_000,
        allocation: float = 0.1,
        enable_cache: bool = True,
    ) -> None:
        super().__init__(symbol, initial_capital=initial_capital, allocation=allocation)
        self.enable_cache = enable_cache
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = threading.Lock()
        self.cache_hits = 0
        self.cache_misses = 0

    # ------------------------------------------------------------------
    def _make_cache_key(self, data: Any, signals: Any) -> Optional[str]:
        if not self.enable_cache:
            return None
        try:
            payload = pickle.dumps((self.symbol, data, list(_iterable_from(signals))))
        except Exception:  # pragma: no cover - fallback to repr
            payload = repr((self.symbol, data, list(_iterable_from(signals)))).encode()
        return hashlib.sha1(payload).hexdigest()

    def backtest_factor(self, data: Any, signals: Any) -> Dict[str, Any]:
        key = self._make_cache_key(data, signals)
        if key is not None:
            with self._cache_lock:
                cached = self._cache.get(key)
            if cached is not None:
                self.cache_hits += 1
                return cached
        result = super().backtest_factor(data, signals)
        if key is not None:
            with self._cache_lock:
                self._cache[key] = result
            self.cache_misses += 1
        return result


def _iterable_from(signals: Any) -> Iterable[float]:
    if hasattr(signals, "to_numpy"):
        array = signals.to_numpy()
        for value in array.tolist():
            yield float(value)
    elif hasattr(signals, "tolist"):
        for value in signals.tolist():
            yield float(value)
    elif isinstance(signals, dict):
        for key in sorted(signals):
            yield float(signals[key])
    else:
        for value in signals:
            yield float(value)


def create_enhanced_backtest_engine(symbol: str, **kwargs: Any) -> EnhancedBacktestEngine:
    """Factory helper mirroring the design documents."""

    return EnhancedBacktestEngine(symbol, **kwargs)


__all__ = ["EnhancedBacktestEngine", "create_enhanced_backtest_engine"]
