"""Caching helpers used by the parallel factor explorer."""

from __future__ import annotations

import hashlib
import pickle
import threading
from dataclasses import dataclass
from typing import Any, Dict, Hashable, Optional, Tuple

try:  # pragma: no cover - optional dependency guard
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None


@dataclass
class CacheStats:
    """Statistics describing cache utilisation."""

    hits: int = 0
    misses: int = 0
    stores: int = 0

    def snapshot(self) -> Dict[str, int]:
        """Return statistics as a serialisable dict."""

        return {"hits": self.hits, "misses": self.misses, "stores": self.stores}


class FactorCache:
    """Thread-safe cache for factor results with basic statistics."""

    def __init__(self) -> None:
        self._data: Dict[Hashable, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self.stats = CacheStats()

    # ------------------------------------------------------------------
    def _make_key(
        self, symbol: str, timeframe: str, factor_name: str, data_signature: Optional[str]
    ) -> Optional[Tuple[str, str, str, str]]:
        if data_signature is None:
            return None
        return (symbol, timeframe, factor_name, data_signature)

    def get(
        self, symbol: str, timeframe: str, factor_name: str, data_signature: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Return cached factor data if available."""

        key = self._make_key(symbol, timeframe, factor_name, data_signature)
        if key is None:
            self.stats.misses += 1
            return None
        with self._lock:
            cached = self._data.get(key)
        if cached is not None:
            self.stats.hits += 1
        else:
            self.stats.misses += 1
        return cached

    def set(
        self,
        symbol: str,
        timeframe: str,
        factor_name: str,
        data_signature: Optional[str],
        result: Dict[str, Any],
    ) -> None:
        """Persist the result for subsequent retrieval."""

        key = self._make_key(symbol, timeframe, factor_name, data_signature)
        if key is None:
            return
        with self._lock:
            self._data[key] = result
        self.stats.stores += 1

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
        self.stats = CacheStats()

    # ------------------------------------------------------------------
    @staticmethod
    def compute_signature(data: Any) -> Optional[str]:
        """Compute a stable signature for arbitrary data structures."""

        if data is None:
            return None
        try:
            if pd is not None and isinstance(data, pd.DataFrame):
                sample = data.tail(min(len(data), 200))
                payload = sample.to_json(date_format="iso", orient="split").encode()
            else:
                payload = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
            return hashlib.sha1(payload).hexdigest()
        except Exception:  # pragma: no cover - extremely defensive
            return hashlib.sha1(repr(data).encode()).hexdigest()


_factor_cache: FactorCache | None = None


def get_factor_cache() -> FactorCache:
    """Return a process wide factor cache instance."""

    global _factor_cache
    if _factor_cache is None:
        _factor_cache = FactorCache()
    return _factor_cache


__all__ = ["CacheStats", "FactorCache", "get_factor_cache"]
