"""Simple caching helpers used across the application."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Hashable, Optional, Protocol


class CacheBackend(Protocol):
    """Protocol implemented by cache backends."""

    def get(self, key: Hashable) -> Any | None:
        ...

    def set(self, key: Hashable, value: Any, ttl: Optional[int] = None) -> None:
        ...

    def clear(self) -> None:
        ...


@dataclass
class _CacheEntry:
    value: Any
    expires_at: float | None


class InMemoryCache(CacheBackend):
    """Thread-safe TTL aware cache."""

    def __init__(self) -> None:
        self._data: Dict[Hashable, _CacheEntry] = {}
        self._lock = threading.Lock()

    def get(self, key: Hashable) -> Any | None:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            if entry.expires_at is not None and entry.expires_at < time.time():
                del self._data[key]
                return None
            return entry.value

    def set(self, key: Hashable, value: Any, ttl: Optional[int] = None) -> None:
        expires_at = time.time() + ttl if ttl else None
        with self._lock:
            self._data[key] = _CacheEntry(value=value, expires_at=expires_at)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


__all__ = ["CacheBackend", "InMemoryCache"]
