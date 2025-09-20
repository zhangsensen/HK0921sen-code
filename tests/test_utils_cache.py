import time

from utils.cache import InMemoryCache


def test_in_memory_cache_store_and_get():
    cache = InMemoryCache()
    cache.set("key", "value", ttl=10)
    assert cache.get("key") == "value"


def test_in_memory_cache_respects_ttl():
    cache = InMemoryCache()
    cache.set("key", "value", ttl=1)
    time.sleep(1.1)
    assert cache.get("key") is None


def test_in_memory_cache_clear():
    cache = InMemoryCache()
    cache.set("key", "value")
    cache.clear()
    assert cache.get("key") is None
