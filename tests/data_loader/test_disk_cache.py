import os
import time

import pytest

pd = pytest.importorskip("pandas")
pd_testing = pytest.importorskip("pandas.testing")

from data_loader_optimized import OptimizedDataLoader


def test_disk_cache_expires_entries(tmp_path, sample_frame):
    warm_loader = OptimizedDataLoader(
        data_provider=lambda *_: sample_frame,
        cache_dir=tmp_path,
        cache_ttl=1,
        preload=False,
    )
    warm_loader.load("0700.HK", "1m")
    cache_path = warm_loader._cache_path("0700.HK", "1m")
    expired_time = time.time() - 10
    os.utime(cache_path, (expired_time, expired_time))
    warm_loader.close()

    calls = {"count": 0}

    def provider(symbol, timeframe):
        calls["count"] += 1
        return sample_frame

    loader = OptimizedDataLoader(data_provider=provider, cache_dir=tmp_path, cache_ttl=1, preload=False)
    loaded = loader.load("0700.HK", "1m")
    loader.close()

    assert calls["count"] == 1
    assert cache_path.stat().st_mtime > expired_time
    pd_testing.assert_frame_equal(loaded, sample_frame, check_freq=False)


def test_load_from_disk_removes_corrupted_file(tmp_path, sample_frame, monkeypatch):
    loader = OptimizedDataLoader(
        data_provider=lambda *_: sample_frame,
        cache_dir=tmp_path,
        cache_ttl=60,
        preload=False,
    )
    cache_path = loader._cache_path("0700.HK", "1m")
    cache_path.write_text("corrupted")
    loader.close()

    calls = {"count": 0}

    def provider(symbol, timeframe):
        calls["count"] += 1
        return sample_frame

    def broken_read_pickle(path, *_, **__):
        raise ValueError("bad pickle")

    monkeypatch.setattr(pd, "read_pickle", broken_read_pickle)

    loader_with_corruption = OptimizedDataLoader(
        data_provider=provider,
        cache_dir=tmp_path,
        cache_ttl=60,
        preload=False,
    )
    loaded = loader_with_corruption.load("0700.HK", "1m")
    loader_with_corruption.close()

    assert calls["count"] == 1
    assert cache_path.stat().st_size > len("corrupted")
    pd_testing.assert_frame_equal(loaded, sample_frame, check_freq=False)


def test_preload_timeframes_handles_failing_future(tmp_path, sample_frame):
    frames = {"1m": sample_frame, "5m": sample_frame.resample("5min").agg("mean")}

    def provider(symbol, timeframe):
        if timeframe == "5m":
            raise RuntimeError("boom")
        return frames[timeframe]

    loader = OptimizedDataLoader(
        data_provider=provider,
        cache_dir=tmp_path,
        cache_ttl=1,
        preload=True,
        max_workers=2,
    )

    loaded = loader.preload_timeframes("0700.HK", ["1m", "5m"])
    assert ("0700.HK", "1m") in loaded
    assert ("0700.HK", "5m") not in loaded

    cached = loader.load("0700.HK", "1m")
    pd_testing.assert_frame_equal(cached, sample_frame, check_freq=False)

    with pytest.raises(RuntimeError):
        loader.load("0700.HK", "5m")

    loader.close()
