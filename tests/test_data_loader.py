import pytest

pd = pytest.importorskip("pandas")
pd_testing = pytest.importorskip("pandas.testing")

from hk_factor_discovery.data_loader import HistoricalDataLoader, OptimizedDataLoader


def test_loader_uses_cache():
    dataframe = _sample_frame()
    calls = {"count": 0}

    def provider(symbol: str, timeframe: str):
        calls["count"] += 1
        return dataframe

    loader = HistoricalDataLoader(data_provider=provider, cache_ttl=1)
    first = loader.load("0700.HK", "1m")
    second = loader.load("0700.HK", "1m")
    assert calls["count"] == 1
    pd_testing.assert_frame_equal(first, second)


def test_stream_batches():
    dataframe = _sample_frame()

    loader = HistoricalDataLoader(data_provider=lambda *_: dataframe)
    batches = list(loader.stream("0700.HK", "1m", batch_size=2))
    assert len(batches) == 2
    assert all(isinstance(batch, pd.DataFrame) for batch in batches)


def _sample_frame():
    index = pd.date_range("2024-01-01 09:30", periods=3, freq="1min")
    return pd.DataFrame(
        {
            "open": [100.0, 100.5, 101.0],
            "high": [101.0, 101.5, 102.0],
            "low": [99.5, 100.0, 100.5],
            "close": [100.2, 100.7, 101.1],
            "volume": [1_000, 1_100, 1_050],
        },
        index=index,
    )


def test_load_raw_supports_timeframe_first_layout(tmp_path, monkeypatch):
    dataframe = _sample_frame()
    timeframe_dir = tmp_path / "1m"
    timeframe_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = timeframe_dir / "0700.HK.parquet"
    parquet_path.touch()

    def fake_read_parquet(path, *_, **__):
        assert path == parquet_path
        return dataframe

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)

    loader = HistoricalDataLoader(data_root=tmp_path)
    loaded = loader.load("0700.HK", "1m")

    pd_testing.assert_frame_equal(loaded, dataframe)


def test_optimized_loader_uses_disk_cache(tmp_path):
    dataframe = _sample_frame()
    calls = {"count": 0}

    def provider(symbol: str, timeframe: str):
        calls["count"] += 1
        return dataframe

    loader = OptimizedDataLoader(data_provider=provider, cache_dir=tmp_path, cache_ttl=1)
    first = loader.load("0700.HK", "1m")
    assert calls["count"] == 1
    pd_testing.assert_frame_equal(first, dataframe)

    def failing_provider(*_args, **_kwargs):
        raise AssertionError("provider should not be called when disk cache hits")

    loader_from_cache = OptimizedDataLoader(
        data_provider=failing_provider,
        cache_dir=tmp_path,
        cache_ttl=1,
    )
    second = loader_from_cache.load("0700.HK", "1m")
    pd_testing.assert_frame_equal(second, dataframe)


def test_optimized_loader_preload_and_batch(tmp_path):
    frames = {"1m": _sample_frame(), "5m": _sample_frame()}
    calls = {"1m": 0, "5m": 0}

    def provider(symbol: str, timeframe: str):
        calls[timeframe] += 1
        return frames[timeframe]

    loader = OptimizedDataLoader(data_provider=provider, cache_dir=tmp_path, max_workers=2)
    preload_results = loader.preload_timeframes("0700.HK", ["1m", "5m"])
    assert set(preload_results) == {("0700.HK", "1m"), ("0700.HK", "5m")}
    assert calls == {"1m": 1, "5m": 1}

    batch = loader.batch_load([("0700.HK", "1m"), ("0700.HK", "5m")])
    assert set(batch) == {("0700.HK", "1m"), ("0700.HK", "5m")}
    assert calls == {"1m": 1, "5m": 1}

    loader.load("0700.HK", "1m")
    assert calls == {"1m": 1, "5m": 1}


def test_load_raw_supports_symbol_first_layout(tmp_path, monkeypatch):
    dataframe = _sample_frame()
    symbol_dir = tmp_path / "0700.HK"
    symbol_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = symbol_dir / "1m.parquet"
    parquet_path.touch()

    def fake_read_parquet(path, *_, **__):
        assert path == parquet_path
        return dataframe

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)

    loader = HistoricalDataLoader(data_root=tmp_path)
    loaded = loader.load("0700.HK", "1m")

    pd_testing.assert_frame_equal(loaded, dataframe)
