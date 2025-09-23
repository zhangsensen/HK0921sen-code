import logging
import os
from collections import defaultdict

import pytest

from data_loader_optimized import OptimizedDataLoader
from phase1.parallel_explorer import ParallelFactorExplorer
from utils.factor_cache import FactorCache

MAIN_PID = os.getpid()


def _timeframe_to_freq(timeframe: str) -> str:
    value = ''.join(ch for ch in timeframe if ch.isdigit()) or timeframe[:-1]
    unit = timeframe[-1]
    if unit == 'm':
        return f"{value}min"
    if unit == 'h':
        return f"{value}h"
    if unit == 'd':
        return f"{value}D"
    raise ValueError(f"Unsupported timeframe {timeframe}")


class DummyLoader:
    def __init__(self, data):
        self._data = data

    def load(self, symbol: str, timeframe: str):
        return list(self._data[timeframe])


class DummyFactor:
    def __init__(self, name: str, fail_in_process: bool = False):
        self.name = name
        self.category = "test"
        self._fail = fail_in_process
        self._main_pid = MAIN_PID

    def generate_signals(self, symbol: str, timeframe: str, data):
        if self._fail and os.getpid() != self._main_pid:
            raise RuntimeError("fail in worker process")
        return [float(x) for x in data]


class DummyEngine:
    def __init__(self, symbol: str):
        self.symbol = symbol

    def backtest_factor(self, data, signals):
        return {
            "returns": list(signals),
            "equity_curve": list(signals),
            "sharpe_ratio": 1.0,
            "stability": 1.0,
            "trades_count": len(signals),
            "win_rate": 0.5,
            "profit_factor": 1.0,
            "max_drawdown": 0.1,
            "information_coefficient": 0.0,
            "process_id": os.getpid(),
        }


def dummy_engine_factory(symbol: str):
    return DummyEngine(symbol)


class RollingWindowFactor:
    """Factor stub that transforms price data into float signals."""

    def __init__(self, name: str, offset: float = 0.0) -> None:
        self.name = name
        self.category = "integration"
        self._offset = float(offset)

    def generate_signals(self, symbol: str, timeframe: str, data):
        closes = data["close"].tolist()
        return [float(value) + self._offset for value in closes]


def _cumulative(values):
    total = 0.0
    curve = []
    for value in values:
        total += float(value)
        curve.append(total)
    return curve


class IntegrationEngine:
    """Lightweight engine producing deterministic performance metrics."""

    def __init__(self, symbol: str):
        self.symbol = symbol

    def backtest_factor(self, data, signals):
        trades = len(signals)
        mean_value = sum(signals) / trades if trades else 0.0
        variance = (
            sum((value - mean_value) ** 2 for value in signals) / trades if trades else 0.0
        )
        sharpe = mean_value / (variance ** 0.5 + 1e-9) if variance else 0.0
        return {
            "returns": list(signals),
            "equity_curve": _cumulative(signals),
            "sharpe_ratio": sharpe,
            "stability": 0.5,
            "trades_count": trades,
            "win_rate": 0.55,
            "profit_factor": 1.05,
            "max_drawdown": max(signals) - min(signals) if trades else 0.0,
            "information_coefficient": 0.1,
            "process_id": os.getpid(),
        }


def integration_engine_factory(symbol: str) -> IntegrationEngine:
    return IntegrationEngine(symbol)


@pytest.fixture
def loader():
    return DummyLoader({"1m": [1, 2, 3]})


def _build_explorer(loader, factors, cache=None):
    cache = cache or FactorCache()
    logger = logging.getLogger("test.parallel")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    return ParallelFactorExplorer(
        "0700.HK",
        timeframes=["1m"],
        factors=factors,
        data_loader=loader,
        factor_cache=cache,
        backtest_engine_factory=dummy_engine_factory,
        max_workers=2,
        logger=logger,
    )


def test_parallel_explorer_runs_in_separate_processes(loader, caplog):
    explorer = _build_explorer(loader, [DummyFactor("A"), DummyFactor("B")])
    with caplog.at_level(logging.INFO):
        results = explorer.explore_all_factors()
    assert set(results) == {"1m_A", "1m_B"}
    process_ids = {value["process_id"] for value in results.values()}
    if explorer.process_pool_available:
        assert any(pid != MAIN_PID for pid in process_ids)
    else:
        assert all(pid == MAIN_PID for pid in process_ids)
        assert any("进程池不可用" in record.message for record in caplog.records)
    assert any("并行探索进度" in record.message for record in caplog.records)
    stats = explorer.cache_stats
    assert stats["stores"] == 2


def test_parallel_explorer_fallback_on_error(loader, caplog):
    explorer = _build_explorer(loader, [DummyFactor("fail", True), DummyFactor("ok")])
    with caplog.at_level(logging.ERROR):
        results = explorer.explore_all_factors()
    fallback = results["1m_fail"]
    assert fallback["process_id"] == MAIN_PID
    assert any("并行任务失败" in record.message for record in caplog.records) or not explorer.process_pool_available


def test_parallel_explorer_uses_cache(loader):
    cache = FactorCache()
    explorer = _build_explorer(loader, [DummyFactor("cached")], cache=cache)
    first = explorer.explore_all_factors()
    assert cache.stats.stores == 1
    second = explorer.explore_all_factors()
    assert first == second
    assert explorer.cache_stats["hits"] >= 1


def test_parallel_explorer_preloads_and_hits_disk(tmp_path, caplog, monkeypatch):
    pd = pytest.importorskip("pandas")
    pd_testing = pytest.importorskip("pandas.testing")

    symbols = ["0700.HK", "0005.HK", "0388.HK"]
    timeframes = ["1m", "5m", "15m"]
    dataset = {}
    for symbol in symbols:
        symbol_data = {}
        for timeframe in timeframes:
            freq = _timeframe_to_freq(timeframe)
            periods = 240 if timeframe.endswith("m") else 64
            base = 90 + (abs(hash((symbol, timeframe))) % 25)
            index = pd.date_range("2024-01-01 09:30", periods=periods, freq=freq)
            close = [base + step * 0.2 for step in range(periods)]
            frame = pd.DataFrame(
                {
                    "open": [value - 0.1 for value in close],
                    "high": [value + 0.2 for value in close],
                    "low": [value - 0.3 for value in close],
                    "close": close,
                    "volume": [10_000 + step for step in range(periods)],
                },
                index=index,
            )
            symbol_data[timeframe] = frame
        dataset[symbol] = symbol_data

    provider_calls = defaultdict(int)

    def provider(symbol: str, timeframe: str):
        provider_calls[(symbol, timeframe)] += 1
        return dataset[symbol][timeframe].copy()

    cache_dir = tmp_path / "optimized_cache"
    loader = OptimizedDataLoader(data_provider=provider, cache_dir=cache_dir, max_workers=4)
    cache = FactorCache()
    factors = [
        RollingWindowFactor("offset0", 0.0),
        RollingWindowFactor("offset1", 1.0),
        RollingWindowFactor("offset2", 2.0),
    ]
    logger = logging.getLogger("test.parallel.integration")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger=logger.name)
    monkeypatch.setattr("phase1.parallel_explorer._current_memory_mb", lambda: 512.0)

    explorer = ParallelFactorExplorer(
        symbols[0],
        timeframes=timeframes,
        factors=factors,
        data_loader=loader,
        factor_cache=cache,
        backtest_engine_factory=integration_engine_factory,
        max_workers=2,
        memory_limit_mb=64,
        logger=logger,
    )

    results = explorer.explore_all_factors()
    expected_keys = {f"{timeframe}_{factor.name}" for timeframe in timeframes for factor in factors}
    assert set(results) == expected_keys

    stats = loader.stats()
    assert stats["preload_hits"] == len(timeframes)
    assert stats["preload_misses"] == 0
    assert stats["disk_writes"] >= len(timeframes)
    for timeframe in timeframes:
        assert provider_calls[(symbols[0], timeframe)] == 1

    warning_records = [record for record in caplog.records if "内存使用" in record.message]
    if explorer.process_pool_available:
        assert warning_records, "memory pressure warning should be logged"
    else:
        assert any("进程池不可用" in record.message for record in caplog.records)

    process_ids = {value["process_id"] for value in results.values()}
    if explorer.process_pool_available:
        assert any(pid != MAIN_PID for pid in process_ids)

    def disk_only_provider(*_args, **_kwargs):
        raise AssertionError("disk cache should satisfy the second loader")

    disk_loader = OptimizedDataLoader(data_provider=disk_only_provider, cache_dir=cache_dir, preload=False)
    for timeframe in timeframes:
        cached = disk_loader.load(symbols[0], timeframe)
        pd_testing.assert_frame_equal(cached, dataset[symbols[0]][timeframe], check_freq=False)

    disk_stats = disk_loader.stats()
    assert disk_stats["disk_hits"] == len(timeframes)
