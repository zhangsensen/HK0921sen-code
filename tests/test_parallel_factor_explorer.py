import logging
import os

import pytest

from phase1.parallel_explorer import ParallelFactorExplorer
from utils.factor_cache import FactorCache

MAIN_PID = os.getpid()


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
