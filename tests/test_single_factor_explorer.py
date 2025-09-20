import asyncio
import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from data_loader import HistoricalDataLoader
from factors import all_factors
from phase1.explorer import SingleFactorExplorer


def _make_price_frame(periods: int = 200):
    index = pd.date_range("2024-01-01", periods=periods, freq="1min")
    base = np.linspace(100, 110, periods)
    noise = np.sin(np.linspace(0, 6, periods))
    close = base + noise
    open_ = close * (1 - 0.001)
    high = close * (1 + 0.002)
    low = close * (1 - 0.002)
    volume = np.linspace(1_000_000, 1_200_000, periods)
    return pd.DataFrame({
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }, index=index)


def test_single_factor_explorer_runs_subset():
    data_map = {"1m": _make_price_frame()}

    def provider(symbol: str, timeframe: str):
        return data_map[timeframe]

    loader = HistoricalDataLoader(data_provider=provider)
    subset = all_factors()[:3]
    explorer = SingleFactorExplorer(
        "0700.HK",
        timeframes=["1m"],
        factors=subset,
        data_loader=loader,
    )
    results = explorer.explore_all_factors()
    assert len(results) == len(subset)
    sample = next(iter(results.values()))
    for key in {
        "symbol",
        "timeframe",
        "factor",
        "sharpe_ratio",
        "stability",
        "trades_count",
        "win_rate",
        "profit_factor",
        "max_drawdown",
        "information_coefficient",
    }:
        assert key in sample
        assert sample[key] is not None


def test_async_explorer_matches_sync():
    data_map = {"1m": _make_price_frame()}

    loader = HistoricalDataLoader(data_provider=lambda *_: data_map["1m"])
    subset = all_factors()[:2]
    explorer = SingleFactorExplorer(
        "0700.HK",
        timeframes=["1m"],
        factors=subset,
        data_loader=loader,
    )

    expected = explorer.explore_all_factors()
    actual = asyncio.run(explorer.explore_all_factors_async(batch_size=1))
    assert expected.keys() == actual.keys()


def test_explorer_uses_preload_and_batch():
    class RecordingLoader:
        def __init__(self) -> None:
            self.preload_args = None
            self.batch_requests = None

        def preload_timeframes(self, symbol, timeframes):
            self.preload_args = (symbol, tuple(timeframes))
            return {(symbol, tf): _make_price_frame() for tf in timeframes}

        def batch_load(self, requests):
            self.batch_requests = tuple(requests)
            return {req: _make_price_frame() for req in requests}

        def load(self, *_args, **_kwargs):  # pragma: no cover - should not be used
            raise AssertionError("load should not be called when batch interface is available")

    loader = RecordingLoader()
    subset = all_factors()[:1]
    explorer = SingleFactorExplorer(
        "0700.HK",
        timeframes=["1m", "5m"],
        factors=subset,
        data_loader=loader,  # type: ignore[arg-type]
    )
    results = explorer.explore_all_factors()
    assert results  # ensure at least one result computed
    assert loader.preload_args == ("0700.HK", ("1m", "5m"))
    assert loader.batch_requests == (("0700.HK", "1m"), ("0700.HK", "5m"))
