import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from config import CombinerConfig
from phase2.combiner import MultiFactorCombiner
from utils.performance_metrics import PerformanceMetrics


def test_combiner_creates_sorted_strategies():
    phase1_results = {
        "1m_factor_a": {
            "symbol": "0700.HK",
            "timeframe": "1m",
            "factor": "factor_a",
            "sharpe_ratio": 1.2,
            "stability": 0.8,
            "trades_count": 12,
            "win_rate": 0.6,
            "profit_factor": 1.5,
            "max_drawdown": 0.1,
            "returns": pd.Series(
                [0.01, 0.005, -0.002, 0.004],
                index=pd.date_range("2024-01-01", periods=4, freq="D"),
            ),
            "information_coefficient": 0.08,
        },
        "1m_factor_b": {
            "symbol": "0700.HK",
            "timeframe": "1m",
            "factor": "factor_b",
            "sharpe_ratio": 1.1,
            "stability": 0.7,
            "trades_count": 10,
            "win_rate": 0.55,
            "profit_factor": 1.4,
            "max_drawdown": 0.11,
            "returns": pd.Series(
                [0.008, 0.004, -0.001, 0.003],
                index=pd.date_range("2024-01-01", periods=4, freq="D"),
            ),
            "information_coefficient": 0.05,
        },
        "1m_factor_c": {
            "symbol": "0700.HK",
            "timeframe": "1m",
            "factor": "factor_c",
            "sharpe_ratio": 0.9,
            "stability": 0.6,
            "trades_count": 8,
            "win_rate": 0.52,
            "profit_factor": 1.2,
            "max_drawdown": 0.12,
            "returns": pd.Series(
                [0.006, 0.002, -0.003, 0.002],
                index=pd.date_range("2024-01-01", periods=4, freq="D"),
            ),
            "information_coefficient": 0.02,
        },
    }

    combiner = MultiFactorCombiner("0700.HK", phase1_results)
    strategies = combiner.discover_strategies()
    assert strategies
    sharpe_values = [s["sharpe_ratio"] for s in strategies]
    assert sharpe_values == sorted(sharpe_values, reverse=True)
    assert all(len(s["factors"]) >= 2 for s in strategies)
    assert all("average_information_coefficient" in s for s in strategies)


def test_select_top_factors_prioritizes_sharpe_and_ic():
    phase1_results = {
        f"1m_factor_{i}": {
            "symbol": "0700.HK",
            "timeframe": "1m",
            "factor": f"factor_{i}",
            "sharpe_ratio": 1.0 - i * 0.01,
            "stability": 0.5,
            "trades_count": 5,
            "win_rate": 0.5,
            "profit_factor": 1.1,
            "max_drawdown": 0.1,
            "returns": pd.Series(
                [0.01, 0.0, -0.002, 0.003],
                index=pd.date_range("2024-01-01", periods=4, freq="D"),
            ),
            "information_coefficient": 0.001 if i == 0 else 0.1,
        }
        for i in range(4)
    }

    combiner = MultiFactorCombiner(
        "0700.HK",
        phase1_results,
        config=CombinerConfig(min_information_coefficient=0.05),
    )
    top = combiner.select_top_factors(top_n=2)
    assert len(top) == 2
    # factor_0 has the best sharpe but a near-zero IC so it should be filtered out
    assert all(entry["factor"] != "factor_0" for entry in top)


def test_combination_backtest_aligns_on_time_index():
    # Generate sufficient data points (25) to meet minimum requirements
    index_fast = pd.date_range("2024-01-01", periods=25, freq="h")
    index_slow = pd.date_range("2024-01-01 02:00", periods=24, freq="h")

    np.random.seed(42)
    fast_returns = pd.Series(np.random.normal(0.005, 0.01, 25), index=index_fast)
    slow_returns = pd.Series(np.random.normal(0.007, 0.012, 24), index=index_slow)

    combo = [
        {
            "factor": "fast",
            "timeframe": "1h",
            "returns": fast_returns,
            "information_coefficient": 0.1,
            "trades_count": 15,
            "sharpe_ratio": 1.2,
        },
        {
            "factor": "slow",
            "timeframe": "4h",
            "returns": slow_returns,
            "information_coefficient": 0.2,
            "trades_count": 12,
            "sharpe_ratio": 1.5,
        },
    ]

    combiner = MultiFactorCombiner("0700.HK", {})
    result = combiner.backtest_combination(combo)

    expected = pd.concat([fast_returns, slow_returns], axis=1, join="inner").mean(axis=1)
    expected_array = expected.to_numpy(dtype=float)

    assert result
    assert isinstance(result["returns"], pd.Series)
    assert list(result["returns"].index) == list(expected.index)
    assert np.allclose(result["returns"], expected)
    assert result["timeframes"] == ["1h", "4h"]
    assert np.isclose(
        result["average_information_coefficient"], np.mean([0.1, 0.2])
    )
    assert np.isclose(
        result["sharpe_ratio"], PerformanceMetrics.calculate_sharpe_ratio(expected_array)
    )
    assert np.isclose(
        result["stability"], PerformanceMetrics.calculate_stability(expected_array)
    )
    assert np.isclose(
        result["profit_factor"],
        PerformanceMetrics.calculate_profit_factor(
            expected[expected > 0].to_numpy(dtype=float),
            expected[expected < 0].to_numpy(dtype=float),
        ),
    )
    assert np.isclose(
        result["max_drawdown"],
        PerformanceMetrics.calculate_max_drawdown(np.cumprod(1 + expected_array)),
    )
    assert result["trades_count"] == int(
        np.count_nonzero(np.diff(np.sign(expected_array)))
    )
    assert np.isclose(result["win_rate"], float((expected > 0).mean()))


def test_combiner_uses_explicit_index_for_array_returns():
    # Generate sufficient data points (25) to meet minimum requirements
    index_primary = pd.date_range("2024-02-01", periods=25, freq="D")
    index_secondary = pd.date_range("2024-02-02", periods=25, freq="D")

    np.random.seed(42)
    returns_primary = np.random.normal(0.005, 0.01, 25).astype(float)
    returns_secondary = np.random.normal(0.007, 0.012, 25).astype(float)

    combo = [
        {
            "factor": "primary",
            "timeframe": "1d",
            "returns": returns_primary,
            "index": index_primary,
            "information_coefficient": 0.12,
            "trades_count": 18,
            "sharpe_ratio": 1.1,
        },
        {
            "factor": "secondary",
            "timeframe": "1d",
            "returns": returns_secondary,
            "timestamps": [ts.strftime("%Y-%m-%d") for ts in index_secondary],
            "information_coefficient": 0.09,
            "trades_count": 16,
            "sharpe_ratio": 1.3,
        },
    ]

    combiner = MultiFactorCombiner("0700.HK", {})
    result = combiner.backtest_combination(combo)

    expected = pd.concat(
        [
            pd.Series(returns_primary, index=index_primary),
            pd.Series(returns_secondary, index=index_secondary),
        ],
        axis=1,
        join="inner",
    ).mean(axis=1)

    assert result
    assert isinstance(result["returns"], pd.Series)
    assert list(result["returns"].index) == list(expected.index)
    assert np.allclose(result["returns"], expected)
