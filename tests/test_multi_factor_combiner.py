import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("pandas")

from config import CombinerConfig
from phase2.combiner import MultiFactorCombiner


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
            "returns": np.array([0.01, 0.005, -0.002, 0.004]),
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
            "returns": np.array([0.008, 0.004, -0.001, 0.003]),
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
            "returns": np.array([0.006, 0.002, -0.003, 0.002]),
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
            "returns": np.array([0.01, 0.0, -0.002, 0.003]),
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
