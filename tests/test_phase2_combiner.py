"""Focused tests for the phase2 multi-factor combiner pipeline."""

from __future__ import annotations

from typing import Dict, Mapping, Sequence, Tuple

import pytest

np = pytest.importorskip("numpy")

try:  # pandas is optional in the project – the tests adapt if it is missing.
    import pandas as pd  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - exercised when pandas is absent
    pd = None  # type: ignore[assignment]

from config import CombinerConfig
from phase2.combiner import MultiFactorCombiner
from utils.performance_metrics import PerformanceMetrics


def _make_factor(
    *,
    name: str,
    sharpe: float,
    ic: float,
    timeframe: str = "1d",
    returns: Mapping[str, object] | Sequence[float] | None = None,
) -> Dict[str, object]:
    """Utility helper to create phase-one factor payloads for tests."""

    payload: Dict[str, object] = {
        "factor": name,
        "timeframe": timeframe,
        "sharpe_ratio": sharpe,
        "information_coefficient": ic,
    }
    if returns is not None:
        payload["returns"] = returns
    return payload


def test_select_top_factors_respects_thresholds_and_sorting():
    phase1_results = {
        "alpha": _make_factor(name="alpha", sharpe=1.50, ic=0.06),
        "beta": _make_factor(name="beta", sharpe=1.50, ic=0.20),
        "gamma": _make_factor(name="gamma", sharpe=1.40, ic=-0.30),
        "delta": _make_factor(name="delta", sharpe=0.30, ic=0.80),  # filtered by min_sharpe
        "epsilon": _make_factor(name="epsilon", sharpe=1.30, ic=0.01),  # filtered by min_ic
    }

    config = CombinerConfig(min_sharpe=1.0, min_information_coefficient=0.05)
    combiner = MultiFactorCombiner("0700.HK", phase1_results, config=config)

    selected = combiner.select_top_factors(top_n=5)

    # Only factors meeting both thresholds are returned, sorted by sharpe then |IC|.
    assert [f["factor"] for f in selected] == ["beta", "alpha", "gamma"]


def test_generate_combinations_obeys_limits_and_counts():
    phase1_results: Dict[str, Dict[str, object]] = {}
    config = CombinerConfig(top_n=4, max_factors=3, max_combinations=10)
    combiner = MultiFactorCombiner("0700.HK", phase1_results, config=config)

    factors = [
        {"factor": f"f{i}", "timeframe": "1d", "sharpe_ratio": 1.0, "information_coefficient": 0.1}
        for i in range(4)
    ]

    combos = combiner.generate_combinations(factors)

    # With 4 available factors and a max depth of 3, we expect C(4, 2) + C(4, 3) combinations.
    assert len(combos) == 10
    combo_sets = {tuple(entry["factor"] for entry in combo) for combo in combos}
    expected_sets = {
        ("f0", "f1"),
        ("f0", "f2"),
        ("f0", "f3"),
        ("f1", "f2"),
        ("f1", "f3"),
        ("f2", "f3"),
        ("f0", "f1", "f2"),
        ("f0", "f1", "f3"),
        ("f0", "f2", "f3"),
        ("f1", "f2", "f3"),
    }
    assert combo_sets == expected_sets


def test_generate_combinations_raises_when_exceeding_maximum():
    config = CombinerConfig(top_n=5, max_factors=4, max_combinations=5)
    combiner = MultiFactorCombiner("0700.HK", {}, config=config)
    factors = [
        {"factor": f"f{i}", "timeframe": "1d", "sharpe_ratio": 1.0, "information_coefficient": 0.2}
        for i in range(5)
    ]

    with pytest.raises(ValueError) as excinfo:
        combiner.generate_combinations(factors)

    assert "exceeds" in str(excinfo.value)


def test_backtest_combination_with_numpy_returns_computes_expected_metrics():
    returns_alpha = np.array([0.02, -0.01, 0.015, 0.005], dtype=float)
    returns_beta = np.array([0.01, 0.0, -0.005, 0.008], dtype=float)

    combo = [
        {
            "factor": "alpha",
            "timeframe": "1d",
            "returns": returns_alpha,
            "information_coefficient": 0.30,
        },
        {
            "factor": "beta",
            "timeframe": "4h",
            "returns": returns_beta,
            "information_coefficient": -0.10,
        },
    ]

    combiner = MultiFactorCombiner("0700.HK", {})
    result = combiner.backtest_combination(combo)

    if pd is not None:
        expected_returns = pd.concat(
            [pd.Series(returns_alpha, dtype=float), pd.Series(returns_beta, dtype=float)],
            axis=1,
            join="inner",
        ).mean(axis=1)
        assert "returns" in result
        assert isinstance(result["returns"], pd.Series)
        assert result["returns"].equals(expected_returns.astype(float))
    else:  # pragma: no cover - exercised when pandas is not installed
        expected_returns = np.vstack((returns_alpha, returns_beta)).mean(axis=0)
        assert "returns" not in result

    expected_array = np.asarray(expected_returns, dtype=float)

    assert result["strategy_name"] == "alpha+beta"
    assert result["factors"] == ["alpha", "beta"]
    assert result["timeframes"] == ["1d", "4h"]
    assert np.isclose(result["average_information_coefficient"], np.mean([0.30, -0.10]))
    assert np.isclose(
        result["sharpe_ratio"], PerformanceMetrics.calculate_sharpe_ratio(expected_array)
    )
    assert np.isclose(
        result["stability"], PerformanceMetrics.calculate_stability(expected_array)
    )
    gains = expected_array[expected_array > 0]
    losses = expected_array[expected_array < 0]
    assert np.isclose(
        result["profit_factor"],
        PerformanceMetrics.calculate_profit_factor(gains, losses),
    )
    equity_curve = np.cumprod(1 + expected_array)
    assert np.isclose(
        result["max_drawdown"], PerformanceMetrics.calculate_max_drawdown(equity_curve)
    )
    assert result["trades_count"] == int(np.count_nonzero(np.diff(np.sign(expected_array))))
    assert np.isclose(result["win_rate"], float(np.mean(expected_array > 0)))


def test_backtest_combination_returns_empty_when_no_valid_returns():
    combo = [
        {"factor": "empty_a", "timeframe": "1d", "returns": None},
        {"factor": "empty_b", "timeframe": "4h", "returns": None},
    ]

    combiner = MultiFactorCombiner("0700.HK", {})
    assert combiner.backtest_combination(combo) == {}


def test_discover_strategies_tracks_shortlist_and_sorts(monkeypatch):
    phase1_results = {
        "alpha": _make_factor(name="alpha", sharpe=1.60, ic=0.20),
        "beta": _make_factor(name="beta", sharpe=1.60, ic=0.40),
        "gamma": _make_factor(name="gamma", sharpe=1.40, ic=0.20),
        "delta": _make_factor(name="delta", sharpe=0.20, ic=0.60),
        "epsilon": _make_factor(name="epsilon", sharpe=1.50, ic=0.01),
    }

    config = CombinerConfig(
        top_n=3,
        max_factors=3,
        max_combinations=10,
        min_sharpe=1.0,
        min_information_coefficient=0.05,
    )
    combiner = MultiFactorCombiner("0700.HK", phase1_results, config=config)

    expected_top = combiner.select_top_factors(top_n=3)

    def fake_backtest(self: MultiFactorCombiner, combo: Sequence[Mapping[str, object]]):
        names: Tuple[str, ...] = tuple(factor["factor"] for factor in combo)
        fake_payloads = {
            ("beta", "alpha"): {"strategy_name": "beta+alpha", "sharpe_ratio": 1.30},
            ("beta", "gamma"): {"strategy_name": "beta+gamma", "sharpe_ratio": 1.60},
            ("alpha", "gamma"): {"strategy_name": "alpha+gamma", "sharpe_ratio": 0.80},
            ("beta", "alpha", "gamma"): {
                "strategy_name": "beta+alpha+gamma",
                "sharpe_ratio": 1.10,
            },
        }
        payload = fake_payloads.get(names)
        if not payload:
            return {}
        return {
            "symbol": self.symbol,
            "factors": list(names),
            "timeframes": [factor["timeframe"] for factor in combo],
            **payload,
        }

    monkeypatch.setattr(
        MultiFactorCombiner,
        "backtest_combination",
        fake_backtest,
        raising=False,
    )

    strategies = combiner.discover_strategies()

    assert [s["strategy_name"] for s in strategies] == [
        "beta+gamma",
        "beta+alpha",
        "beta+alpha+gamma",
    ]
    assert all(s["sharpe_ratio"] >= config.min_sharpe for s in strategies)
    assert combiner.last_selected_factors == expected_top
