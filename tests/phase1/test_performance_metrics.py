import math

import pytest

np = pytest.importorskip("numpy")

from utils.performance_metrics import PerformanceMetrics


@pytest.mark.parametrize(
    ("returns", "expected"),
    [
        ([], 0.0),
        ([0.01, 0.01, 0.01], 0.0),
    ],
)
def test_calculate_sharpe_ratio_edge_cases(returns, expected):
    assert PerformanceMetrics.calculate_sharpe_ratio(returns) == pytest.approx(expected)


def test_calculate_sharpe_ratio_matches_manual_formula():
    returns = [0.01, -0.02, 0.03]
    excess = np.asarray(returns, dtype=float) - 0.02 / 252.0
    expected = float(np.sqrt(252) * excess.mean() / excess.std(ddof=1))
    assert PerformanceMetrics.calculate_sharpe_ratio(returns) == pytest.approx(expected)


@pytest.mark.parametrize(
    "returns",
    [
        [],
        [0.01, 0.01, 0.01],
    ],
)
def test_calculate_stability_edge_cases(returns):
    if not returns:
        expected = 0.0
    else:
        array = np.asarray(returns, dtype=float)
        cumulative_returns = np.cumprod(1 + array)
        x = np.arange(len(cumulative_returns), dtype=float)
        x_mean = x.mean()
        y_mean = cumulative_returns.mean()
        cov = np.mean((x - x_mean) * (cumulative_returns - y_mean))
        var_x = np.mean((x - x_mean) ** 2)
        var_y = np.mean((cumulative_returns - y_mean) ** 2)
        expected = 0.0 if var_x == 0 or var_y == 0 else float((cov / np.sqrt(var_x * var_y)) ** 2)
    assert PerformanceMetrics.calculate_stability(returns) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("gains", "losses", "expected"),
    [
        ([], [], 0.0),
        ([0.02], [], math.inf),
        ([0.01, 0.02], [-0.02], 1.5),
    ],
)
def test_calculate_profit_factor_edge_cases(gains, losses, expected):
    result = PerformanceMetrics.calculate_profit_factor(gains, losses)
    if math.isinf(expected):
        assert math.isinf(result)
    else:
        assert result == pytest.approx(expected)


@pytest.mark.parametrize(
    ("equity", "expected"),
    [
        ([], 0.0),
        ([100.0, 120.0, 90.0, 95.0], 0.25),
    ],
)
def test_calculate_max_drawdown_edge_cases(equity, expected):
    assert PerformanceMetrics.calculate_max_drawdown(equity) == pytest.approx(expected)


@pytest.mark.parametrize(
    ("signals", "future_returns", "expected"),
    [
        ([], [], 0.0),
        ([np.nan, np.nan], [0.01, 0.02], 0.0),
        ([0.1, np.nan, -0.2, 0.3], [0.05, 0.01, np.nan, 0.04], -1.0),
    ],
)
def test_calculate_information_coefficient_with_nans(signals, future_returns, expected):
    result = PerformanceMetrics.calculate_information_coefficient(signals, future_returns)
    assert result == pytest.approx(expected, abs=1e-12)
