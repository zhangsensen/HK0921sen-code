import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from phase1.backtest_engine import SimpleBacktestEngine
from utils.performance_metrics import PerformanceMetrics


def _build_phase1_fixture():
    index = pd.date_range("2024-03-01 09:30", periods=6, freq="1min")
    close = pd.Series([100.0, 101.0, 99.0, 102.0, 101.0, 103.0], index=index)
    data = pd.DataFrame(
        {
            "open": close,
            "high": close + 0.5,
            "low": close - 0.5,
            "close": close,
            "volume": [1000, 1100, 900, 1200, 950, 1300],
        },
        index=index,
    )
    # Missing the first timestamp to trigger the reindex branch and include NaNs.
    signals = pd.Series([1.0, np.nan, -1.0, -1.0, 0.5], index=index[1:])
    return data, signals


def test_backtest_engine_phase1_metrics_with_nan_and_reindex(monkeypatch):
    data, signals = _build_phase1_fixture()
    engine = SimpleBacktestEngine("0700.HK", allocation=1.0)

    base_cost_fn = engine.costs.calculate_total_cost
    discount = 0.5
    monkeypatch.setattr(
        engine.costs,
        "calculate_total_cost",
        lambda trade_value: base_cost_fn(trade_value) * discount,
    )

    result = engine.backtest_factor(data, signals)

    aligned_signals = signals.reindex(data.index).astype(float).fillna(0.0)
    positions = aligned_signals.shift(1).fillna(0.0) * engine.allocation
    returns = data["close"].pct_change().fillna(0.0).astype(float)
    raw_strategy_returns = (returns * positions).astype(float)
    previous_positions = positions.shift(1).fillna(0.0)
    trade_changes = (positions - previous_positions).abs()
    expected_trade_cost = base_cost_fn(engine.initial_capital * engine.allocation) * discount
    expected_cost_drag = (trade_changes > 0).astype(float) * (
        expected_trade_cost / engine.initial_capital
    )
    expected_strategy_returns = raw_strategy_returns - expected_cost_drag
    expected_equity_curve = engine.initial_capital * (1 + expected_strategy_returns).cumprod()

    pd.testing.assert_series_equal(
        result["returns"], expected_strategy_returns, check_names=False
    )
    pd.testing.assert_series_equal(
        result["equity_curve"], expected_equity_curve, check_names=False
    )

    assert result["trades_count"] == int((trade_changes > 0).sum())
    expected_win_rate = float((expected_strategy_returns[trade_changes > 0] > 0).mean())
    assert result["win_rate"] == pytest.approx(expected_win_rate)

    strategy_array = expected_strategy_returns.to_numpy(dtype=float)
    assert result["sharpe_ratio"] == pytest.approx(
        PerformanceMetrics.calculate_sharpe_ratio(strategy_array)
    )

    gains = expected_strategy_returns[expected_strategy_returns > 0].to_numpy(dtype=float)
    losses = expected_strategy_returns[expected_strategy_returns < 0].to_numpy(dtype=float)
    assert result["profit_factor"] == pytest.approx(
        PerformanceMetrics.calculate_profit_factor(gains, losses)
    )

    equity_array = expected_equity_curve.to_numpy(dtype=float)
    assert result["max_drawdown"] == pytest.approx(
        PerformanceMetrics.calculate_max_drawdown(equity_array)
    )

    future_returns = returns.shift(-1).fillna(0.0).to_numpy(dtype=float)
    assert result["information_coefficient"] == pytest.approx(
        PerformanceMetrics.calculate_information_coefficient(
            aligned_signals.to_numpy(dtype=float), future_returns
        )
    )

    pd.testing.assert_series_equal(
        raw_strategy_returns - expected_strategy_returns,
        expected_cost_drag,
        check_names=False,
    )

    assert not signals.index.equals(data.index)
