import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from phase1.backtest_engine import SimpleBacktestEngine


def _build_sample_data():
    index = pd.date_range("2024-01-01 09:30", periods=5, freq="1min")
    close = pd.Series([100.0, 101.0, 101.5, 102.0, 102.5], index=index)
    data = pd.DataFrame(
        {
            "open": close,
            "high": close + 0.2,
            "low": close - 0.2,
            "close": close,
            "volume": [1_000, 1_050, 980, 1_100, 1_040],
        },
        index=index,
    )
    # Enter on the second bar and flat again on the final bar.
    signals = pd.Series([0, 1, 1, 1, 0], index=index, name="factor_signal")
    return data, signals


def test_backtest_engine_scales_returns_with_allocation(monkeypatch):
    data, signals = _build_sample_data()

    engine_full = SimpleBacktestEngine("0700.HK", allocation=1.0)
    engine_partial = SimpleBacktestEngine("0700.HK", allocation=0.25)

    monkeypatch.setattr(engine_full.costs, "calculate_total_cost", lambda _: 0.0)
    monkeypatch.setattr(engine_partial.costs, "calculate_total_cost", lambda _: 0.0)

    result_full = engine_full.backtest_factor(data, signals)
    result_partial = engine_partial.backtest_factor(data, signals)

    assert np.allclose(result_partial["returns"], result_full["returns"] * 0.25)
    assert result_full["trades_count"] == result_partial["trades_count"]


def test_backtest_engine_applies_transaction_costs(monkeypatch):
    data, signals = _build_sample_data()

    engine_no_cost = SimpleBacktestEngine("0700.HK", allocation=1.0)
    monkeypatch.setattr(engine_no_cost.costs, "calculate_total_cost", lambda _: 0.0)
    baseline = engine_no_cost.backtest_factor(data, signals)

    fixed_cost = 100.0
    engine_with_cost = SimpleBacktestEngine("0700.HK", allocation=1.0)
    monkeypatch.setattr(engine_with_cost.costs, "calculate_total_cost", lambda _: fixed_cost)
    with_cost = engine_with_cost.backtest_factor(data, signals)

    expected_drag = fixed_cost / engine_with_cost.initial_capital
    difference = baseline["returns"] - with_cost["returns"]
    non_zero = difference[np.abs(difference) > 0]
    assert non_zero.size == with_cost["trades_count"]
    assert np.allclose(non_zero, expected_drag)
