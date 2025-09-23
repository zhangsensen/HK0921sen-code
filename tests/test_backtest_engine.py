import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from phase1.backtest_engine import SimpleBacktestEngine


def _build_sample_data():
    # Increase data points to meet minimum requirements (20+ points)
    index = pd.date_range("2024-01-01 09:30", periods=25, freq="1min")
    
    # Generate realistic price data with some trend and volatility
    np.random.seed(42)  # For reproducible tests
    base_price = 100.0
    price_changes = np.random.normal(0, 0.5, 25)
    price_changes[0] = 0  # Start at base price
    close_prices = base_price + np.cumsum(price_changes)
    
    close = pd.Series(close_prices, index=index)
    data = pd.DataFrame(
        {
            "open": close.shift(1).fillna(close.iloc[0]),
            "high": close + abs(np.random.normal(0, 0.3, 25)),
            "low": close - abs(np.random.normal(0, 0.3, 25)),
            "close": close,
            "volume": np.random.randint(900, 1200, 25),
        },
        index=index,
    )
    
    # Create more realistic trading signals
    # Enter long when price is trending up, exit when trending down
    signals = pd.Series(0, index=index, name="factor_signal")
    signals.iloc[5:15] = 1  # Long position for middle period
    signals.iloc[15:20] = -1  # Short position
    # Rest are flat (0)
    
    return data, signals


def test_backtest_engine_scales_returns_with_allocation(monkeypatch):
    data, signals = _build_sample_data()

    engine_full = SimpleBacktestEngine("0700.HK", allocation=1.0)
    engine_partial = SimpleBacktestEngine("0700.HK", allocation=0.25)

    monkeypatch.setattr(engine_full.costs, "calculate_total_cost", lambda _: 0.0)
    monkeypatch.setattr(engine_partial.costs, "calculate_total_cost", lambda _: 0.0)

    result_full = engine_full.backtest_factor(data, signals)
    result_partial = engine_partial.backtest_factor(data, signals)

    assert isinstance(result_full["returns"], pd.Series)
    assert list(result_full["returns"].index) == list(data.index)
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


def test_backtest_engine_aligns_numpy_signals(monkeypatch):
    data, signals = _build_sample_data()
    engine = SimpleBacktestEngine("0700.HK", allocation=0.5)
    monkeypatch.setattr(engine.costs, "calculate_total_cost", lambda _: 0.0)

    numpy_signals = signals.to_numpy(dtype=float)
    result = engine.backtest_factor(data, numpy_signals)

    assert isinstance(result["returns"], pd.Series)
    assert list(result["returns"].index) == list(data.index)
    assert isinstance(result["equity_curve"], pd.Series)
