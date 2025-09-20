"""Vectorized backtest engine for single-factor evaluation."""
from __future__ import annotations

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from utils.cost_model import HongKongTradingCosts
from utils.performance_metrics import PerformanceMetrics


class SimpleBacktestEngine:
    """Execute simplified backtests for factor signals."""

    def __init__(
        self,
        symbol: str,
        initial_capital: float = 100_000,
        allocation: float = 0.1,
    ) -> None:
        if pd is None:
            raise ModuleNotFoundError("pandas is required for backtests")
        self.symbol = symbol
        self.initial_capital = float(initial_capital)
        self.allocation = float(allocation)
        self.costs = HongKongTradingCosts()

    def backtest_factor(self, data: "pd.DataFrame", signals: "pd.Series") -> dict:
        if not isinstance(signals, pd.Series):
            signals = pd.Series(signals, index=data.index, dtype=float)
        else:
            if not signals.index.equals(data.index):
                signals = signals.reindex(data.index)
            signals = signals.astype(float)
        signals = signals.fillna(0.0)

        close = data["close"].astype(float)
        returns = close.pct_change().fillna(0.0)
        future_returns = returns.shift(-1).fillna(0.0).to_numpy(dtype=float)
        raw_signals = signals.to_numpy(dtype=float)
        positions = signals.shift(1).fillna(0.0) * self.allocation
        strategy_returns = (returns * positions).astype(float)

        previous_positions = positions.shift(1).fillna(0.0)
        trade_changes = (positions - previous_positions).abs()
        trade_cost = self.costs.calculate_total_cost(self.initial_capital * self.allocation)
        cost_returns = (trade_changes > 0).astype(float) * (trade_cost / self.initial_capital)
        strategy_returns = strategy_returns - cost_returns

        equity_curve = self.initial_capital * (1 + strategy_returns).cumprod()
        strategy_array = strategy_returns.to_numpy(dtype=float)
        gains = strategy_returns[strategy_returns > 0].to_numpy(dtype=float)
        losses = strategy_returns[strategy_returns < 0].to_numpy(dtype=float)
        trades = strategy_returns[trade_changes > 0].to_numpy(dtype=float)

        sharpe = PerformanceMetrics.calculate_sharpe_ratio(strategy_array)
        stability = PerformanceMetrics.calculate_stability(strategy_array)
        profit_factor = PerformanceMetrics.calculate_profit_factor(gains, losses)
        max_drawdown = PerformanceMetrics.calculate_max_drawdown(
            equity_curve.to_numpy(dtype=float)
        )
        win_rate = float((trades > 0).mean()) if trades.size else 0.0
        information_coefficient = PerformanceMetrics.calculate_information_coefficient(
            raw_signals, future_returns
        )

        return {
            "symbol": self.symbol,
            "returns": strategy_returns,
            "equity_curve": equity_curve,
            "sharpe_ratio": sharpe,
            "stability": stability,
            "trades_count": int((trade_changes > 0).sum()),
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "max_drawdown": max_drawdown,
            "information_coefficient": information_coefficient,
        }
