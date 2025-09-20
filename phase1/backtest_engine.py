"""Vectorized backtest engine for single-factor evaluation."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from ..utils.cost_model import HongKongTradingCosts
from ..utils.performance_metrics import PerformanceMetrics


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
        close = data["close"].astype(float)
        returns = close.pct_change().fillna(0.0).to_numpy()
        future_returns = close.pct_change().shift(-1).fillna(0.0).to_numpy()
        raw_signals = signals.fillna(0.0).to_numpy(dtype=float)
        positions = signals.shift(1).fillna(0.0).to_numpy() * self.allocation
        strategy_returns = returns * positions

        trade_changes = np.abs(np.diff(np.concatenate([[0.0], positions])))
        trade_cost = self.costs.calculate_total_cost(self.initial_capital * self.allocation)
        cost_returns = (trade_changes > 0).astype(float) * (trade_cost / self.initial_capital)
        strategy_returns -= cost_returns

        equity_curve = self.initial_capital * np.cumprod(1 + strategy_returns)
        gains = strategy_returns[strategy_returns > 0]
        losses = strategy_returns[strategy_returns < 0]
        trades = strategy_returns[trade_changes > 0]

        sharpe = PerformanceMetrics.calculate_sharpe_ratio(strategy_returns)
        stability = PerformanceMetrics.calculate_stability(strategy_returns)
        profit_factor = PerformanceMetrics.calculate_profit_factor(gains, losses)
        max_drawdown = PerformanceMetrics.calculate_max_drawdown(equity_curve)
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
