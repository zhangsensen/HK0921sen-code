"""Performance metric helpers for the discovery workflow."""
from __future__ import annotations

import numpy as np


class PerformanceMetrics:
    """Collection of static helpers to compute portfolio statistics."""

    @staticmethod
    def calculate_sharpe_ratio(returns: np.ndarray, risk_free_rate: float = 0.02) -> float:
        if returns.size == 0:
            return 0.0
        excess_returns = returns - risk_free_rate / 252.0
        std = np.std(excess_returns, ddof=1)
        if std == 0 or np.isnan(std):
            return 0.0
        return float(np.sqrt(252) * np.mean(excess_returns) / std)

    @staticmethod
    def calculate_stability(returns: np.ndarray) -> float:
        if returns.size <= 1:
            return 0.0
        cumulative_returns = np.cumprod(1 + returns)
        x = np.arange(len(cumulative_returns))
        x_mean = x.mean()
        y_mean = cumulative_returns.mean()
        cov = np.mean((x - x_mean) * (cumulative_returns - y_mean))
        var_x = np.mean((x - x_mean) ** 2)
        var_y = np.mean((cumulative_returns - y_mean) ** 2)
        if var_x == 0 or var_y == 0:
            return 0.0
        r_value = cov / np.sqrt(var_x * var_y)
        return float(r_value ** 2)

    @staticmethod
    def calculate_profit_factor(gains: np.ndarray, losses: np.ndarray) -> float:
        total_gains = gains.sum()
        total_losses = losses.sum()
        if np.isclose(total_losses, 0):
            return float("inf") if total_gains > 0 else 0.0
        return float(total_gains / abs(total_losses))

    @staticmethod
    def calculate_max_drawdown(equity_curve: np.ndarray) -> float:
        if equity_curve.size == 0:
            return 0.0
        running_max = np.maximum.accumulate(equity_curve)
        drawdown = (running_max - equity_curve) / running_max
        return float(np.nanmax(drawdown))

    @staticmethod
    def calculate_information_coefficient(signals: np.ndarray, future_returns: np.ndarray) -> float:
        """Pearson correlation between signals and subsequent period returns."""

        if signals.size == 0 or future_returns.size == 0:
            return 0.0

        limit = min(signals.size, future_returns.size)
        if limit == 0:
            return 0.0

        aligned_signals = np.asarray(signals[:limit], dtype=float)
        aligned_returns = np.asarray(future_returns[:limit], dtype=float)
        mask = ~np.isnan(aligned_signals) & ~np.isnan(aligned_returns)
        if not np.any(mask):
            return 0.0

        valid_signals = aligned_signals[mask]
        valid_returns = aligned_returns[mask]
        if valid_signals.size == 0 or valid_returns.size == 0:
            return 0.0

        # Skip constant signals to avoid numpy warnings and meaningless correlations.
        if np.nanstd(valid_signals) == 0 or np.nanstd(valid_returns) == 0:
            return 0.0

        coefficient = np.corrcoef(valid_signals, valid_returns)[0, 1]
        if np.isnan(coefficient):
            return 0.0
        return float(coefficient)
