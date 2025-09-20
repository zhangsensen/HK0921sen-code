"""Collection of helpers to compute basic portfolio statistics."""
from __future__ import annotations

from typing import Sequence

try:  # pragma: no cover - optional dependency guard
    import numpy as _np  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - allows lightweight test environments
    _np = None  # type: ignore[assignment]


class PerformanceMetrics:
    """Namespace of statistical helpers used by backtests and combiners."""

    @staticmethod
    def _require_numpy():  # type: ignore[return-any]
        if _np is None:
            raise ModuleNotFoundError("numpy is required for performance metric calculations")
        return _np

    @staticmethod
    def calculate_sharpe_ratio(returns: Sequence[float], risk_free_rate: float = 0.02) -> float:
        np = PerformanceMetrics._require_numpy()
        array = np.asarray(returns, dtype=float)
        if array.size == 0:
            return 0.0
        excess_returns = array - risk_free_rate / 252.0
        std = np.std(excess_returns, ddof=1)
        if std == 0 or np.isnan(std):
            return 0.0
        return float(np.sqrt(252) * np.mean(excess_returns) / std)

    @staticmethod
    def calculate_stability(returns: Sequence[float]) -> float:
        np = PerformanceMetrics._require_numpy()
        array = np.asarray(returns, dtype=float)
        if array.size <= 1:
            return 0.0
        cumulative_returns = np.cumprod(1 + array)
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
    def calculate_profit_factor(gains: Sequence[float], losses: Sequence[float]) -> float:
        np = PerformanceMetrics._require_numpy()
        gains_array = np.asarray(gains, dtype=float)
        losses_array = np.asarray(losses, dtype=float)
        total_gains = gains_array.sum()
        total_losses = losses_array.sum()
        if np.isclose(total_losses, 0):
            return float("inf") if total_gains > 0 else 0.0
        return float(total_gains / abs(total_losses))

    @staticmethod
    def calculate_max_drawdown(equity_curve: Sequence[float]) -> float:
        np = PerformanceMetrics._require_numpy()
        array = np.asarray(equity_curve, dtype=float)
        if array.size == 0:
            return 0.0
        running_max = np.maximum.accumulate(array)
        drawdown = (running_max - array) / running_max
        return float(np.nanmax(drawdown))

    @staticmethod
    def calculate_information_coefficient(signals: Sequence[float], future_returns: Sequence[float]) -> float:
        np = PerformanceMetrics._require_numpy()
        signal_array = np.asarray(signals, dtype=float)
        return_array = np.asarray(future_returns, dtype=float)

        if signal_array.size == 0 or return_array.size == 0:
            return 0.0

        limit = min(signal_array.size, return_array.size)
        if limit == 0:
            return 0.0

        aligned_signals = signal_array[:limit]
        aligned_returns = return_array[:limit]
        mask = ~np.isnan(aligned_signals) & ~np.isnan(aligned_returns)
        if not np.any(mask):
            return 0.0

        valid_signals = aligned_signals[mask]
        valid_returns = aligned_returns[mask]
        if valid_signals.size == 0 or valid_returns.size == 0:
            return 0.0

        if np.nanstd(valid_signals) == 0 or np.nanstd(valid_returns) == 0:
            return 0.0

        coefficient = np.corrcoef(valid_signals, valid_returns)[0, 1]
        if np.isnan(coefficient):
            return 0.0
        return float(coefficient)


__all__ = ["PerformanceMetrics"]
