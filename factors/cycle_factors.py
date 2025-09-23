"""Cycle detection factors."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from .base_factor import register_factor


def _dominant_cycle(close: "pd.Series", window: int) -> "pd.Series":
    def _calc(values: np.ndarray) -> float:
        detrended = values - values.mean()
        spectrum = np.fft.rfft(detrended)
        power = np.abs(spectrum)
        if power.size <= 1:
            return 0.0
        return float(np.argmax(power[1:]) + 1)

    return close.rolling(window, min_periods=window).apply(_calc, raw=True)


def _sinewave(close: "pd.Series", window: int) -> "pd.Series":
    mean = close.rolling(window).mean()
    std = close.rolling(window).std(ddof=0)
    z = (close - mean) / std.replace(0, np.nan)
    return np.sin(z)


def _cycle_phase(close: "pd.Series", window: int) -> "pd.Series":
    delta = close.diff()
    smoothed = close.rolling(window).mean()
    return np.arctan2(delta, smoothed).fillna(0)


def _rsi_period(close: "pd.Series", base_period: int) -> "pd.Series":
    volatility = close.pct_change(fill_method=None).rolling(base_period).std(ddof=0)
    return base_period * (1 + volatility)


def _trendflex(close: "pd.Series", window: int) -> "pd.Series":
    def _calc(values: np.ndarray) -> float:
        numerator = values[-1] - values[0]
        denominator = np.sum(np.abs(np.diff(values)))
        return numerator / denominator if denominator != 0 else 0.0

    return close.rolling(window, min_periods=window).apply(_calc, raw=True)


def _cycle_momentum(close: "pd.Series", window: int) -> "pd.Series":
    return close - close.rolling(window).mean()


register_factor("dominant_cycle", "cycle", lambda data: _dominant_cycle(data["close"], 50))
register_factor("sinewave", "cycle", lambda data: _sinewave(data["close"], 20))
register_factor("dc_phase", "cycle", lambda data: _cycle_phase(data["close"], 30))
register_factor("rsi_period", "cycle", lambda data: _rsi_period(data["close"], 14))
register_factor("trendflex", "cycle", lambda data: _trendflex(data["close"], 20))
register_factor("cycle_momentum", "cycle", lambda data: _cycle_momentum(data["close"], 14))
