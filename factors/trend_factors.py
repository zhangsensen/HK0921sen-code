"""Trend-oriented factors."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from .base_factor import register_factor
from .common import atr, ema, rate_of_change, sma


def _dema(close: "pd.Series", span: int) -> "pd.Series":
    ema1 = ema(close, span)
    ema2 = ema(ema1, span)
    return 2 * ema1 - ema2


def _tema(close: "pd.Series", span: int) -> "pd.Series":
    ema1 = ema(close, span)
    ema2 = ema(ema1, span)
    ema3 = ema(ema2, span)
    return 3 * ema1 - 3 * ema2 + ema3


def _kama(close: "pd.Series", period: int) -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")
    change = close.diff(period).abs()
    volatility = close.diff().abs().rolling(period).sum()
    efficiency_ratio = change / volatility.replace(0, np.nan)
    fast = 2 / (2 + 1)
    slow = 2 / (30 + 1)
    smoothing_constant = (efficiency_ratio * (fast - slow) + slow) ** 2
    kama = close.copy().astype(float)
    kama.iloc[: period + 1] = close.iloc[: period + 1]
    for i in range(period + 1, len(close)):
        alpha = smoothing_constant.iat[i]
        if np.isnan(alpha):
            kama.iat[i] = kama.iat[i - 1]
        else:
            kama.iat[i] = kama.iat[i - 1] + alpha * (close.iat[i] - kama.iat[i - 1])
    return kama


def _trix(close: "pd.Series", period: int) -> "pd.Series":
    ema1 = ema(close, period)
    ema2 = ema(ema1, period)
    ema3 = ema(ema2, period)
    return ema3.pct_change() * 100


def _aroon(series: "pd.Series", period: int, mode: str) -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")

    def _calc(window: "pd.Series") -> float:
        idx = window.argmax() if mode == "up" else window.argmin()
        return (period - (period - idx - 1)) / period * 100

    return series.rolling(period, min_periods=period).apply(_calc, raw=False)


def _directional_index(high: "pd.Series", low: "pd.Series", close: "pd.Series", period: int) -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")
    up_move = high.diff()
    down_move = low.diff().abs()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = atr(high, low, close, period)
    plus_di = 100 * ema(pd.Series(plus_dm, index=close.index), period) / tr
    minus_di = 100 * ema(pd.Series(minus_dm, index=close.index), period) / tr
    dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)) * 100
    return ema(dx, period)


register_factor("sma_20", "trend", lambda data: sma(data["close"], 20))
register_factor("ema_12", "trend", lambda data: ema(data["close"], 12))
register_factor("dema_14", "trend", lambda data: _dema(data["close"], 14))
register_factor("tema_14", "trend", lambda data: _tema(data["close"], 14))
register_factor("kama_14", "trend", lambda data: _kama(data["close"], 14))
register_factor("trix_14", "trend", lambda data: _trix(data["close"], 14))
register_factor("aroon_up", "trend", lambda data: _aroon(data["high"], 14, "up"))
register_factor("aroon_down", "trend", lambda data: _aroon(data["low"], 14, "down"))
register_factor("adx_14", "trend", lambda data: _directional_index(data["high"], data["low"], data["close"], 14))
register_factor("macd", "trend", lambda data: ema(data["close"], 12) - ema(data["close"], 26))
register_factor("ppo", "trend", lambda data: (ema(data["close"], 12) - ema(data["close"], 26)) / ema(data["close"], 26))
register_factor("apo", "trend", lambda data: ema(data["close"], 12) - ema(data["close"], 26))
register_factor(
    "cci_14",
    "trend",
    lambda data: (data["close"] - (data["high"] + data["low"] + data["close"]) / 3)
    / (0.015 * (data["high"] - data["low"]).rolling(14, min_periods=14).mean()),
)
register_factor("roc_12", "trend", lambda data: rate_of_change(data["close"], 12))
register_factor(
    "willr_14",
    "trend",
    lambda data: -100
    * (data["high"].rolling(14).max() - data["close"]) / (data["high"].rolling(14).max() - data["low"].rolling(14).min()),
)
