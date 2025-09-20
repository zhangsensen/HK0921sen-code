"""Momentum-oriented factors."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from .base_factor import register_factor
from .common import ema


def _rsi(close: "pd.Series", period: int) -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _stochastic(close: "pd.Series", high: "pd.Series", low: "pd.Series", period: int) -> "pd.Series":
    highest = high.rolling(period, min_periods=period).max()
    lowest = low.rolling(period, min_periods=period).min()
    return 100 * (close - lowest) / (highest - lowest).replace(0, np.nan)


def _stoch_rsi(close: "pd.Series", period: int) -> "pd.Series":
    rsi = _rsi(close, period)
    lowest = rsi.rolling(period).min()
    highest = rsi.rolling(period).max()
    return (rsi - lowest) / (highest - lowest).replace(0, np.nan)


def _money_flow_index(high: "pd.Series", low: "pd.Series", close: "pd.Series", volume: "pd.Series", period: int) -> "pd.Series":
    typical_price = (high + low + close) / 3
    money_flow = typical_price * volume
    delta = typical_price.diff()
    positive_flow = money_flow.where(delta > 0, 0.0)
    negative_flow = money_flow.where(delta < 0, 0.0)
    positive = positive_flow.rolling(period, min_periods=period).sum()
    negative = -negative_flow.rolling(period, min_periods=period).sum()
    ratio = positive / negative.replace(0, np.nan)
    return 100 - (100 / (1 + ratio))


def _tsi(close: "pd.Series", short: int, long: int) -> "pd.Series":
    momentum = close.diff()
    ema1 = momentum.ewm(span=short, adjust=False).mean()
    ema2 = ema1.ewm(span=long, adjust=False).mean()
    abs_momentum = momentum.abs()
    ema3 = abs_momentum.ewm(span=short, adjust=False).mean()
    ema4 = ema3.ewm(span=long, adjust=False).mean()
    return 100 * ema2 / ema4.replace(0, np.nan)


def _ultimate_oscillator(high: "pd.Series", low: "pd.Series", close: "pd.Series") -> "pd.Series":
    bp = close - np.minimum(low, close.shift(1))
    tr = np.maximum(high, close.shift(1)) - np.minimum(low, close.shift(1))
    avg1 = (bp.rolling(7).sum() / tr.rolling(7).sum()).replace(0, np.nan)
    avg2 = (bp.rolling(14).sum() / tr.rolling(14).sum()).replace(0, np.nan)
    avg3 = (bp.rolling(28).sum() / tr.rolling(28).sum()).replace(0, np.nan)
    return 100 * (4 * avg1 + 2 * avg2 + avg3) / 7


def _chande_momentum(close: "pd.Series", period: int) -> "pd.Series":
    delta = close.diff()
    up = delta.clip(lower=0).rolling(period).sum()
    down = (-delta.clip(upper=0)).rolling(period).sum()
    return 100 * (up - down) / (up + down).replace(0, np.nan)


def _dx(high: "pd.Series", low: "pd.Series", close: "pd.Series", period: int) -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")
    up_move = high.diff()
    down_move = low.diff().abs()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = (high.combine(high.shift(1), np.maximum) - low.combine(low.shift(1), np.minimum)).rolling(period).sum()
    plus_di = 100 * pd.Series(plus_dm, index=close.index).rolling(period).sum() / tr
    minus_di = 100 * pd.Series(minus_dm, index=close.index).rolling(period).sum() / tr
    return 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)


register_factor("rsi_2", "momentum", lambda data: _rsi(data["close"], 2))
register_factor("rsi_14", "momentum", lambda data: _rsi(data["close"], 14))
register_factor("rsi_100", "momentum", lambda data: _rsi(data["close"], 100))
register_factor("stoch_rsi", "momentum", lambda data: _stoch_rsi(data["close"], 14))
register_factor("stoch_k", "momentum", lambda data: _stochastic(data["close"], data["high"], data["low"], 14))
register_factor("stoch_d", "momentum", lambda data: _stochastic(data["close"], data["high"], data["low"], 14).rolling(3).mean())
register_factor(
    "mfi_14",
    "momentum",
    lambda data: _money_flow_index(data["high"], data["low"], data["close"], data["volume"], 14),
)
register_factor("tsi_25", "momentum", lambda data: _tsi(data["close"], 13, 25))
register_factor("uo_7", "momentum", lambda data: _ultimate_oscillator(data["high"], data["low"], data["close"]))
register_factor("wr_14", "momentum", lambda data: -_stochastic(data["close"], data["high"], data["low"], 14))
register_factor("cmo_14", "momentum", lambda data: _chande_momentum(data["close"], 14))
register_factor("dx_14", "momentum", lambda data: _dx(data["high"], data["low"], data["close"], 14))
