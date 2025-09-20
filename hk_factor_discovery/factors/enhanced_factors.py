"""Enhanced engineered factors."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from .base_factor import register_factor
from .common import atr, ema


def _macd_enhanced(data: "pd.DataFrame") -> "pd.Series":
    macd = ema(data["close"], 12) - ema(data["close"], 26)
    signal = ema(macd, 9)
    volume_ratio = data["volume"] / data["volume"].rolling(20).mean().replace(0, np.nan)
    return (macd - signal) * volume_ratio


def _rsi_enhanced(data: "pd.DataFrame") -> "pd.Series":
    delta = data["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(span=14, adjust=False).mean()
    avg_loss = loss.ewm(span=14, adjust=False).mean()
    rsi = 100 - (100 / (1 + avg_gain / avg_loss.replace(0, np.nan)))
    volume_weight = data["volume"] / data["volume"].rolling(20).mean().replace(0, np.nan)
    return rsi * volume_weight


def _atr_enhanced(data: "pd.DataFrame") -> "pd.Series":
    atr_value = atr(data["high"], data["low"], data["close"], 14)
    trend = ema(data["close"], 20) - ema(data["close"], 50)
    return atr_value * trend


def _smart_money_flow(data: "pd.DataFrame") -> "pd.Series":
    intraday_move = data["close"] - data["open"]
    last_move = data["close"].shift(1) - data["open"].shift(1)
    vwap = (data["close"] * data["volume"]).cumsum() / data["volume"].cumsum().replace(0, np.nan)
    return (intraday_move - last_move).fillna(0) + (data["close"] - vwap)


def _adaptive_momentum(data: "pd.DataFrame") -> "pd.Series":
    returns = data["close"].pct_change().fillna(0)
    volatility = returns.rolling(20).std(ddof=0)
    adaptive_weight = np.exp(-volatility)
    return returns.rolling(10).mean() * adaptive_weight


def _composite_alpha(data: "pd.DataFrame") -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")
    components = [
        _macd_enhanced(data),
        _rsi_enhanced(data),
        _atr_enhanced(data),
        _adaptive_momentum(data),
    ]
    stacked = pd.concat(components, axis=1)
    return stacked.mean(axis=1)


register_factor("macd_enhanced", "enhanced", _macd_enhanced)
register_factor("rsi_enhanced", "enhanced", _rsi_enhanced)
register_factor("atr_enhanced", "enhanced", _atr_enhanced)
register_factor("smart_money_flow", "enhanced", _smart_money_flow)
register_factor("composite_alpha", "enhanced", _composite_alpha)
register_factor("adaptive_momentum", "enhanced", _adaptive_momentum)
