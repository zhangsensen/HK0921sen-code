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
    return ema(data["close"], 12) - ema(data["close"], 26)


def _rsi_enhanced(data: "pd.DataFrame") -> "pd.Series":
    delta = data["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(span=14, adjust=False).mean()
    avg_loss = loss.ewm(span=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _atr_enhanced(data: "pd.DataFrame") -> "pd.Series":
    return atr(data["high"], data["low"], data["close"], 14)


def _smart_money_flow(data: "pd.DataFrame") -> "pd.Series":
    typical_price = (data["high"] + data["low"] + data["close"]) / 3
    money_flow = typical_price * data["volume"]
    delta = typical_price.diff()
    positive_flow = money_flow.where(delta > 0, 0.0)
    negative_flow = money_flow.where(delta < 0, 0.0)
    positive = positive_flow.rolling(14).sum()
    negative = -negative_flow.rolling(14).sum()
    ratio = positive / negative.replace(0, np.nan)
    return 100 - (100 / (1 + ratio))


def _adaptive_momentum(data: "pd.DataFrame") -> "pd.Series":
    return data["close"].pct_change(fill_method=None).rolling(10).mean()


def _composite_alpha(data: "pd.DataFrame") -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")
    macd = ema(data["close"], 12) - ema(data["close"], 26)
    rsi = _rsi_enhanced(data)
    atr_val = atr(data["high"], data["low"], data["close"], 14)
    momentum = data["close"].pct_change(fill_method=None).rolling(10).mean()
    components = [macd, rsi, atr_val, momentum]
    stacked = pd.concat(components, axis=1)
    return stacked.mean(axis=1)


register_factor("macd_enhanced", "enhanced", _macd_enhanced)
register_factor("rsi_enhanced", "enhanced", _rsi_enhanced)
register_factor("atr_enhanced", "enhanced", _atr_enhanced)
register_factor("smart_money_flow", "enhanced", _smart_money_flow)
register_factor("composite_alpha", "enhanced", _composite_alpha)
register_factor("adaptive_momentum", "enhanced", _adaptive_momentum)
