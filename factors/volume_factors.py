"""Volume-based factors."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from .base_factor import register_factor
from .common import sma


def _vwap(price: "pd.Series", volume: "pd.Series", window: int) -> "pd.Series":
    cum_price_volume = (price * volume).rolling(window).sum()
    cum_volume = volume.rolling(window).sum()
    return cum_price_volume / cum_volume.replace(0, np.nan)


def _on_balance_volume(close: "pd.Series", volume: "pd.Series") -> "pd.Series":
    direction = np.sign(close.diff().fillna(0))
    return (direction * volume).cumsum()


def _accumulation_distribution(high: "pd.Series", low: "pd.Series", close: "pd.Series", volume: "pd.Series") -> "pd.Series":
    money_flow_multiplier = ((close - low) - (high - close)) / (high - low).replace(0, np.nan)
    money_flow_volume = money_flow_multiplier * volume
    return money_flow_volume.cumsum()


def _chaikin_money_flow(high: "pd.Series", low: "pd.Series", close: "pd.Series", volume: "pd.Series", period: int) -> "pd.Series":
    money_flow_multiplier = ((close - low) - (high - close)) / (high - low).replace(0, np.nan)
    money_flow_volume = money_flow_multiplier * volume
    return money_flow_volume.rolling(period).sum() / volume.rolling(period).sum().replace(0, np.nan)


def _force_index(close: "pd.Series", volume: "pd.Series", period: int) -> "pd.Series":
    return (close.diff() * volume).ewm(span=period, adjust=False).mean()


def _volume_price_trend(close: "pd.Series", volume: "pd.Series") -> "pd.Series":
    pct_change = close.pct_change(fill_method=None).fillna(0)
    return (pct_change * volume).cumsum()


register_factor("volume_sma", "volume", lambda data: sma(data["volume"], 20))
register_factor(
    "volume_ratio",
    "volume",
    lambda data: data["volume"] / data["volume"].rolling(10).mean().replace(0, np.nan),
)
register_factor(
    "vwap_deviation",
    "volume",
    lambda data: (data["close"] - _vwap((data["high"] + data["low"] + data["close"]) / 3, data["volume"], 20))
    / data["close"],
)
register_factor("obv", "volume", lambda data: _on_balance_volume(data["close"], data["volume"]))
register_factor(
    "ad_line",
    "volume",
    lambda data: _accumulation_distribution(data["high"], data["low"], data["close"], data["volume"]),
)
register_factor(
    "cmf",
    "volume",
    lambda data: _chaikin_money_flow(data["high"], data["low"], data["close"], data["volume"], 20),
)
register_factor("fi", "volume", lambda data: _force_index(data["close"], data["volume"], 13))
register_factor("vpt", "volume", lambda data: _volume_price_trend(data["close"], data["volume"]))
