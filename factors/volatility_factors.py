"""Volatility-oriented factors."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from .base_factor import register_factor
from .common import atr, ema


def _bollinger_bands(close: "pd.Series", period: int, num_std: float = 2.0):
    ma = close.rolling(period, min_periods=period).mean()
    std = close.rolling(period, min_periods=period).std(ddof=0)
    upper = ma + num_std * std
    lower = ma - num_std * std
    return ma, upper, lower


def _keltner_channels(data: "pd.DataFrame", period: int = 20, multiplier: float = 2.0):
    ma = ema(data["close"], period)
    atr_value = atr(data["high"], data["low"], data["close"], period)
    upper = ma + multiplier * atr_value
    lower = ma - multiplier * atr_value
    return ma, upper, lower


def _parkinson(high: "pd.Series", low: "pd.Series", period: int) -> "pd.Series":
    log_hl = (high / low).apply(np.log) ** 2
    return np.sqrt(log_hl.rolling(period).mean() / (4 * np.log(2)))


def _garman_klass(open_: "pd.Series", high: "pd.Series", low: "pd.Series", close: "pd.Series", period: int) -> "pd.Series":
    log_hl = (np.log(high) - np.log(low)) ** 2
    log_co = (np.log(close) - np.log(open_)) ** 2
    return np.sqrt((0.5 * log_hl - (2 * np.log(2) - 1) * log_co).rolling(period).mean())


def _rogers_satchell(open_: "pd.Series", high: "pd.Series", low: "pd.Series", close: "pd.Series", period: int) -> "pd.Series":
    term1 = np.log(high / close) * np.log(high / open_)
    term2 = np.log(low / close) * np.log(low / open_)
    return np.sqrt((term1 + term2).rolling(period).mean())


def _yang_zhang(open_: "pd.Series", high: "pd.Series", low: "pd.Series", close: "pd.Series", period: int) -> "pd.Series":
    log_return = np.log(close / close.shift(1)).fillna(0)
    log_open = np.log(open_ / close.shift(1)).fillna(0)
    log_co = np.log(close / open_).fillna(0)
    k = 0.34 / (1 + (period + 1) / (period - 1))
    close_vol = log_return.rolling(period).var()
    open_vol = log_open.rolling(period).var()
    overnight = (np.log(open_ / close.shift(1))).rolling(period).var()
    return np.sqrt(open_vol + k * close_vol + (1 - k) * overnight)


def _volatility_ratio(close: "pd.Series", short: int, long: int) -> "pd.Series":
    returns = close.pct_change(fill_method=None).fillna(0)
    short_vol = returns.rolling(short).std(ddof=0)
    long_vol = returns.rolling(long).std(ddof=0)
    return short_vol / long_vol.replace(0, np.nan)


register_factor("atr_14", "volatility", lambda data: atr(data["high"], data["low"], data["close"], 14))
register_factor(
    "atrp",
    "volatility",
    lambda data: atr(data["high"], data["low"], data["close"], 14) / data["close"],
)
register_factor(
    "bb_width",
    "volatility",
    lambda data: (
        _bollinger_bands(data["close"], 20)[1] - _bollinger_bands(data["close"], 20)[2]
    )
    / _bollinger_bands(data["close"], 20)[0],
)
register_factor(
    "bb_squeeze",
    "volatility",
    lambda data: (
        (_bollinger_bands(data["close"], 20)[1] - _bollinger_bands(data["close"], 20)[2])
        / (_keltner_channels(data, 20)[1] - _keltner_channels(data, 20)[2]).replace(0, np.nan)
    ),
)
register_factor(
    "keltner_position",
    "volatility",
    lambda data: (data["close"] - _keltner_channels(data, 20)[0])
    / (_keltner_channels(data, 20)[1] - _keltner_channels(data, 20)[2]).replace(0, np.nan),
)
register_factor(
    "volatility_ratio",
    "volatility",
    lambda data: _volatility_ratio(data["close"], 10, 30),
)
register_factor("parkinson_vol", "volatility", lambda data: _parkinson(data["high"], data["low"], 20))
register_factor(
    "garman_klass_vol",
    "volatility",
    lambda data: _garman_klass(data["open"], data["high"], data["low"], data["close"], 20),
)
register_factor(
    "rogers_satchell_vol",
    "volatility",
    lambda data: _rogers_satchell(data["open"], data["high"], data["low"], data["close"], 20),
)
register_factor(
    "yang_zhang_vol",
    "volatility",
    lambda data: _yang_zhang(data["open"], data["high"], data["low"], data["close"], 20),
)
