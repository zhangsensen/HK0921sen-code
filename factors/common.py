"""Shared utilities for factor calculations."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None


def ensure_series(series: "pd.Series") -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")
    return series.astype(float)


def sma(series: "pd.Series", window: int) -> "pd.Series":
    return ensure_series(series).rolling(window=window, min_periods=window).mean()


def ema(series: "pd.Series", span: int) -> "pd.Series":
    return ensure_series(series).ewm(span=span, adjust=False).mean()


def true_range(high: "pd.Series", low: "pd.Series", close: "pd.Series") -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")
    prev_close = close.shift(1)
    ranges = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    return pd.Series(ranges, index=close.index, dtype=float)


def atr(high: "pd.Series", low: "pd.Series", close: "pd.Series", period: int) -> "pd.Series":
    tr = true_range(high, low, close)
    return tr.rolling(window=period, min_periods=period).mean()


def percentile_rank(series: "pd.Series", window: int) -> "pd.Series":
    if pd is None:
        raise ModuleNotFoundError("pandas is required for factor computation")

    def _rank(values: "pd.Series") -> float:
        arr = values.to_numpy()
        order = arr.argsort()
        rank = order.argsort()[-1]
        return rank / (len(arr) - 1) if len(arr) > 1 else 0.5

    return ensure_series(series).rolling(window).apply(_rank, raw=False)


def zscore(series: "pd.Series", window: int) -> "pd.Series":
    rolling = ensure_series(series).rolling(window)
    mean = rolling.mean()
    std = rolling.std(ddof=0)
    return (series - mean) / std.replace(0, np.nan)


def rate_of_change(series: "pd.Series", period: int) -> "pd.Series":
    return ensure_series(series).pct_change(periods=period, fill_method=None)
