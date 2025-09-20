"""Microstructure related factors."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from .base_factor import register_factor
from .common import atr


register_factor(
    "hl_spread",
    "microstructure",
    lambda data: (data["high"] - data["low"]) / data["close"].replace(0, np.nan),
)
register_factor(
    "co_spread",
    "microstructure",
    lambda data: (data["close"] - data["open"]) / data["open"].replace(0, np.nan),
)
register_factor(
    "price_efficiency",
    "microstructure",
    lambda data: data["close"].diff(20).abs()
    / data["close"].diff().abs().rolling(20).sum().replace(0, np.nan),
)
register_factor(
    "volume_intensity",
    "microstructure",
    lambda data: data["volume"] / data["volume"].rolling(20).mean().replace(0, np.nan),
)
register_factor(
    "tick_imbalance",
    "microstructure",
    lambda data: data["close"].pct_change().apply(np.sign).rolling(10).mean(),
)
register_factor(
    "spread_dynamics",
    "microstructure",
    lambda data: atr(data["high"], data["low"], data["close"], 14)
    / (data["close"].rolling(14).mean()).replace(0, np.nan),
)
register_factor(
    "liquidity_ratio",
    "microstructure",
    lambda data: data["volume"] / (data["high"] - data["low"]).replace(0, np.nan),
)
