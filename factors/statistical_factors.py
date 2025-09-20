"""Statistical analysis factors."""
from __future__ import annotations

import numpy as np

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from .base_factor import register_factor
from .common import percentile_rank, zscore


register_factor("zscore_20", "statistical", lambda data: zscore(data["close"], 20))
register_factor("zscore_50", "statistical", lambda data: zscore(data["close"], 50))
register_factor("skewness_20", "statistical", lambda data: data["close"].pct_change().rolling(20).skew())
register_factor("kurtosis_20", "statistical", lambda data: data["close"].pct_change().rolling(20).kurt())
register_factor("percentile_20", "statistical", lambda data: percentile_rank(data["close"], 20))
register_factor("percentile_50", "statistical", lambda data: percentile_rank(data["close"], 50))
register_factor(
    "correlation_10",
    "statistical",
    lambda data: data["close"].pct_change().rolling(10).corr(data["volume"].pct_change()),
)
register_factor(
    "beta_20",
    "statistical",
    lambda data: (
        data["close"].pct_change().rolling(20).cov(data["close"].pct_change().shift(1))
        / data["close"].pct_change().rolling(20).var().replace(0, np.nan)
    ),
)
