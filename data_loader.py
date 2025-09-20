"""Historical data loader with smart resampling for HK stocks."""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable, Optional, Tuple

try:  # pragma: no cover - optional dependency guard
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled via runtime error
    pd = None

from config import DEFAULT_TIMEFRAMES, RAW_TIMEFRAMES, TIMEFRAME_TO_PANDAS_RULE
from utils.cache import CacheBackend, InMemoryCache

DataProvider = Callable[[str, str], "pd.DataFrame"]


class HistoricalDataLoader:
    """Load OHLCV data and resample missing timeframes on demand."""

    def __init__(
        self,
        data_root: Path | str | None = None,
        data_provider: Optional[DataProvider] = None,
        cache_backend: Optional[CacheBackend] = None,
        cache_ttl: int = 300,
    ) -> None:
        if pd is None:
            raise ModuleNotFoundError(
                "pandas is required to use HistoricalDataLoader. "
                "Install pandas or skip data dependent features."
            )
        self.data_root = Path(data_root) if data_root is not None else None
        self.data_provider = data_provider
        self.cache = cache_backend or InMemoryCache()
        self.cache_ttl = cache_ttl

    def load(self, symbol: str, timeframe: str) -> "pd.DataFrame":
        """Return OHLCV dataframe indexed by datetime."""

        if timeframe not in TIMEFRAME_TO_PANDAS_RULE:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        cache_key = (symbol, timeframe)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        if timeframe in RAW_TIMEFRAMES:
            df = self._load_raw(symbol, timeframe)
        else:
            df = self._resample_from_base(symbol, timeframe)

        df = df.sort_index().dropna(how="all")
        self.cache.set(cache_key, df, ttl=self.cache_ttl)
        return df

    def available_timeframes(self) -> Tuple[str, ...]:
        """Return supported timeframes in ascending order."""

        return tuple(sorted(TIMEFRAME_TO_PANDAS_RULE, key=lambda tf: DEFAULT_TIMEFRAMES.index(tf)))

    def clear_cache(self) -> None:
        self.cache.clear()

    def stream(self, symbol: str, timeframe: str, batch_size: int = 10_000) -> Iterable["pd.DataFrame"]:
        """Yield dataframe batches to avoid loading everything into memory."""

        data = self.load(symbol, timeframe)
        if batch_size <= 0 or len(data) <= batch_size:
            yield data
            return

        for start in range(0, len(data), batch_size):
            yield data.iloc[start : start + batch_size]

    # ------------------------------------------------------------------
    def _load_raw(self, symbol: str, timeframe: str) -> "pd.DataFrame":
        if self.data_provider is not None:
            df = self.data_provider(symbol, timeframe)
            if df is None:
                raise FileNotFoundError(f"Data provider returned None for {symbol} {timeframe}")
            return df

        if self.data_root is None:
            raise FileNotFoundError("No data root provided for raw data loading")

        # Check in raw_data subdirectory first
        timeframe_first = self.data_root / "raw_data" / timeframe / f"{symbol}.parquet"
        symbol_first = self.data_root / "raw_data" / symbol / f"{timeframe}.parquet"

        # If not found in raw_data, try legacy locations
        if not timeframe_first.exists() and not symbol_first.exists():
            timeframe_first_legacy = self.data_root / timeframe / f"{symbol}.parquet"
            symbol_first_legacy = self.data_root / symbol / f"{timeframe}.parquet"

            if timeframe_first_legacy.exists():
                timeframe_first = timeframe_first_legacy
            elif symbol_first_legacy.exists():
                symbol_first = symbol_first_legacy

        if timeframe_first.exists():
            file_path = timeframe_first
        elif symbol_first.exists():
            file_path = symbol_first
        else:
            raise FileNotFoundError(
                f"Missing data file for {symbol} {timeframe}. "
                "Checked both timeframe and symbol directories."
            )

        return pd.read_parquet(file_path)

    def _resample_from_base(self, symbol: str, timeframe: str) -> "pd.DataFrame":
        base_timeframe = self._select_base_timeframe(timeframe)
        base_df = self.load(symbol, base_timeframe)

        rule = TIMEFRAME_TO_PANDAS_RULE[timeframe]
        resampled = base_df.resample(rule).agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        return resampled.dropna(how="all")

    @staticmethod
    def _select_base_timeframe(timeframe: str) -> str:
        if timeframe.endswith("d"):
            return "1d"
        if timeframe.endswith("h"):
            return "1h" if timeframe in {"2h", "4h"} else "1m"
        return "1m"
