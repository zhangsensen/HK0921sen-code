"""Historical data loader for HK stocks with direct timeframe files."""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable, Optional, Tuple

try:  # pragma: no cover - optional dependency guard
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled via runtime error
    pd = None

from config import DEFAULT_TIMEFRAMES
from utils.cache import CacheBackend, InMemoryCache

DataProvider = Callable[[str, str], "pd.DataFrame"]


class HistoricalDataLoader:
    """Load OHLCV data for a symbol/timeframe directly from disk or provider."""

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

        if timeframe not in DEFAULT_TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        cache_key = (symbol, timeframe)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        df = self._load_raw(symbol, timeframe)

        df = df.sort_index().dropna(how="all")
        self.cache.set(cache_key, df, ttl=self.cache_ttl)
        return df

    def available_timeframes(self) -> Tuple[str, ...]:
        """Return supported timeframes in ascending order."""

        return tuple(DEFAULT_TIMEFRAMES)

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
        self._ensure_safe_component(symbol, "symbol")
        self._ensure_safe_component(timeframe, "timeframe")

        if self.data_provider is not None:
            df = self.data_provider(symbol, timeframe)
            if df is None:
                raise FileNotFoundError(f"Data provider returned None for {symbol} {timeframe}")
            return df

        if self.data_root is None:
            raise FileNotFoundError("No data root provided for raw data loading")

        errors: list[str] = []
        # 支持多种文件命名格式
        symbol_clean = symbol.replace('.', '')  # 0700.HK -> 0700HK
        search_locations = [
            (self.data_root / "raw_data" / timeframe, symbol),
            (self.data_root / "raw_data" / symbol, timeframe),
            (self.data_root / timeframe, symbol),
            (self.data_root / symbol, timeframe),
            # 新增支持: 0700HK_1d.parquet 格式
            (self.data_root / "raw_data" / timeframe, f"{symbol_clean}_{timeframe}"),
        ]

        for extension in (".parquet", ".csv"):
            for base_dir, file_stem in search_locations:
                file_path = base_dir / f"{file_stem}{extension}"
                if not file_path.exists():
                    continue
                if extension == ".parquet":
                    try:
                        df = pd.read_parquet(file_path)
                        # 确保parquet文件也经过时间戳处理
                        return self._process_dataframe(df)
                    except Exception as exc:  # pragma: no cover - surfaced via fallback logic
                        errors.append(f"{file_path}: {exc}")
                        continue
                try:
                    return self._read_csv(file_path)
                except ValueError as exc:
                    errors.append(f"{file_path}: {exc}")
                    continue

        if errors:
            raise ValueError(
                "Failed to load data for "
                f"{symbol} {timeframe}. Encountered: " + "; ".join(errors)
            )

        raise FileNotFoundError(
            f"Missing data file for {symbol} {timeframe}. Expected e.g. data/raw_data/{timeframe}/{symbol}.parquet"
        )

    def _process_dataframe(self, dataframe: "pd.DataFrame") -> "pd.DataFrame":
        """处理数据框，设置时间戳索引并标准化列名"""
        if dataframe.empty:
            return dataframe

        dataframe = dataframe.rename(columns={col: col.lower() for col in dataframe.columns})

        # 查找时间戳列
        datetime_column = None
        for candidate in ("timestamp", "datetime", "date"):
            if candidate in dataframe.columns:
                datetime_column = candidate
                break
        if datetime_column is None and len(dataframe.columns) > 0:
            datetime_column = dataframe.columns[0]

        # 设置时间戳索引
        if datetime_column and datetime_column in dataframe.columns:
            timestamps = pd.to_datetime(dataframe[datetime_column], errors="coerce")
            valid_mask = timestamps.notna()
            dataframe = dataframe.loc[valid_mask].copy()
            timestamps = timestamps[valid_mask]
            dataframe = dataframe.drop(columns=[datetime_column], errors="ignore")
            dataframe.index = timestamps
            dataframe.index.name = None

        # 确保数值列为数值类型
        for column in ("open", "high", "low", "close", "volume"):
            if column in dataframe.columns:
                dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")

        return dataframe

    def _read_csv(self, file_path: Path) -> "pd.DataFrame":
        try:
            dataframe = pd.read_csv(file_path)
        except Exception as exc:  # pragma: no cover - surfaced in tests
            raise ValueError(f"Unable to parse CSV file {file_path}: {exc}") from exc
        
        return self._process_dataframe(dataframe)

    @staticmethod
    def _ensure_safe_component(value: str, kind: str) -> None:
        if not value:
            raise ValueError(f"{kind.capitalize()} cannot be empty")
        if value != value.strip():
            raise ValueError(f"{kind.capitalize()} contains leading/trailing whitespace: {value!r}")
        if "/" in value or "\\" in value:
            raise ValueError(f"{kind.capitalize()} contains invalid path separators: {value}")
        path = Path(value)
        if path.is_absolute() or any(part in {"..", ""} for part in path.parts):
            raise ValueError(f"{kind.capitalize()} contains invalid path components: {value}")
