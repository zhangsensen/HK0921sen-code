import pandas as pd
import pytest
from pathlib import Path

from resampling.core.data_output.data_writer import ParquetDataWriter


def test_parquet_writer_persists_timestamp_column(tmp_path):
    writer = ParquetDataWriter()
    index = pd.date_range("2025-01-01", periods=3, freq="1h")
    frame = pd.DataFrame({
        "open": [1.0, 2.0, 3.0],
        "close": [1.5, 2.5, 3.5],
    }, index=index)
    output = tmp_path / "sample.parquet"

    writer.write_data(frame, "0700.HK", "1h", output)

    loaded = pd.read_parquet(output)
    assert "timestamp" in loaded.columns
    assert pd.api.types.is_datetime64_any_dtype(loaded["timestamp"])
    expected = pd.Series(index, name="timestamp")
    pd.testing.assert_series_equal(loaded["timestamp"], expected, check_names=False)


def test_parquet_writer_requires_timestamp(tmp_path):
    writer = ParquetDataWriter()
    frame = pd.DataFrame({"value": [1, 2, 3]})
    output = tmp_path / "sample.parquet"

    with pytest.raises(ValueError):
        writer.write_data(frame, "0700.HK", "1h", output)
