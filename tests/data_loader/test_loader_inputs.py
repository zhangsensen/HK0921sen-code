import pytest

pd = pytest.importorskip("pandas")
pd_testing = pytest.importorskip("pandas.testing")

from data_loader import HistoricalDataLoader


def test_load_rejects_path_traversal_symbol(tmp_path):
    loader = HistoricalDataLoader(data_root=tmp_path)

    with pytest.raises(ValueError, match="invalid path separators"):
        loader.load("../etc/passwd", "1m")


def test_load_raw_skips_unknown_extensions(tmp_path, sample_frame):
    csv_dir = tmp_path / "raw_data" / "1m"
    csv_dir.mkdir(parents=True, exist_ok=True)
    # Create an unsupported file extension to ensure we skip it.
    unsupported = csv_dir / "0700.HK.bin"
    unsupported.write_text("binary content")

    loader = HistoricalDataLoader(data_root=tmp_path)

    with pytest.raises(FileNotFoundError, match="Parquet/CSV"):
        loader.load("0700.HK", "1m")

    # Ensure the placeholder file is untouched for manual inspection/debugging.
    assert unsupported.exists()


def test_corrupted_parquet_falls_back_to_csv(tmp_path, monkeypatch, sample_frame):
    csv_dir = tmp_path / "raw_data" / "1m"
    csv_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = csv_dir / "0700.HK.parquet"
    parquet_path.touch()
    csv_path = csv_dir / "0700.HK.csv"
    sample_frame.reset_index().rename(columns={"index": "timestamp"}).to_csv(csv_path, index=False)

    def fake_read_parquet(path, *_, **__):
        raise ValueError("corrupted parquet")

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)

    loader = HistoricalDataLoader(data_root=tmp_path)
    loaded = loader.load("0700.HK", "1m")

    pd_testing.assert_frame_equal(loaded, sample_frame, check_freq=False)


def test_stream_returns_single_batch_when_smaller_than_requested(sample_frame):
    loader = HistoricalDataLoader(data_provider=lambda *_: sample_frame)

    batches = list(loader.stream("0700.HK", "1m", batch_size=len(sample_frame) * 5))
    assert len(batches) == 1
    pd_testing.assert_frame_equal(batches[0], sample_frame, check_freq=False)
