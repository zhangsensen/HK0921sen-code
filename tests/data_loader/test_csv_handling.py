import pytest

pd = pytest.importorskip("pandas")
from data_loader import HistoricalDataLoader


def test_read_csv_filters_invalid_rows(tmp_path):
    csv_dir = tmp_path / "raw_data" / "1m"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / "0700.HK.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2024-01-01 09:30:00,100,101,99.5,100.2,1000\n"
        "not-a-timestamp,101,102,100,101,900\n"
    )

    loader = HistoricalDataLoader(data_root=tmp_path)
    result = loader.load("0700.HK", "1m")

    assert len(result) == 1
    assert result.index[0] == pd.Timestamp("2024-01-01 09:30:00")


def test_read_csv_raises_clear_error_for_corrupted_file(tmp_path, monkeypatch):
    csv_dir = tmp_path / "raw_data" / "1m"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / "0700.HK.csv"
    csv_path.write_text("garbage")

    loader = HistoricalDataLoader(data_root=tmp_path)

    def broken_read_csv(path, *_, **__):
        raise pd.errors.ParserError("bad csv")

    monkeypatch.setattr(pd, "read_csv", broken_read_csv)

    with pytest.raises(ValueError, match="Unable to parse CSV"):
        loader.load("0700.HK", "1m")
