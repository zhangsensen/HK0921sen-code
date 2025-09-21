import pytest


@pytest.fixture
def sample_frame():
    pd = pytest.importorskip("pandas")
    index = pd.date_range("2024-01-01 09:30", periods=3, freq="1min")
    return pd.DataFrame(
        {
            "open": [100.0, 100.5, 101.0],
            "high": [101.0, 101.5, 102.0],
            "low": [99.5, 100.0, 100.5],
            "close": [100.2, 100.7, 101.1],
            "volume": [1_000, 1_100, 1_050],
        },
        index=index,
    )
