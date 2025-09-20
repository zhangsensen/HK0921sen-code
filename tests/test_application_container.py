import pytest

pd = pytest.importorskip("pandas")

from hk_factor_discovery.application.configuration import AppSettings
from hk_factor_discovery.application.container import ServiceContainer
from hk_factor_discovery.data_loader import OptimizedDataLoader


def test_container_provides_singletons(tmp_path):
    settings = AppSettings(
        symbol="0700.HK",
        phase="both",
        reset=False,
        data_root=tmp_path,
        db_path=tmp_path / "db.sqlite",
    )
    container = ServiceContainer(settings)
    loader_one = container.data_loader()
    loader_two = container.data_loader()
    assert isinstance(loader_one, OptimizedDataLoader)
    assert loader_one is loader_two
    db_one = container.database()
    db_two = container.database()
    assert db_one is db_two
