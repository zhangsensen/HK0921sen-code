from argparse import Namespace
from pathlib import Path

from hk_factor_discovery.application.configuration import AppSettings


def test_app_settings_from_cli_args(tmp_path, monkeypatch):
    args = Namespace(
        symbol="0700.HK",
        phase="both",
        reset=True,
        data_root=str(tmp_path),
        db_path=str(tmp_path / "db.sqlite"),
        log_level="DEBUG",
    )
    settings = AppSettings.from_cli_args(args)
    assert settings.symbol == "0700.HK"
    assert settings.db_path == Path(tmp_path / "db.sqlite")
    assert settings.log_level == "DEBUG"
    assert settings.data_root == tmp_path


def test_app_settings_env_fallback(monkeypatch):
    monkeypatch.setenv("HK_DISCOVERY_DB", "/tmp/db.sqlite")
    args = Namespace(symbol="0700.HK", phase="phase1", reset=False, data_root=None, db_path=None, log_level="INFO")
    settings = AppSettings.from_cli_args(args)
    assert settings.db_path == Path("/tmp/db.sqlite")
