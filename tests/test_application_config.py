from argparse import Namespace
from pathlib import Path

from application.configuration import AppSettings


def test_app_settings_from_cli_args(tmp_path, monkeypatch):
    args = Namespace(
        symbol="0700.HK",
        phase="both",
        reset=True,
        data_root=str(tmp_path),
        db_path=str(tmp_path / "db.sqlite"),
        log_level="DEBUG",
        combiner_top_n=15,
        combiner_max_factors=5,
        combiner_min_sharpe=1.1,
        combiner_min_ic=0.07,
    )
    settings = AppSettings.from_cli_args(args)
    assert settings.symbol == "0700.HK"
    assert settings.db_path == Path(tmp_path / "db.sqlite")
    assert settings.log_level == "DEBUG"
    assert settings.data_root == tmp_path
    assert settings.combiner.top_n == 15
    assert settings.combiner.max_factors == 5
    assert settings.combiner.min_sharpe == 1.1
    assert settings.combiner.min_information_coefficient == 0.07


def test_app_settings_env_fallback(monkeypatch):
    monkeypatch.setenv("HK_DISCOVERY_DB", "/tmp/db.sqlite")
    monkeypatch.setenv("HK_DISCOVERY_COMBINER_TOP_N", "12")
    monkeypatch.setenv("HK_DISCOVERY_COMBINER_MAX_FACTORS", "4")
    monkeypatch.setenv("HK_DISCOVERY_COMBINER_MIN_SHARPE", "0.9")
    monkeypatch.setenv("HK_DISCOVERY_COMBINER_MIN_IC", "0.03")
    args = Namespace(
        symbol="0700.HK",
        phase="phase1",
        reset=False,
        data_root=None,
        db_path=None,
        log_level="INFO",
        combiner_top_n=None,
        combiner_max_factors=None,
        combiner_min_sharpe=None,
        combiner_min_ic=None,
    )
    settings = AppSettings.from_cli_args(args)
    assert settings.db_path == Path("/tmp/db.sqlite")
    assert settings.combiner.top_n == 12
    assert settings.combiner.max_factors == 4
    assert settings.combiner.min_sharpe == 0.9
    assert settings.combiner.min_information_coefficient == 0.03


def test_app_settings_cli_overrides_combiner_env(monkeypatch, tmp_path):
    monkeypatch.setenv("HK_DISCOVERY_COMBINER_TOP_N", "9")
    monkeypatch.setenv("HK_DISCOVERY_COMBINER_MAX_FACTORS", "3")
    monkeypatch.setenv("HK_DISCOVERY_COMBINER_MIN_SHARPE", "0.5")
    monkeypatch.setenv("HK_DISCOVERY_COMBINER_MIN_IC", "0.02")

    args = Namespace(
        symbol="0700.HK",
        phase="phase2",
        reset=False,
        data_root=str(tmp_path / "data"),
        db_path=str(tmp_path / "db.sqlite"),
        log_level="WARNING",
        combiner_top_n=25,
        combiner_max_factors=6,
        combiner_min_sharpe=1.2,
        combiner_min_ic=0.11,
    )

    settings = AppSettings.from_cli_args(args)

    assert settings.combiner.top_n == 25
    assert settings.combiner.max_factors == 6
    assert settings.combiner.min_sharpe == 1.2
    assert settings.combiner.min_information_coefficient == 0.11
