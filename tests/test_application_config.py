from pathlib import Path

from application.configuration import AppSettings
from application.container import ServiceContainer
from config import CombinerConfig
from main import _build_parser
from utils.monitoring.config import MonitorConfig


def test_app_settings_from_cli_args(tmp_path):
    parser = _build_parser()
    args = parser.parse_args(
        [
            "--symbol",
            "0700.HK",
            "--phase",
            "both",
            "--reset",
            "--data-root",
            str(tmp_path),
            "--db-path",
            str(tmp_path / "db.sqlite"),
            "--log-level",
            "DEBUG",
            "--combiner-top-n",
            "15",
            "--combiner-max-factors",
            "5",
            "--combiner-min-sharpe",
            "1.1",
            "--combiner-min-ic",
            "0.07",
        ]
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
    parser = _build_parser()
    args = parser.parse_args(["--symbol", "0700.HK", "--phase", "phase1"])
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
    defaults = CombinerConfig()
    parser = _build_parser()
    base_cli = [
        "--symbol",
        "0700.HK",
        "--phase",
        "phase2",
        "--data-root",
        str(tmp_path / "data"),
        "--db-path",
        str(tmp_path / "db.sqlite"),
        "--log-level",
        "WARNING",
    ]

    args_default = parser.parse_args(
        base_cli
        + [
            "--combiner-top-n",
            str(defaults.top_n),
            "--combiner-max-factors",
            str(defaults.max_factors),
            "--combiner-min-sharpe",
            str(defaults.min_sharpe),
            "--combiner-min-ic",
            str(defaults.min_information_coefficient),
        ]
    )

    settings_default = AppSettings.from_cli_args(args_default)

    assert settings_default.combiner.top_n == defaults.top_n
    assert settings_default.combiner.max_factors == defaults.max_factors
    assert settings_default.combiner.min_sharpe == defaults.min_sharpe
    assert (
        settings_default.combiner.min_information_coefficient
        == defaults.min_information_coefficient
    )

    args_custom = parser.parse_args(
        base_cli
        + [
            "--combiner-top-n",
            "25",
            "--combiner-max-factors",
            "6",
            "--combiner-min-sharpe",
            "1.2",
            "--combiner-min-ic",
            "0.11",
        ]
    )

    settings_custom = AppSettings.from_cli_args(args_custom)

    assert settings_custom.combiner.top_n == 25
    assert settings_custom.combiner.max_factors == 6
    assert settings_custom.combiner.min_sharpe == 1.2
    assert settings_custom.combiner.min_information_coefficient == 0.11


def test_app_settings_monitoring_cli_overrides_env(monkeypatch, tmp_path):
    monkeypatch.setenv("HK_DISCOVERY_MONITORING_ENABLED", "0")
    monkeypatch.setenv("HK_DISCOVERY_MONITOR_LOG_DIR", "/env/logs")
    monkeypatch.setenv("HK_DISCOVERY_MONITOR_DB_PATH", "/env/monitor.db")

    parser = _build_parser()
    args = parser.parse_args(
        [
            "--symbol",
            "0700.HK",
            "--phase",
            "both",
            "--enable-monitoring",
            "--monitor-log-dir",
            str(tmp_path / "cli-logs"),
            "--monitor-db-path",
            str(tmp_path / "cli-monitor.db"),
        ]
    )

    settings = AppSettings.from_cli_args(args)
    assert settings.monitoring is not None
    assert settings.monitoring.enabled is True
    assert settings.monitoring.log_dir == str(tmp_path / "cli-logs")
    assert settings.monitoring.database_path == str(tmp_path / "cli-monitor.db")

    container = ServiceContainer(settings)
    monitor_instance = container.performance_monitor()
    monitor_again = container.performance_monitor()
    assert monitor_instance is monitor_again
    assert monitor_instance is not None
    assert monitor_instance.config.log_dir == settings.monitoring.log_dir


def test_app_settings_monitoring_env_defaults_and_disabled(monkeypatch):
    monkeypatch.delenv("HK_DISCOVERY_MONITORING_ENABLED", raising=False)
    monkeypatch.delenv("HK_DISCOVERY_MONITOR_LOG_DIR", raising=False)
    monkeypatch.delenv("HK_DISCOVERY_MONITOR_DB_PATH", raising=False)

    parser = _build_parser()
    args = parser.parse_args(["--symbol", "0700.HK", "--phase", "phase1"])
    settings = AppSettings.from_cli_args(args)
    assert settings.monitoring is None
    container = ServiceContainer(settings)
    assert container.performance_monitor() is None

    monkeypatch.setenv("HK_DISCOVERY_MONITORING_ENABLED", "true")

    parser_env = _build_parser()
    args_env = parser_env.parse_args(["--symbol", "0700.HK", "--phase", "phase2"])
    settings_env = AppSettings.from_cli_args(args_env)
    assert settings_env.monitoring is not None
    assert settings_env.monitoring.enabled is True
    assert settings_env.monitoring.log_dir == "logs/performance"
    assert settings_env.monitoring.database_path == "monitoring/performance.db"


def test_service_container_manual_monitoring_singleton(tmp_path):
    monitoring_config = MonitorConfig(
        enabled=True,
        log_dir=str(tmp_path / "manual-logs"),
        database_path=str(tmp_path / "manual.db"),
    )
    manual_settings = AppSettings(
        symbol="0700.HK",
        phase="both",
        reset=False,
        data_root=None,
        db_path=tmp_path / "db.sqlite",
        monitoring=monitoring_config,
    )

    container = ServiceContainer(manual_settings)
    monitor = container.performance_monitor()
    monitor_second = container.performance_monitor()
    assert monitor is monitor_second
    assert monitor is not None
    assert monitor.config.database_path == monitoring_config.database_path
