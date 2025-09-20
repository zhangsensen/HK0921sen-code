"""Tests for factor-focused monitoring helpers."""
from __future__ import annotations

from scripts import factor_metrics
from utils.monitoring import (
    AlertSeverity,
    FactorAlertDefinition,
    FactorMetricTemplate,
    MetricCategory,
    MonitorConfig,
    PerformanceMonitor,
    get_performance_monitor,
    stop_global_monitoring,
)


def _monitor_config(tmp_path):
    log_dir = tmp_path / "logs"
    db_path = tmp_path / "monitor.db"
    return MonitorConfig(
        enabled=False,
        log_dir=str(log_dir),
        database_path=str(db_path),
        factor_metrics=[
            FactorMetricTemplate(
                name="sharpe_ratio",
                unit=None,
                default_tags={"horizon": "daily"},
                metadata={"template": "baseline"},
            ),
            FactorMetricTemplate(name="max_drawdown"),
        ],
    )


def test_record_factor_metrics_adds_tags_and_category(tmp_path):
    config = _monitor_config(tmp_path)
    monitor = PerformanceMonitor(config)

    monitor.record_factor_metrics(
        "alpha",
        {"sharpe_ratio": 1.25, "max_drawdown": 0.18},
        extra_tags={"timeframe": "1d"},
        metadata={"window": "2023-01"},
    )

    sharpe_series = monitor.metrics_history["factor.sharpe_ratio"]
    latest = sharpe_series[-1]
    assert latest.category == MetricCategory.FACTOR_COMPUTATION
    assert latest.tags["factor_name"] == "alpha"
    assert latest.tags["timeframe"] == "1d"
    assert latest.tags["horizon"] == "daily"
    assert latest.metadata == {"template": "baseline", "window": "2023-01"}

    monitor.stop()


def test_factor_alerts_trigger_and_resolve(tmp_path):
    config = MonitorConfig(
        enabled=False,
        log_dir=str(tmp_path / "logs"),
        database_path=str(tmp_path / "monitor.db"),
        factor_metrics=[FactorMetricTemplate(name="sharpe_ratio")],
        factor_alerts=[
            FactorAlertDefinition(
                name="low_sharpe",
                metric="sharpe_ratio",
                condition="<",
                threshold=0.5,
                severity=AlertSeverity.WARNING,
                message_template="Sharpe below 0.5 for {factor_name}: {value:.2f}",
            )
        ],
    )
    monitor = PerformanceMonitor(config)

    monitor.record_factor_metrics("momentum", {"sharpe_ratio": 0.4})
    assert monitor.active_alerts, "Expected alert to trigger for low sharpe ratio"
    active_alert = next(iter(monitor.active_alerts.values()))
    assert active_alert.rule_name == "low_sharpe"
    assert "momentum" in active_alert.message
    assert active_alert.tags == {"factor_name": "momentum"}

    monitor.record_factor_metrics("momentum", {"sharpe_ratio": 0.8})
    assert not monitor.active_alerts, "Alert should resolve once metric recovers"

    monitor.stop()


def test_factor_metrics_cli_reports_grouped_values(tmp_path, capsys):
    stop_global_monitoring()
    config = MonitorConfig(
        enabled=False,
        log_dir=str(tmp_path / "logs"),
        database_path=str(tmp_path / "monitor.db"),
        factor_metrics=[
            FactorMetricTemplate(name="sharpe_ratio"),
            FactorMetricTemplate(name="max_drawdown"),
        ],
    )
    monitor = get_performance_monitor(config)

    monitor.record_factor_metrics("alpha", {"sharpe_ratio": 1.2, "max_drawdown": 0.1})
    monitor.record_factor_metrics("beta", {"sharpe_ratio": 0.9})

    exit_code = factor_metrics.main(["--hours", "1", "--metric", "sharpe_ratio", "--top", "1"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "alpha" in captured.out
    assert "Leaderboard" in captured.out

    stop_global_monitoring()
