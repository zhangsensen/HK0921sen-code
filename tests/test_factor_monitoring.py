"""Tests for factor-focused monitoring helpers."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from scripts import factor_metrics
from utils.monitoring.config import (
    FactorAlertDefinition,
    FactorMetricTemplate,
    MonitorConfig,
)
from utils.monitoring.models import AlertSeverity, MetricCategory, MetricData, MetricType
from utils.monitoring.runtime import (
    PerformanceMonitor,
    get_performance_monitor,
    measure_operation_performance,
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


def test_measure_operation_context_manager_records_metrics(tmp_path):
    stop_global_monitoring()
    config = MonitorConfig(
        enabled=False,
        enable_system_metrics=False,
        enable_alerting=False,
        log_dir=str(tmp_path / "logs"),
        database_path=str(tmp_path / "monitor.db"),
    )
    monitor = get_performance_monitor(config)

    operation_name = "unit_test_operation"
    with measure_operation_performance(operation_name, tags={"scope": "test"}):
        pass

    stats = monitor.get_operation_statistics()
    assert stats[operation_name]["count"] == 1

    operation_metrics = monitor.get_metrics(categories=[MetricCategory.OPERATION])
    recorded_names = {metric.name for metric in operation_metrics}
    assert f"{operation_name}_duration" in recorded_names
    assert f"{operation_name}_memory_delta" in recorded_names

    stop_global_monitoring()


def test_export_metrics_to_file_includes_recorded_payload(tmp_path):
    config = MonitorConfig(
        enabled=False,
        enable_system_metrics=False,
        enable_alerting=False,
        log_dir=str(tmp_path / "logs"),
        database_path=str(tmp_path / "monitor.db"),
    )
    monitor = PerformanceMonitor(config)

    metric = MetricData(
        name="custom.metric",
        value=1.23,
        type=MetricType.GAUGE,
        category=MetricCategory.CUSTOM,
        timestamp=datetime.now(timezone.utc),
        tags={"source": "test"},
    )
    monitor._save_metric_to_storage(metric)

    output = tmp_path / "metrics.json"
    assert monitor.export_metrics_to_file(str(output), compress=False) is True

    payload = json.loads(output.read_text())
    assert payload[0]["name"] == "custom.metric"

    monitor.stop()


def test_monitoring_package_re_exports_public_api():
    import utils.monitoring as monitoring

    assert monitoring.PerformanceMonitor is PerformanceMonitor
    assert monitoring.MonitorConfig is MonitorConfig
    assert monitoring.MetricCategory is MetricCategory
