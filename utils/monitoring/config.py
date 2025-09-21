"""Configuration objects for the monitoring subsystem."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from .models import AlertSeverity


@dataclass
class FactorMetricTemplate:
    """Template describing a factor-level metric to be collected."""

    name: str
    unit: Optional[str] = None
    description: Optional[str] = None
    default_tags: Dict[str, str] = field(default_factory=dict)
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class FactorAlertDefinition:
    """Configuration for alerting on factor metrics."""

    name: str
    metric: str
    condition: str
    threshold: float
    severity: AlertSeverity
    message_template: str
    enabled: bool = True
    cooldown_minutes: int = 5


@dataclass
class MonitorConfig:
    """Runtime configuration for :class:`~utils.monitoring.runtime.PerformanceMonitor`."""

    enabled: bool = True
    collection_interval_seconds: int = 30
    history_retention_hours: int = 24
    alert_check_interval_seconds: int = 60
    max_history_size: int = 1000
    enable_system_metrics: bool = True
    enable_custom_metrics: bool = True
    enable_alerting: bool = True
    log_dir: str = "logs/performance"
    database_path: str = "monitoring/performance.db"
    export_interval_seconds: int = 300
    compression_enabled: bool = True
    alert_thresholds: Optional[Dict[str, Dict[str, float]]] = None
    factor_metrics: List[FactorMetricTemplate] = field(default_factory=list)
    factor_alerts: List[FactorAlertDefinition] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.alert_thresholds is None:
            self.alert_thresholds = {
                "cpu_percent": {"warning": 80.0, "critical": 95.0},
                "memory_percent": {"warning": 85.0, "critical": 95.0},
                "disk_usage_percent": {"warning": 90.0, "critical": 98.0},
                "operation_duration": {"warning": 10.0, "critical": 30.0},
                "error_rate": {"warning": 0.05, "critical": 0.1},
            }

        def _ensure_template(
            item: Union[FactorMetricTemplate, Dict[str, Any]]
        ) -> FactorMetricTemplate:
            if isinstance(item, FactorMetricTemplate):
                return item
            return FactorMetricTemplate(**item)

        def _ensure_alert(
            item: Union[FactorAlertDefinition, Dict[str, Any]]
        ) -> FactorAlertDefinition:
            if isinstance(item, FactorAlertDefinition):
                return item
            severity = item.get("severity")
            if isinstance(severity, str):
                try:
                    severity_enum = AlertSeverity(severity.lower())
                except ValueError as exc:  # pragma: no cover - config validation
                    raise ValueError(f"Unknown alert severity: {severity}") from exc
                item = {**item, "severity": severity_enum}
            return FactorAlertDefinition(**item)

        self.factor_metrics = [_ensure_template(template) for template in self.factor_metrics]
        self.factor_alerts = [_ensure_alert(alert) for alert in self.factor_alerts]
