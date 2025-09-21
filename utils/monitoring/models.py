"""Data models used by the monitoring subsystem."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class MetricCategory(Enum):
    """Classification for captured metrics."""

    SYSTEM = "system"
    MEMORY = "memory"
    CPU = "cpu"
    DISK = "disk"
    NETWORK = "network"
    OPERATION = "operation"
    FACTOR_COMPUTATION = "factor_computation"
    BACKTEST = "backtest"
    DATA_LOADING = "data_loading"
    CUSTOM = "custom"


class MetricType(Enum):
    """Type of metric that was captured."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


class AlertSeverity(Enum):
    """Severity for alert rules and events."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class MetricData:
    """Represents a single metric observation."""

    name: str
    value: float
    type: MetricType
    category: MetricCategory
    timestamp: datetime
    tags: Dict[str, str]
    unit: Optional[str] = None
    session_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class PerformanceSnapshot:
    """Summary of system utilisation for a collection interval."""

    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    memory_total_mb: float
    disk_usage_percent: float
    network_sent_mb: float
    network_recv_mb: float
    thread_count: int
    process_count: int


@dataclass
class AlertRule:
    """Rule describing when a metric triggers an alert."""

    name: str
    metric_name: str
    condition: str
    threshold: float
    severity: AlertSeverity
    message_template: str
    enabled: bool = True
    cooldown_minutes: int = 5


@dataclass
class Alert:
    """Alert instance raised by the monitor."""

    id: str
    rule_name: str
    severity: AlertSeverity
    message: str
    metric_value: float
    timestamp: datetime
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    tags: Optional[Dict[str, str]] = None
