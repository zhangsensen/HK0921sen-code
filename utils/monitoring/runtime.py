#!/usr/bin/env python3
"""
Enhanced Performance Monitoring System for Hong Kong Factor Discovery System
提供实时性能监控、资源跟踪、操作性能分析、智能告警和历史数据分析
集成了系统资源监控、操作性能跟踪、阈值告警和历史数据管理功能
"""

import json
import os
import sqlite3
import threading
import time
import gzip
import hashlib
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

try:  # pragma: no cover - optional dependency
    import psutil
except ModuleNotFoundError:  # pragma: no cover - monitoring can run without system stats
    psutil = None  # type: ignore[assignment]

from ..enhanced_logging import EnhancedStructuredLogger, LogCategory, get_enhanced_logger
from .config import FactorAlertDefinition, FactorMetricTemplate, MonitorConfig
from .models import (
    Alert,
    AlertRule,
    AlertSeverity,
    MetricCategory,
    MetricData,
    MetricType,
    PerformanceSnapshot,
)


# 全局性能监控器实例占位
_global_monitor: Optional["PerformanceMonitor"] = None
_monitor_lock = threading.Lock()


def _make_serializable(value: Any) -> Any:
    """Convert objects (including Enums) into JSON-serializable structures."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _make_serializable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_make_serializable(item) for item in value]
    return value


def _metric_to_dict(metric: "MetricData") -> Dict[str, Any]:
    """Convert MetricData into a JSON-friendly dictionary."""

    return {
        "name": metric.name,
        "value": metric.value,
        "type": metric.type.value,
        "category": metric.category.value,
        "timestamp": metric.timestamp.isoformat(),
        "tags": _make_serializable(metric.tags),
        "unit": metric.unit,
        "session_id": metric.session_id,
        "metadata": _make_serializable(metric.metadata),
    }


class PerformanceMonitor:
    """增强性能监控系统"""

    def __init__(self, config: MonitorConfig, logger: Optional[EnhancedStructuredLogger] = None):
        self.config = config
        self.logger = logger or get_enhanced_logger()

        # 初始化存储
        self.metrics_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=config.max_history_size))
        self.snapshots: deque = deque(maxlen=config.max_history_size)
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_rules: Dict[str, AlertRule] = {}
        self.custom_metrics: Dict[str, Dict[str, Any]] = {}
        self.factor_metric_templates: Dict[str, FactorMetricTemplate] = {}

        if psutil is None and config.enable_system_metrics:
            self.logger.warning(
                LogCategory.PERFORMANCE,
                "psutil not available; system metrics collection disabled",
            )
            config.enable_system_metrics = False

        # 操作统计
        self.operation_timers: Dict[str, float] = {}
        self.operation_counts: Dict[str, int] = defaultdict(int)
        self.operation_errors: Dict[str, int] = defaultdict(int)

        # 控制变量
        self._running = False
        self._collection_thread = None
        self._alert_thread = None
        self._export_thread = None
        self._lock = threading.Lock()

        # 初始化文件系统和数据库
        self._setup_directories()
        self._init_database()

        # 注册默认告警规则
        self._register_default_alert_rules()
        if config.factor_metrics:
            self.register_factor_metric_templates(config.factor_metrics)
        if config.factor_alerts:
            self.register_factor_alerts(config.factor_alerts)

        # 启动监控
        if config.enabled:
            self.start()

    def _setup_directories(self):
        """创建监控相关目录"""
        self.log_dir = Path(self.config.log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 创建子目录
        (self.log_dir / "metrics").mkdir(exist_ok=True)
        (self.log_dir / "alerts").mkdir(exist_ok=True)
        (self.log_dir / "reports").mkdir(exist_ok=True)

        # 创建数据库目录
        db_dir = Path(self.config.database_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

    def _init_database(self):
        """初始化监控数据库"""
        conn = sqlite3.connect(self.config.database_path)
        cursor = conn.cursor()

        # 创建指标表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                category TEXT NOT NULL,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                type TEXT NOT NULL,
                unit TEXT,
                tags TEXT,
                session_id TEXT,
                metadata TEXT
            )
        ''')

        # 创建告警表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                metric_value REAL NOT NULL,
                resolved BOOLEAN DEFAULT FALSE,
                resolved_at TEXT
            )
        ''')

        # 创建性能快照表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                snapshot_data TEXT NOT NULL
            )
        ''')

        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_metrics_category ON metrics(category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_alerts_resolved ON alerts(resolved)')

        conn.commit()
        conn.close()

    def _register_default_alert_rules(self):
        """注册默认告警规则"""
        default_rules = [
            AlertRule(
                name="high_cpu_usage",
                metric_name="cpu_percent",
                condition=">",
                threshold=80.0,
                severity=AlertSeverity.WARNING,
                message_template="CPU使用率过高: {value:.1f}%"
            ),
            AlertRule(
                name="critical_cpu_usage",
                metric_name="cpu_percent",
                condition=">",
                threshold=95.0,
                severity=AlertSeverity.CRITICAL,
                message_template="CPU使用率严重过高: {value:.1f}%"
            ),
            AlertRule(
                name="high_memory_usage",
                metric_name="memory_percent",
                condition=">",
                threshold=85.0,
                severity=AlertSeverity.WARNING,
                message_template="内存使用率过高: {value:.1f}%"
            ),
            AlertRule(
                name="critical_memory_usage",
                metric_name="memory_percent",
                condition=">",
                threshold=95.0,
                severity=AlertSeverity.CRITICAL,
                message_template="内存使用率严重过高: {value:.1f}%"
            ),
            AlertRule(
                name="high_disk_usage",
                metric_name="disk_usage_percent",
                condition=">",
                threshold=90.0,
                severity=AlertSeverity.WARNING,
                message_template="磁盘使用率过高: {value:.1f}%"
            )
        ]

        for rule in default_rules:
            self.alert_rules[rule.name] = rule

    def start(self):
        """启动监控系统"""
        if self._running:
            return

        self._running = True
        self.logger.info(LogCategory.PERFORMANCE, "Enhanced performance monitoring started")

        # 启动数据收集线程
        if self.config.enable_system_metrics:
            self._collection_thread = threading.Thread(target=self._collection_loop, daemon=True)
            self._collection_thread.start()

        # 启动告警检查线程
        if self.config.enable_alerting:
            self._alert_thread = threading.Thread(target=self._alert_loop, daemon=True)
            self._alert_thread.start()

        # 启动数据导出线程
        self._export_thread = threading.Thread(target=self._export_loop, daemon=True)
        self._export_thread.start()

    def stop(self):
        """停止监控系统"""
        self._running = False
        self.logger.info(LogCategory.PERFORMANCE, "Performance monitoring stopped")

    def _collection_loop(self):
        """数据收集循环"""
        while self._running:
            try:
                snapshot = self._collect_system_metrics()
                with self._lock:
                    self.snapshots.append(snapshot)

                # 保存到数据库和文件
                self._save_snapshot_to_db(snapshot)
                self._save_snapshot(snapshot)

                # 检查是否需要清理历史数据
                self._cleanup_old_data()

            except Exception as e:
                self.logger.error(LogCategory.PERFORMANCE, "Error collecting system metrics", exception=e)

            time.sleep(self.config.collection_interval_seconds)

    def _export_loop(self):
        """数据导出循环"""
        while self._running:
            try:
                time.sleep(self.config.export_interval_seconds)
                self._export_performance_snapshot()
            except Exception as e:
                self.logger.error(LogCategory.PERFORMANCE, "Error in export loop", exception=e)
                time.sleep(60)  # 错误时等待更长时间

    def _alert_loop(self):
        """告警检查循环"""
        while self._running:
            try:
                self._check_alerts()
            except Exception as e:
                self.logger.error(LogCategory.PERFORMANCE, "Error checking alerts", exception=e)

            time.sleep(self.config.alert_check_interval_seconds)

    def _collect_system_metrics(self) -> PerformanceSnapshot:
        """收集系统指标"""
        if psutil is None:  # pragma: no cover - guarded by __init__
            raise RuntimeError("psutil is required to collect system metrics")
        process = psutil.Process()

        # 获取网络信息
        net_io = psutil.net_io_counters()

        snapshot = PerformanceSnapshot(
            timestamp=datetime.now(),
            cpu_percent=psutil.cpu_percent(interval=1),
            memory_percent=psutil.virtual_memory().percent,
            memory_used_mb=psutil.virtual_memory().used / 1024 / 1024,
            memory_total_mb=psutil.virtual_memory().total / 1024 / 1024,
            disk_usage_percent=psutil.disk_usage('/').percent,
            network_sent_mb=net_io.bytes_sent / 1024 / 1024,
            network_recv_mb=net_io.bytes_recv / 1024 / 1024,
            thread_count=process.num_threads(),
            process_count=len(psutil.pids())
        )

        return snapshot

    def _save_snapshot_to_db(self, snapshot: PerformanceSnapshot):
        """保存性能快照到数据库"""
        try:
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()

            data = asdict(snapshot)
            data['timestamp'] = snapshot.timestamp.isoformat()

            cursor.execute('''
                INSERT INTO performance_snapshots (timestamp, snapshot_data)
                VALUES (?, ?)
            ''', (snapshot.timestamp.isoformat(), json.dumps(data, ensure_ascii=False)))

            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(LogCategory.PERFORMANCE, "Error saving snapshot to database", exception=e)

    def _save_snapshot(self, snapshot: PerformanceSnapshot):
        """保存性能快照到文件"""
        filename = self.log_dir / "metrics" / f"snapshot_{snapshot.timestamp.strftime('%Y%m%d_%H%M%S')}.json"

        data = asdict(snapshot)
        data['timestamp'] = snapshot.timestamp.isoformat()

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _check_alerts(self):
        """检查告警规则"""
        if not self.snapshots:
            return

        latest_snapshot = self.snapshots[-1]

        for rule_name, rule in self.alert_rules.items():
            if not rule.enabled:
                continue

            # 获取指标值
            metric_value = getattr(latest_snapshot, rule.metric_name, None)
            if metric_value is None:
                continue

            # 检查条件
            should_alert = self._evaluate_condition(metric_value, rule.condition, rule.threshold)

            if should_alert:
                # 检查是否已经有未解决的告警
                existing_alert = self.active_alerts.get(rule_name)
                if existing_alert is None:
                    # 创建新告警
                    alert = Alert(
                        id=f"{rule_name}_{int(time.time())}",
                        rule_name=rule_name,
                        severity=rule.severity,
                        message=rule.message_template.format(value=metric_value),
                        metric_value=metric_value,
                        timestamp=datetime.now()
                    )
                    self.active_alerts[rule_name] = alert
                    self._handle_alert(alert)
            else:
                # 如果有未解决的告警且条件不再满足，则解决告警
                existing_alert = self.active_alerts.get(rule_name)
                if existing_alert and not existing_alert.resolved:
                    existing_alert.resolved = True
                    existing_alert.resolved_at = datetime.now()
                    self._handle_alert_resolution(existing_alert)
                    del self.active_alerts[rule_name]

    def _evaluate_condition(self, value: float, condition: str, threshold: float) -> bool:
        """评估条件"""
        if condition == ">":
            return value > threshold
        elif condition == "<":
            return value < threshold
        elif condition == ">=":
            return value >= threshold
        elif condition == "<=":
            return value <= threshold
        elif condition == "==":
            return value == threshold
        elif condition == "!=":
            return value != threshold
        return False

    def _normalise_factor_metric_name(self, name: str) -> str:
        """Ensure factor metrics share the same prefix for storage and alerting."""

        cleaned = name.strip()
        if cleaned.startswith("factor."):
            return cleaned
        if cleaned.startswith("factor_"):
            cleaned = cleaned[len("factor_") :]
        cleaned = cleaned.replace(" ", "_")
        return f"factor.{cleaned}"

    def _build_alert_key(self, rule: AlertRule, metric: MetricData) -> str:
        """Create a stable key for active alerts, including factor tags when present."""

        if metric.category == MetricCategory.FACTOR_COMPUTATION and metric.tags:
            factor_name = metric.tags.get("factor_name")
            if factor_name:
                return f"{rule.name}:{factor_name}"
        return rule.name

    def _render_alert_message(self, rule: AlertRule, metric: MetricData) -> str:
        """Build an alert message that understands metric tags."""

        context = {"value": metric.value}
        if metric.tags:
            context.update(metric.tags)

        try:
            return rule.message_template.format(**context)
        except KeyError:
            # 回落到旧格式，仅包含 value
            return rule.message_template.format(value=metric.value)

    def _process_metric_alerts(self, metric: MetricData):
        """Evaluate alert rules that match a newly recorded metric."""

        if not self.config.enable_alerting:
            return

        for rule in self.alert_rules.values():
            if not rule.enabled:
                continue
            if rule.metric_name != metric.name:
                continue

            alert_key = self._build_alert_key(rule, metric)
            should_alert = self._evaluate_condition(metric.value, rule.condition, rule.threshold)

            if should_alert:
                existing_alert = self.active_alerts.get(alert_key)
                if existing_alert is None:
                    alert = Alert(
                        id=f"{alert_key}_{int(time.time())}",
                        rule_name=rule.name,
                        severity=rule.severity,
                        message=self._render_alert_message(rule, metric),
                        metric_value=metric.value,
                        timestamp=metric.timestamp,
                        tags=metric.tags.copy() if metric.tags else None,
                    )
                    self.active_alerts[alert_key] = alert
                    self._handle_alert(alert)
            else:
                existing_alert = self.active_alerts.get(alert_key)
                if existing_alert and not existing_alert.resolved:
                    existing_alert.resolved = True
                    existing_alert.resolved_at = datetime.now()
                    self._handle_alert_resolution(existing_alert)
                    del self.active_alerts[alert_key]

    def _handle_alert(self, alert: Alert):
        """处理告警"""
        self.logger.warning(
            LogCategory.PERFORMANCE,
            f"Alert triggered: {alert.message}",
            alert_id=alert.id,
            severity=alert.severity.value,
            metric_value=alert.metric_value,
            tags=alert.tags or {},
        )

        # 保存告警到文件
        alert_file = self.log_dir / "alerts" / f"alert_{alert.timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        alert_data = asdict(alert)
        alert_data = _make_serializable(alert_data)
        alert_data['timestamp'] = alert.timestamp.isoformat()
        if alert.resolved_at:
            alert_data['resolved_at'] = alert.resolved_at.isoformat()

        with open(alert_file, 'w', encoding='utf-8') as f:
            json.dump(alert_data, f, indent=2, ensure_ascii=False)

    def _handle_alert_resolution(self, alert: Alert):
        """处理告警解决"""
        self.logger.info(
            LogCategory.PERFORMANCE,
            f"Alert resolved: {alert.message}",
            alert_id=alert.id,
            duration_minutes=(alert.resolved_at - alert.timestamp).total_seconds() / 60,
            tags=alert.tags or {},
        )

    def record_metric(self, name: str, value: float, metric_type: MetricType = MetricType.GAUGE,
                     category: MetricCategory = MetricCategory.CUSTOM, tags: Optional[Dict[str, str]] = None,
                     unit: Optional[str] = None, session_id: Optional[str] = None,
                     metadata: Optional[Dict[str, Any]] = None):
        """记录自定义指标"""
        if not self.config.enable_custom_metrics:
            return

        metric_data = MetricData(
            name=name,
            value=value,
            type=metric_type,
            category=category,
            timestamp=datetime.now(),
            tags=tags or {},
            unit=unit,
            session_id=session_id,
            metadata=metadata
        )

        with self._lock:
            self.metrics_history[name].append(metric_data)

        self._process_metric_alerts(metric_data)

        # 异步保存到数据库和文件
        threading.Thread(
            target=self._save_metric_to_storage,
            args=(metric_data,),
            daemon=True
        ).start()

    def record_factor_metrics(
        self,
        factor_name: str,
        payload: Dict[str, float],
        extra_tags: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Record multiple metrics for a factor in a single call."""

        if not payload:
            return

        base_tags = {"factor_name": factor_name}
        if extra_tags:
            base_tags.update(extra_tags)

        for metric_name, value in payload.items():
            if value is None:
                continue

            normalized_name = self._normalise_factor_metric_name(metric_name)
            template = self.factor_metric_templates.get(normalized_name)

            metric_tags = base_tags.copy()
            if template and template.default_tags:
                metric_tags.update(template.default_tags)

            unit = template.unit if template else None

            combined_metadata: Optional[Dict[str, Any]] = None
            if template and template.metadata:
                combined_metadata = dict(template.metadata)
                if metadata:
                    combined_metadata.update(metadata)
            elif metadata:
                combined_metadata = dict(metadata)

            self.record_metric(
                name=normalized_name,
                value=float(value),
                metric_type=MetricType.GAUGE,
                category=MetricCategory.FACTOR_COMPUTATION,
                tags=metric_tags,
                unit=unit,
                metadata=combined_metadata,
            )

    def _save_metric_to_storage(self, metric: MetricData):
        """保存指标到数据库和文件"""
        try:
            # 保存到数据库
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO metrics (timestamp, category, name, value, type, unit, tags, session_id, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                metric.timestamp.isoformat(),
                metric.category.value,
                metric.name,
                metric.value,
                metric.type.value,
                metric.unit,
                json.dumps(_make_serializable(metric.tags)) if metric.tags else None,
                metric.session_id,
                json.dumps(_make_serializable(metric.metadata)) if metric.metadata else None
            ))

            conn.commit()
            conn.close()

        except Exception as e:
            self.logger.error(LogCategory.PERFORMANCE, "Error saving metric to database", exception=e)

        # 保存到文件
        self._save_metric_to_file(metric)

    def _save_metric_to_file(self, metric: MetricData):
        """保存指标到文件"""
        filename = self.log_dir / "metrics" / f"metric_{metric.name}_{metric.timestamp.strftime('%Y%m%d_%H%M%S')}.json"

        data = _metric_to_dict(metric)

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _cleanup_old_data(self):
        """清理旧数据"""
        cutoff_time = datetime.now() - timedelta(hours=self.config.history_retention_hours)

        # 清理快照文件
        for snapshot_file in (self.log_dir / "metrics").glob("snapshot_*.json"):
            if snapshot_file.stat().st_mtime < cutoff_time.timestamp():
                try:
                    snapshot_file.unlink()
                except Exception:
                    pass

        # 清理指标文件
        for metric_file in (self.log_dir / "metrics").glob("metric_*.json"):
            if metric_file.stat().st_mtime < cutoff_time.timestamp():
                try:
                    metric_file.unlink()
                except Exception:
                    pass

        # 清理已解决的告警文件（保留7天）
        alert_cutoff = datetime.now() - timedelta(days=7)
        for alert_file in (self.log_dir / "alerts").glob("alert_*.json"):
            if alert_file.stat().st_mtime < alert_cutoff.timestamp():
                try:
                    alert_file.unlink()
                except Exception:
                    pass

    def get_latest_metrics(self) -> Optional[PerformanceSnapshot]:
        """获取最新的性能指标"""
        if not self.snapshots:
            return None
        return self.snapshots[-1]

    def get_metrics_history(self, metric_name: str, hours: int = 1) -> List[Dict[str, Any]]:
        """获取指标历史数据"""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        if metric_name in self.metrics_history:
            return [
                _metric_to_dict(metric)
                for metric in self.metrics_history[metric_name]
                if metric.timestamp >= cutoff_time
            ]
        elif metric_name in ['cpu_percent', 'memory_percent', 'disk_usage_percent']:
            # 从快照中提取系统指标
            return [
                {
                    'timestamp': snapshot.timestamp.isoformat(),
                    'value': getattr(snapshot, metric_name),
                    'unit': '%' if metric_name.endswith('_percent') else None
                }
                for snapshot in self.snapshots
                if snapshot.timestamp >= cutoff_time
            ]

        return []

    def get_active_alerts(self) -> List[Alert]:
        """获取活跃告警"""
        return list(self.active_alerts.values())

    def add_alert_rule(self, rule: AlertRule):
        """添加告警规则"""
        self.alert_rules[rule.name] = rule
        self.logger.info(LogCategory.PERFORMANCE, f"Added alert rule: {rule.name}")

    def remove_alert_rule(self, rule_name: str):
        """移除告警规则"""
        if rule_name in self.alert_rules:
            del self.alert_rules[rule_name]
            self.logger.info(LogCategory.PERFORMANCE, f"Removed alert rule: {rule_name}")

    def register_factor_metric_templates(self, templates: Iterable[FactorMetricTemplate]):
        """Register reusable templates for factor metrics."""

        for template in templates:
            metric_name = self._normalise_factor_metric_name(template.name)
            self.factor_metric_templates[metric_name] = template
            self.logger.info(
                LogCategory.PERFORMANCE,
                f"Registered factor metric template: {metric_name}",
                template=template.name,
            )

    def register_factor_alerts(self, alerts: Iterable[FactorAlertDefinition]):
        """Register alert rules bound to factor metrics."""

        for alert in alerts:
            metric_name = self._normalise_factor_metric_name(alert.metric)
            rule = AlertRule(
                name=alert.name,
                metric_name=metric_name,
                condition=alert.condition,
                threshold=alert.threshold,
                severity=alert.severity,
                message_template=alert.message_template,
                enabled=alert.enabled,
                cooldown_minutes=alert.cooldown_minutes,
            )
            self.add_alert_rule(rule)

    def generate_performance_report(self, hours: int = 24) -> Dict[str, Any]:
        """生成性能报告"""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        # 过滤指定时间范围内的快照
        relevant_snapshots = [
            snapshot for snapshot in self.snapshots
            if snapshot.timestamp >= cutoff_time
        ]

        if not relevant_snapshots:
            return {"error": "No data available for the specified time range"}

        # 计算统计数据
        cpu_values = [s.cpu_percent for s in relevant_snapshots]
        memory_values = [s.memory_percent for s in relevant_snapshots]
        disk_values = [s.disk_usage_percent for s in relevant_snapshots]

        report = {
            "time_range": {
                "start": cutoff_time.isoformat(),
                "end": datetime.now().isoformat(),
                "hours": hours
            },
            "cpu_stats": {
                "current": cpu_values[-1] if cpu_values else 0,
                "average": sum(cpu_values) / len(cpu_values) if cpu_values else 0,
                "max": max(cpu_values) if cpu_values else 0,
                "min": min(cpu_values) if cpu_values else 0
            },
            "memory_stats": {
                "current": memory_values[-1] if memory_values else 0,
                "average": sum(memory_values) / len(memory_values) if memory_values else 0,
                "max": max(memory_values) if memory_values else 0,
                "min": min(memory_values) if memory_values else 0
            },
            "disk_stats": {
                "current": disk_values[-1] if disk_values else 0,
                "average": sum(disk_values) / len(disk_values) if disk_values else 0,
                "max": max(disk_values) if disk_values else 0,
                "min": min(disk_values) if disk_values else 0
            },
            "alerts_triggered": len([
                alert for alert in self.active_alerts.values()
                if alert.timestamp >= cutoff_time
            ]),
            "data_points_collected": len(relevant_snapshots)
        }

        # 保存报告
        report_file = self.log_dir / "reports" / f"performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        return report

    def _export_performance_snapshot(self):
        """导出性能快照"""
        try:
            snapshot = self.get_latest_metrics()
            if not snapshot:
                return

            # 生成性能摘要
            summary = self.generate_performance_report(hours=1)

            # 保存到报告目录
            report_file = self.log_dir / "reports" / f"performance_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

        except Exception as e:
            self.logger.error(LogCategory.PERFORMANCE, "Error exporting performance snapshot", exception=e)

    @contextmanager
    def measure_operation(self, operation_name: str, session_id: Optional[str] = None, tags: Optional[Dict[str, str]] = None):
        """操作性能测量上下文管理器"""
        start_time = time.time()
        start_memory = (
            psutil.Process().memory_info().rss / (1024**2)  # MB
            if psutil is not None
            else 0.0
        )

        try:
            yield
        except Exception as e:
            # 记录操作错误
            self.operation_errors[operation_name] += 1

            error_metric = MetricData(
                name=f"{operation_name}_error",
                value=1.0,
                type=MetricType.COUNTER,
                category=MetricCategory.OPERATION,
                timestamp=datetime.now(),
                tags=tags or {},
                session_id=session_id,
                metadata={"error": str(e)}
            )
            self._save_metric_to_storage(error_metric)
            raise
        finally:
            # 计算执行时间和内存变化
            end_time = time.time()
            end_memory = (
                psutil.Process().memory_info().rss / (1024**2)
                if psutil is not None
                else 0.0
            )

            duration = end_time - start_time
            memory_delta = end_memory - start_memory

            # 记录操作指标
            duration_metric = MetricData(
                name=f"{operation_name}_duration",
                value=duration,
                type=MetricType.TIMER,
                category=MetricCategory.OPERATION,
                timestamp=datetime.now(),
                tags=tags or {},
                unit="seconds",
                session_id=session_id
            )
            self._save_metric_to_storage(duration_metric)

            memory_metric = MetricData(
                name=f"{operation_name}_memory_delta",
                value=memory_delta,
                type=MetricType.GAUGE,
                category=MetricCategory.OPERATION,
                timestamp=datetime.now(),
                tags=tags or {},
                unit="MB",
                session_id=session_id
            )
            self._save_metric_to_storage(memory_metric)

            # 更新操作统计
            self.operation_counts[operation_name] += 1
            total_key = f"{operation_name}_total"
            self.operation_timers[total_key] = self.operation_timers.get(total_key, 0.0) + duration

    def get_operation_statistics(self) -> Dict[str, Dict[str, float]]:
        """获取操作统计信息"""
        stats = {}

        with self._lock:
            for operation_name, count in self.operation_counts.items():
                total_time = self.operation_timers.get(f"{operation_name}_total", 0)
                error_count = self.operation_errors.get(operation_name, 0)

                stats[operation_name] = {
                    "count": count,
                    "total_time": total_time,
                    "average_time": total_time / count if count > 0 else 0,
                    "error_count": error_count,
                    "error_rate": error_count / count if count > 0 else 0
                }

        return stats

    @contextmanager
    def track_operation(
        self,
        operation_type: str,
        operation_name: str,
        session_id: Optional[str] = None,
    ):
        """Track an operation's execution time and optional metadata via a context manager."""

        start_time = time.time()
        metadata: Dict[str, Any] = {}

        try:
            yield metadata
        finally:
            execution_time = time.time() - start_time
            base_tags = {
                "operation_type": operation_type,
                "operation_name": operation_name,
            }

            self.record_metric(
                f"{operation_type}_{operation_name}_time",
                execution_time,
                MetricType.TIMER,
                MetricCategory.OPERATION,
                tags=base_tags,
                unit="seconds",
                session_id=session_id,
            )

            if metadata:
                enriched_metadata = {
                    **base_tags,
                    **metadata,
                }
                self.record_metric(
                    f"{operation_type}_{operation_name}_metadata",
                    1.0,
                    MetricType.GAUGE,
                    MetricCategory.OPERATION,
                    tags=base_tags,
                    unit=None,
                    session_id=session_id,
                    metadata=enriched_metadata,
                )

    def export_metrics_to_file(self, filepath: str, start_time: Optional[datetime] = None,
                              end_time: Optional[datetime] = None,
                              categories: Optional[List[MetricCategory]] = None,
                              compress: bool = True) -> bool:
        """导出指标数据到文件"""
        try:
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()

            query = "SELECT * FROM metrics WHERE 1=1"
            params = []

            if start_time:
                query += " AND timestamp >= ?"
                params.append(start_time.isoformat())

            if end_time:
                query += " AND timestamp <= ?"
                params.append(end_time.isoformat())

            if categories:
                placeholders = ",".join(["?" for _ in categories])
                query += f" AND category IN ({placeholders})"
                params.extend([cat.value for cat in categories])

            query += " ORDER BY timestamp"

            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()

            # 转换为JSON格式
            metrics_data = []
            for row in rows:
                metric_dict = {
                    "id": row[0],
                    "timestamp": row[1],
                    "category": row[2],
                    "name": row[3],
                    "value": row[4],
                    "type": row[5],
                    "unit": row[6],
                    "tags": json.loads(row[7]) if row[7] else None,
                    "session_id": row[8],
                    "metadata": json.loads(row[9]) if row[9] else None
                }
                metrics_data.append(metric_dict)

            # 写入文件（支持压缩）
            if compress and filepath.endswith('.gz'):
                with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                    json.dump(metrics_data, f, indent=2, ensure_ascii=False)
            else:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(metrics_data, f, indent=2, ensure_ascii=False)

            return True

        except Exception as e:
            self.logger.error(LogCategory.PERFORMANCE, "Error exporting metrics to file", exception=e)
            return False

    def get_metrics(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        categories: Optional[List[MetricCategory]] = None,
    ) -> List[MetricData]:
        """Fetch metrics from the SQLite store as MetricData objects."""

        try:
            conn = sqlite3.connect(self.config.database_path)
            cursor = conn.cursor()

            query = "SELECT timestamp, category, name, value, type, unit, tags, session_id, metadata FROM metrics WHERE 1=1"
            params: List[Any] = []

            if start_time:
                query += " AND timestamp >= ?"
                params.append(start_time.isoformat())

            if end_time:
                query += " AND timestamp <= ?"
                params.append(end_time.isoformat())

            if categories:
                placeholders = ",".join(["?" for _ in categories])
                query += f" AND category IN ({placeholders})"
                params.extend([cat.value for cat in categories])

            query += " ORDER BY timestamp"
            cursor.execute(query, params)
            rows = cursor.fetchall()
            conn.close()

            metrics: List[MetricData] = []
            for row in rows:
                tags = json.loads(row[6]) if row[6] else {}
                metadata = json.loads(row[8]) if row[8] else None
                metric = MetricData(
                    name=row[2],
                    value=row[3],
                    type=MetricType(row[4]),
                    category=MetricCategory(row[1]),
                    timestamp=datetime.fromisoformat(row[0]),
                    tags=tags,
                    unit=row[5],
                    session_id=row[7],
                    metadata=metadata,
                )
                metrics.append(metric)

            return metrics

        except Exception as e:
            self.logger.error(LogCategory.PERFORMANCE, "Error loading metrics", exception=e)
            return []

    def export_metrics(
        self,
        format_type: str = "json",
        export_dir: str = "exports",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        categories: Optional[List[MetricCategory]] = None,
    ) -> str:
        """导出指标数据."""

        export_path = Path(export_dir)
        export_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        format_lower = format_type.lower()
        filename = f"metrics_export_{timestamp}.{format_lower}"
        filepath = export_path / filename

        metrics_data = self.get_metrics(start_time=start_time, end_time=end_time, categories=categories)
        serializable = [_metric_to_dict(metric) for metric in metrics_data]

        if format_lower == "json":
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(serializable, f, indent=2, ensure_ascii=False)
        elif format_lower == "csv":
            try:
                import pandas as pd  # type: ignore
            except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("pandas is required to export metrics as CSV") from exc

            df = pd.DataFrame(serializable)
            df.to_csv(filepath, index=False)
        else:
            raise ValueError(f"Unsupported export format: {format_type}")

        return str(filepath)

    def get_system_summary(self) -> Dict[str, Any]:
        """获取增强系统摘要信息"""
        latest = self.get_latest_metrics()
        if not latest:
            return {"status": "No data available"}

        operation_stats = self.get_operation_statistics()

        return {
            "timestamp": latest.timestamp.isoformat(),
            "system_metrics": {
                "cpu_percent": latest.cpu_percent,
                "memory_percent": latest.memory_percent,
                "memory_used_mb": latest.memory_used_mb,
                "memory_total_mb": latest.memory_total_mb,
                "disk_usage_percent": latest.disk_usage_percent,
                "network_sent_mb": latest.network_sent_mb,
                "network_recv_mb": latest.network_recv_mb,
                "thread_count": latest.thread_count,
                "process_count": latest.process_count
            },
            "active_alerts_count": len(self.active_alerts),
            "active_alerts": [
                {
                    "name": alert.rule_name,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "triggered_at": alert.timestamp.isoformat()
                }
                for alert in self.active_alerts.values()
            ],
            "operation_statistics": operation_stats,
            "monitoring_status": "running" if self._running else "stopped"
        }


def get_performance_monitor(config: Optional[MonitorConfig] = None) -> PerformanceMonitor:
    """获取全局性能监控器实例"""
    global _global_monitor

    with _monitor_lock:
        if _global_monitor is None:
            _global_monitor = PerformanceMonitor(config or MonitorConfig())
        return _global_monitor


def get_monitor(log_dir: str = "logs") -> PerformanceMonitor:
    """获取性能监控器实例（向后兼容）"""
    config = MonitorConfig(log_dir=log_dir)
    return get_performance_monitor(config)


def start_global_monitoring(config: Optional[MonitorConfig] = None):
    """启动全局性能监控"""
    global _global_monitor

    with _monitor_lock:
        if _global_monitor is None:
            _global_monitor = PerformanceMonitor(config or MonitorConfig())
        _global_monitor.start()


def stop_global_monitoring():
    """停止全局性能监控"""
    global _global_monitor

    with _monitor_lock:
        if _global_monitor:
            _global_monitor.stop()
            _global_monitor = None


def record_metric(name: str, value: float, unit: str,
                 category: MetricCategory = MetricCategory.CUSTOM,
                 tags: Optional[Dict[str, str]] = None,
                 session_id: Optional[str] = None,
                 metadata: Optional[Dict[str, Any]] = None):
    """记录性能指标"""
    monitor = get_performance_monitor()
    monitor.record_metric(name, value, MetricType.GAUGE, category, tags, unit, session_id, metadata)


def record_counter(name: str, value: float = 1.0,
                  category: MetricCategory = MetricCategory.CUSTOM,
                  tags: Optional[Dict[str, str]] = None,
                  session_id: Optional[str] = None):
    """记录计数器指标"""
    monitor = get_performance_monitor()
    monitor.record_metric(name, value, MetricType.COUNTER, category, tags, None, session_id)


def record_timer(name: str, value: float,
                 category: MetricCategory = MetricCategory.OPERATION,
                 tags: Optional[Dict[str, str]] = None,
                 session_id: Optional[str] = None):
    """记录计时器指标"""
    monitor = get_performance_monitor()
    monitor.record_metric(name, value, MetricType.TIMER, category, tags, "seconds", session_id)


def record_factor_metrics(
    factor_name: str,
    payload: Dict[str, float],
    extra_tags: Optional[Dict[str, str]] = None,
    metadata: Optional[Dict[str, Any]] = None,
):
    """Record factor performance metrics via the global monitor."""

    monitor = get_performance_monitor()
    monitor.record_factor_metrics(factor_name, payload, extra_tags=extra_tags, metadata=metadata)


def performance_monitored(metric_name: str, tags: Optional[Dict[str, str]] = None):
    """性能监控装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            monitor = get_performance_monitor()
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                # 记录执行时间
                monitor.record_metric(
                    f"{metric_name}_execution_time",
                    execution_time,
                    MetricType.TIMER,
                    MetricCategory.OPERATION,
                    tags,
                    "seconds"
                )

                return result
            except Exception as e:
                execution_time = time.time() - start_time

                # 记录错误
                monitor.record_metric(
                    f"{metric_name}_errors",
                    1,
                    MetricType.COUNTER,
                    MetricCategory.OPERATION,
                    tags
                )

                raise

        return wrapper
    return decorator


# 性能跟踪上下文管理器
class PerformanceTracker:
    """性能跟踪上下文管理器"""

    def __init__(self, operation_name: str, tags: Optional[Dict[str, str]] = None, session_id: Optional[str] = None):
        self.operation_name = operation_name
        self.tags = tags or {}
        self.session_id = session_id
        self.monitor = get_performance_monitor()
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time is not None:
            execution_time = time.time() - self.start_time

            self.monitor.record_metric(
                f"{self.operation_name}_execution_time",
                execution_time,
                MetricType.TIMER,
                MetricCategory.OPERATION,
                self.tags,
                "seconds",
                self.session_id
            )

            if exc_type is not None:
                self.monitor.record_metric(
                    f"{self.operation_name}_errors",
                    1,
                    MetricType.COUNTER,
                    MetricCategory.OPERATION,
                    self.tags,
                    None,
                    self.session_id
                )


# 便捷的性能测量上下文管理器
@contextmanager
def measure_operation_performance(operation_name: str, tags: Optional[Dict[str, str]] = None, session_id: Optional[str] = None):
    """便捷的性能测量上下文管理器"""
    monitor = get_performance_monitor()
    with monitor.measure_operation(operation_name, session_id, tags):
        yield


def get_system_metrics_summary() -> Dict[str, Any]:
    """获取系统指标摘要"""
    monitor = get_performance_monitor()
    return monitor.get_system_summary()


def get_operation_stats() -> Dict[str, Dict[str, float]]:
    """获取操作统计信息"""
    monitor = get_performance_monitor()
    return monitor.get_operation_statistics()


# 示例用法和测试
if __name__ == "__main__":
    # 创建监控配置
    config = MonitorConfig(
        collection_interval_seconds=10,
        alert_check_interval_seconds=30,
        history_retention_hours=2
    )

    # 创建监控实例
    monitor = PerformanceMonitor(config)

    print("🚀 性能监控系统启动")
    print(f"📊 数据收集间隔: {config.collection_interval_seconds}秒")
    print(f"🚨 告警检查间隔: {config.alert_check_interval_seconds}秒")

    # 运行一段时间收集数据
    print("\n⏳ 收集性能数据...")
    time.sleep(60)

    # 显示系统摘要
    print("\n📈 系统性能摘要:")
    summary = monitor.get_system_summary()
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    # 生成性能报告
    print("\n📊 生成性能报告...")
    report = monitor.generate_performance_report(hours=1)
    print(f"报告已保存: {report}")

    # 测试自定义指标
    print("\n📝 测试自定义指标记录...")
    monitor.record_metric("test_operations", 100, MetricType.COUNTER, {"module": "test"})
    monitor.record_metric("response_time", 0.045, MetricType.TIMER, {"endpoint": "/api/test"})

    # 显示活跃告警
    print(f"\n🚨 活跃告警数量: {len(monitor.get_active_alerts())}")

    print("\n✅ 性能监控系统测试完成！")
