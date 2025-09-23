#!/usr/bin/env python3
"""
Enhanced Logging System for Hong Kong Factor Discovery System
提供结构化日志记录、多级别日志分类、自动文件轮转和完整审计跟踪
"""

import json
import logging as std_logging
import os
import sys
import traceback
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Type
from dataclasses import dataclass, asdict
import threading
import time


class LogLevel(Enum):
    """日志级别枚举"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogCategory(Enum):
    """日志分类枚举"""
    SYSTEM = "system"              # 系统操作
    DATA_LOADING = "data_loading"   # 数据加载
    FACTOR_COMPUTATION = "factor_computation"  # 因子计算
    BACKTEST = "backtest"          # 回测执行
    PERFORMANCE = "performance"    # 性能监控
    ERROR = "error"               # 错误跟踪
    AUDIT = "audit"               # 审计日志
    METRICS = "metrics"           # 指标记录


@dataclass
class LogConfig:
    """Configuration for logging system."""
    log_level: str = "INFO"
    log_dir: str = "logs"
    max_file_size: int = 50 * 1024 * 1024  # 50MB
    backup_count: int = 10
    json_format: bool = True
    include_timestamp: bool = True
    include_caller: bool = True


@dataclass
class LogContext:
    """日志上下文信息"""
    session_id: str
    user_id: Optional[str] = None
    project_name: Optional[str] = None
    component: Optional[str] = None
    operation: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class PerformanceMetrics:
    """性能指标"""
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    memory_usage: Optional[float] = None
    cpu_usage: Optional[float] = None

    def finish(self):
        """结束性能计时"""
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time


class EnhancedStructuredLogger:
    """Enhanced logging system with comprehensive audit tracking and performance monitoring."""

    def __init__(self, config: LogConfig):
        self.config = config
        self.log_dir = Path(config.log_dir)
        self._instances = {}
        self._lock = threading.Lock()
        self._performance_stack = []
        self._setup_directories()
        self._setup_loggers()

    def _setup_directories(self):
        """Create necessary directories for logs."""
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 为每个日志分类创建子目录
        for category in LogCategory:
            (self.log_dir / category.value).mkdir(exist_ok=True)

    def _setup_loggers(self):
        """Setup different loggers for different purposes."""
        # 为每个日志分类创建记录器
        self.loggers = {}
        for category in LogCategory:
            self.loggers[category] = self._create_category_logger(category)

    def _create_category_logger(self, category: LogCategory):
        """Create a logger for specific category."""
        logger = std_logging.getLogger(f"hk_factor.{category.value}")
        logger.setLevel(getattr(std_logging, self.config.log_level))

        # Remove existing handlers
        logger.handlers.clear()

        # Simple file handler
        file_path = self.log_dir / category.value / f"{category.value}.log"
        handler = std_logging.FileHandler(file_path, encoding='utf-8')

        # JSON formatter
        formatter = std_logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # 添加控制台处理器（仅ERROR及以上级别）
        if category == LogCategory.ERROR:
            console_handler = std_logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def _log(self,
             level: LogLevel,
             category: LogCategory,
             message: str,
             context: Optional[LogContext] = None,
             metrics: Optional[PerformanceMetrics] = None,
             **kwargs):
        """
        记录日志

        Args:
            level: 日志级别
            category: 日志分类
            message: 日志消息
            context: 日志上下文
            metrics: 性能指标
            **kwargs: 额外字段
        """
        logger = self.loggers.get(category)
        if not logger:
            return

        # 构建结构化日志记录
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level.value,
            "category": category.value,
            "message": message,
            **kwargs
        }

        # 添加上下文信息
        if context:
            log_entry["context"] = asdict(context)

        # 添加性能指标
        if metrics and hasattr(metrics, '__dataclass_fields__'):
            log_entry["metrics"] = asdict(metrics)
        elif metrics:
            log_entry["metrics"] = metrics

        # 添加调用栈信息（ERROR级别及以上）
        if level in [LogLevel.ERROR, LogLevel.CRITICAL]:
            log_entry["traceback"] = traceback.format_stack()[-5:-1]

        # 记录到文件
        logger.log(getattr(std_logging, level.value), json.dumps(log_entry, ensure_ascii=False))

    # 基本日志方法
    def debug(self, category: LogCategory, message: str, context: Optional[LogContext] = None, **kwargs):
        """记录DEBUG级别日志"""
        self._log(LogLevel.DEBUG, category, message, context, **kwargs)

    def info(self, category: LogCategory, message: str, context: Optional[LogContext] = None, **kwargs):
        """记录INFO级别日志"""
        self._log(LogLevel.INFO, category, message, context, **kwargs)

    def warning(self, category: LogCategory, message: str, context: Optional[LogContext] = None, **kwargs):
        """记录WARNING级别日志"""
        self._log(LogLevel.WARNING, category, message, context, **kwargs)

    def error(self, category: LogCategory, message: str, context: Optional[LogContext] = None, exception: Optional[Exception] = None, **kwargs):
        """记录ERROR级别日志"""
        if exception:
            kwargs["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "traceback": traceback.format_exc()
            }
        self._log(LogLevel.ERROR, category, message, context, **kwargs)

    def critical(self, category: LogCategory, message: str, context: Optional[LogContext] = None, exception: Optional[Exception] = None, **kwargs):
        """记录CRITICAL级别日志"""
        if exception:
            kwargs["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "traceback": traceback.format_exc()
            }
        self._log(LogLevel.CRITICAL, category, message, context, **kwargs)

    # 性能跟踪方法
    def start_performance_tracking(self, category: LogCategory, operation: str, context: Optional[LogContext] = None) -> PerformanceMetrics:
        """开始性能跟踪"""
        metrics = PerformanceMetrics(start_time=time.time())
        self._performance_stack.append((category, operation, context, metrics))

        self.info(category, f"Performance tracking started: {operation}", context, operation=operation, phase="start")
        return metrics

    def end_performance_tracking(self, metrics: PerformanceMetrics):
        """结束性能跟踪"""
        if not self._performance_stack:
            return

        category, operation, context, _ = self._performance_stack.pop()
        metrics.finish()

        self.info(category, f"Performance tracking ended: {operation}", context, operation=operation, phase="end", duration_seconds=metrics.duration)

    # 专门的日志记录方法
    def log_system_event(self, event_type: str, message: str, context: Optional[LogContext] = None, **kwargs):
        """记录系统事件"""
        self.info(LogCategory.SYSTEM, message, context, event_type=event_type, **kwargs)

    def log_data_operation(self, operation: str, symbol: str, timeframe: str, record_count: int, context: Optional[LogContext] = None, **kwargs):
        """记录数据操作"""
        self.info(LogCategory.DATA_LOADING, f"Data operation: {operation}", context, operation=operation, symbol=symbol, timeframe=timeframe, record_count=record_count, **kwargs)

    def log_factor_computation(self, factor_name: str, symbol: str, timeframe: str, computation_time: float, context: Optional[LogContext] = None, **kwargs):
        """记录因子计算"""
        self.info(LogCategory.FACTOR_COMPUTATION, f"Factor computed: {factor_name}", context, factor_name=factor_name, symbol=symbol, timeframe=timeframe, computation_time=computation_time, **kwargs)

    def log_backtest_event(self, strategy_id: str, event_type: str, message: str, context: Optional[LogContext] = None, **kwargs):
        """记录回测事件"""
        self.info(LogCategory.BACKTEST, message, context, strategy_id=strategy_id, event_type=event_type, **kwargs)

    def log_audit_event(self, action: str, resource: str, result: str, context: Optional[LogContext] = None, **kwargs):
        """记录审计事件"""
        self.info(LogCategory.AUDIT, f"Audit event: {action} on {resource}", context, action=action, resource=resource, result=result, **kwargs)

    # 日志查询和管理方法
    def get_recent_logs(self, category: Optional[LogCategory] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """获取最近的日志记录"""
        logs = []
        categories = [category] if category else LogCategory

        for cat in categories:
            log_file = self.log_dir / cat.value / f"{cat.value}.log"
            if not log_file.exists():
                continue

            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

                for line in lines[-limit:]:
                    try:
                        parts = line.strip().split(' - ', 3)
                        if len(parts) >= 4:
                            json_part = parts[3]
                            log_data = json.loads(json_part)
                            logs.append(log_data)
                    except (json.JSONDecodeError, IndexError):
                        continue

            except Exception as e:
                self.error(LogCategory.ERROR, f"Failed to read log file: {log_file}", exception=e)

        logs.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return logs[:limit]

    def cleanup_old_logs(self, days_to_keep: int = 30):
        """清理旧日志文件"""
        cutoff_date = datetime.now().timestamp() - (days_to_keep * 24 * 60 * 60)

        for log_file in self.log_dir.rglob("*.log*"):
            try:
                if log_file.stat().st_mtime < cutoff_date:
                    log_file.unlink()
                    self.info(LogCategory.SYSTEM, f"Cleaned up old log file: {log_file.name}")
            except Exception as e:
                self.error(LogCategory.ERROR, f"Failed to cleanup log file: {log_file}", exception=e)

    def get_log_statistics(self) -> Dict[str, Any]:
        """获取日志统计信息"""
        stats = {"total_files": 0, "total_size_bytes": 0, "category_stats": {}}

        for category in LogCategory:
            log_file = self.log_dir / category.value / f"{category.value}.log"
            if log_file.exists():
                stats["total_files"] += 1
                size = log_file.stat().st_size
                stats["total_size_bytes"] += size

                stats["category_stats"][category.value] = {
                    "file_exists": True,
                    "size_bytes": size,
                    "size_mb": round(size / (1024 * 1024), 2)
                }
            else:
                stats["category_stats"][category.value] = {
                    "file_exists": False,
                    "size_bytes": 0,
                    "size_mb": 0
                }

        return stats

    # 保持向后兼容的方法
    def log_factor_exploration(self, symbol: str, timeframe: str, factor: str,
                             metrics: Dict[str, Any], execution_time: float):
        """Log factor exploration results."""
        self.log_factor_computation(factor, symbol, timeframe, execution_time,
                                   context=None, metrics=metrics)

    def log_strategy_discovery(self, symbol: str, strategy_name: str,
                              factors: list, performance: Dict[str, Any]):
        """Log strategy discovery results."""
        self.info(LogCategory.BACKTEST, f"Strategy discovery completed: {strategy_name}",
                 context=None, symbol=symbol, strategy_name=strategy_name,
                 factors=factors, performance=performance)

    def log_performance_metrics(self, operation: str, duration: float,
                              memory_usage: float, success: bool):
        """Log performance metrics."""
        self.info(LogCategory.PERFORMANCE, f"Performance metrics: {operation}",
                 context=None, operation=operation, duration=duration,
                 memory_usage=memory_usage, success=success)

    def log_error(self, error_type: str, error_message: str,
                  context: Optional[Dict[str, Any]] = None):
        """Log errors with context."""
        self.error(LogCategory.ERROR, error_message, context=None,
                  error_type=error_type, error_context=context)


class EnhancedLogManager:
    """Enhanced singleton manager for logging system."""

    _instances = {}
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, config: Optional[LogConfig] = None, log_dir: str = "logs") -> 'EnhancedStructuredLogger':
        """Get or create logger instance."""
        log_dir_path = Path(log_dir)

        with cls._lock:
            if str(log_dir_path) not in cls._instances:
                cls._instances[str(log_dir_path)] = EnhancedStructuredLogger(config or LogConfig())
            return cls._instances[str(log_dir_path)]

    @classmethod
    def initialize(cls, config: LogConfig, log_dir: str = "logs"):
        """Initialize logging system."""
        log_dir_path = Path(log_dir)
        cls._instances[str(log_dir_path)] = EnhancedStructuredLogger(config)

    @classmethod
    def get_logger(cls, name: str):
        """Get standard Python logger."""
        return std_logging.getLogger(f"hk_factor.{name}")

    @classmethod
    def cleanup_all_instances(cls):
        """Clean up all logger instances."""
        with cls._lock:
            for instance in cls._instances.values():
                try:
                    instance.cleanup_old_logs()
                except Exception:
                    pass
            cls._instances.clear()


# Convenience functions
def get_enhanced_logger(log_dir: str = "logs") -> 'EnhancedStructuredLogger':
    """Get enhanced logger instance."""
    return EnhancedLogManager.get_instance(log_dir=log_dir)


def create_context(session_id: str,
                  user_id: Optional[str] = None,
                  project_name: Optional[str] = None,
                  component: Optional[str] = None,
                  operation: Optional[str] = None,
                  **kwargs) -> LogContext:
    """Create log context."""
    return LogContext(
        session_id=session_id,
        user_id=user_id,
        project_name=project_name,
        component=component,
        operation=operation,
        metadata=kwargs
    )


# 向后兼容的便捷函数
def get_structured_logger() -> 'EnhancedStructuredLogger':
    """Get structured logger instance (backward compatibility)."""
    return get_enhanced_logger()


def log_factor_result(symbol: str, timeframe: str, factor: str,
                      metrics: Dict[str, Any], execution_time: float):
    """Log factor exploration result."""
    logger = get_enhanced_logger()
    logger.log_factor_exploration(symbol, timeframe, factor, metrics, execution_time)


def log_strategy_result(symbol: str, strategy_name: str,
                       factors: list, performance: Dict[str, Any]):
    """Log strategy discovery result."""
    logger = get_enhanced_logger()
    logger.log_strategy_discovery(symbol, strategy_name, factors, performance)


def log_performance(operation: str, duration: float,
                   memory_usage: float, success: bool):
    """Log performance metrics."""
    logger = get_enhanced_logger()
    logger.log_performance_metrics(operation, duration, memory_usage, success)


def log_error(error_type: str, error_message: str,
             context: Optional[Dict[str, Any]] = None):
    """Log error."""
    logger = get_enhanced_logger()
    logger.log_error(error_type, error_message, context)


# 性能跟踪装饰器
def performance_tracked(category: LogCategory, operation_name: str):
    """Performance tracking decorator."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_enhanced_logger()
            context = None

            # 尝试从参数中获取context
            for arg in args:
                if isinstance(arg, LogContext):
                    context = arg
                    break

            metrics = logger.start_performance_tracking(category, operation_name, context)
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.error(
                    category,
                    f"Performance tracking failed: {operation_name}",
                    context,
                    exception=e
                )
                raise
            finally:
                logger.end_performance_tracking(metrics)

        return wrapper
    return decorator


# 保持向后兼容性
LogManager = EnhancedLogManager
StructuredLogger = EnhancedStructuredLogger


# 示例用法和测试
if __name__ == "__main__":
    # 创建增强日志记录器
    logger = get_enhanced_logger("test_logs")

    # 创建上下文
    context = create_context(
        session_id="test_session_001",
        user_id="test_user",
        project_name="hk_factor_test",
        component="data_loader",
        operation="load_data"
    )

    # 记录各种日志
    logger.info(LogCategory.SYSTEM, "System started", context)
    logger.log_data_operation("load", "0700.HK", "1m", 40709, context)
    logger.log_factor_computation("ma_5", "0700.HK", "1m", 0.023, context)

    # 性能跟踪
    metrics = logger.start_performance_tracking(LogCategory.DATA_LOADING, "load_data", context)
    time.sleep(0.1)  # 模拟操作
    logger.end_performance_tracking(metrics)

    # 测试向后兼容函数
    log_factor_result("0700.HK", "1m", "rsi_14", {"sharpe": 1.2}, 0.045)

    # 获取日志统计
    stats = logger.get_log_statistics()
    print(f"Log statistics: {json.dumps(stats, indent=2, ensure_ascii=False)}")

    # 获取最近的日志
    recent_logs = logger.get_recent_logs(limit=5)
    print(f"Recent logs: {len(recent_logs)} entries")

    # 清理
    EnhancedLogManager.cleanup_all_instances()
    print("Enhanced logging system test completed successfully!")
