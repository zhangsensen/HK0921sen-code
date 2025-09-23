"""配置文件验证模块"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .config_loader import ConfigLoader


class ConfigValidationError(Exception):
    """配置验证错误"""
    pass


class ConfigValidator:
    """配置文件验证器"""

    def __init__(self, config_loader: Optional[ConfigLoader] = None):
        self.config_loader = config_loader or ConfigLoader()
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def validate_config(self) -> bool:
        """验证配置文件"""
        self.errors.clear()
        self.warnings.clear()
        
        try:
            config = self.config_loader.load_config()
        except Exception as e:
            self.errors.append(f"配置文件加载失败: {e}")
            return False
        
        # 验证各个配置节
        self._validate_data_config(config.get("data", {}))
        self._validate_database_config(config.get("database", {}))
        self._validate_logging_config(config.get("logging", {}))
        self._validate_parallel_config(config.get("parallel", {}))
        self._validate_combiner_config(config.get("combiner", {}))
        self._validate_monitoring_config(config.get("monitoring", {}))
        self._validate_app_config(config.get("app", {}))
        
        return len(self.errors) == 0

    def _validate_data_config(self, data_config: Dict[str, Any]) -> None:
        """验证数据配置"""
        # 验证数据根目录
        data_root = data_config.get("root")
        if data_root is None:
            self.errors.append("数据根目录配置缺失: data.root")
        else:
            data_root_path = Path(data_root).expanduser()
            if not data_root_path.exists():
                self.warnings.append(f"数据根目录不存在: {data_root_path}")
            elif not data_root_path.is_dir():
                self.errors.append(f"数据根目录不是有效目录: {data_root_path}")
        
        # 验证缓存TTL
        cache_ttl = data_config.get("cache_ttl", 300)
        if not isinstance(cache_ttl, int) or cache_ttl < 0:
            self.errors.append(f"缓存TTL必须是非负整数: {cache_ttl}")
        
        # 验证异步批处理大小
        async_batch_size = data_config.get("async_batch_size", 8)
        if not isinstance(async_batch_size, int) or async_batch_size < 1:
            self.errors.append(f"异步批处理大小必须是正整数: {async_batch_size}")

    def _validate_database_config(self, db_config: Dict[str, Any]) -> None:
        """验证数据库配置"""
        db_path = db_config.get("path")
        if db_path is None:
            self.errors.append("数据库路径配置缺失: database.path")
        else:
            db_path_obj = Path(db_path).expanduser()
            # 确保数据库目录存在
            db_dir = db_path_obj.parent
            if not db_dir.exists():
                try:
                    db_dir.mkdir(parents=True, exist_ok=True)
                    self.warnings.append(f"创建数据库目录: {db_dir}")
                except Exception as e:
                    self.errors.append(f"无法创建数据库目录 {db_dir}: {e}")

    def _validate_logging_config(self, logging_config: Dict[str, Any]) -> None:
        """验证日志配置"""
        log_level = logging_config.get("level", "INFO")
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        if log_level.upper() not in valid_levels:
            self.errors.append(f"无效的日志级别: {log_level}，必须是: {', '.join(valid_levels)}")

    def _validate_parallel_config(self, parallel_config: Dict[str, Any]) -> None:
        """验证并行处理配置"""
        mode = parallel_config.get("mode", "off")
        valid_modes = ["off", "process"]
        if mode not in valid_modes:
            self.errors.append(f"无效的并行模式: {mode}，必须是: {', '.join(valid_modes)}")
        
        max_workers = parallel_config.get("max_workers")
        if max_workers is not None:
            if not isinstance(max_workers, int) or max_workers < 1:
                self.errors.append(f"最大工作进程数必须是正整数: {max_workers}")
        
        memory_limit_mb = parallel_config.get("memory_limit_mb")
        if memory_limit_mb is not None:
            if not isinstance(memory_limit_mb, (int, float)) or memory_limit_mb <= 0:
                self.errors.append(f"内存限制必须是正数: {memory_limit_mb}")

    def _validate_combiner_config(self, combiner_config: Dict[str, Any]) -> None:
        """验证因子组合器配置"""
        top_n = combiner_config.get("top_n", 20)
        if not isinstance(top_n, int) or top_n < 1:
            self.errors.append(f"top_n必须是正整数: {top_n}")
        
        max_factors = combiner_config.get("max_factors", 3)
        if not isinstance(max_factors, int) or max_factors < 1:
            self.errors.append(f"max_factors必须是正整数: {max_factors}")
        
        max_combinations = combiner_config.get("max_combinations", 1000)
        if max_combinations is not None:
            if not isinstance(max_combinations, int) or max_combinations < 1:
                self.errors.append(f"max_combinations必须是正整数: {max_combinations}")
        
        min_sharpe = combiner_config.get("min_sharpe", 0.0)
        if not isinstance(min_sharpe, (int, float)):
            self.errors.append(f"min_sharpe必须是数字: {min_sharpe}")
        
        min_ic = combiner_config.get("min_information_coefficient", 0.0)
        if not isinstance(min_ic, (int, float)):
            self.errors.append(f"min_information_coefficient必须是数字: {min_ic}")

    def _validate_monitoring_config(self, monitoring_config: Dict[str, Any]) -> None:
        """验证性能监控配置"""
        enabled = monitoring_config.get("enabled", False)
        if not isinstance(enabled, bool):
            self.errors.append(f"monitoring.enabled必须是布尔值: {enabled}")
        
        if enabled:
            log_dir = monitoring_config.get("log_dir")
            if log_dir is None:
                self.errors.append("监控日志目录配置缺失: monitoring.log_dir")
            else:
                log_dir_path = Path(log_dir).expanduser()
                try:
                    log_dir_path.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    self.errors.append(f"无法创建监控日志目录 {log_dir_path}: {e}")
            
            db_path = monitoring_config.get("database_path")
            if db_path is None:
                self.errors.append("监控数据库路径配置缺失: monitoring.database_path")
            else:
                db_path_obj = Path(db_path).expanduser()
                db_dir = db_path_obj.parent
                try:
                    db_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    self.errors.append(f"无法创建监控数据库目录 {db_dir}: {e}")

    def _validate_app_config(self, app_config: Dict[str, Any]) -> None:
        """验证应用配置"""
        reset = app_config.get("reset", False)
        if not isinstance(reset, bool):
            self.errors.append(f"app.reset必须是布尔值: {reset}")
        
        default_phase = app_config.get("default_phase", "both")
        valid_phases = ["phase1", "phase2", "both"]
        if default_phase not in valid_phases:
            self.errors.append(f"无效的默认阶段: {default_phase}，必须是: {', '.join(valid_phases)}")

    def get_errors(self) -> List[str]:
        """获取所有错误"""
        return self.errors.copy()

    def get_warnings(self) -> List[str]:
        """获取所有警告"""
        return self.warnings.copy()

    def print_validation_result(self) -> None:
        """打印验证结果"""
        if not self.errors and not self.warnings:
            print("✅ 配置验证通过")
            return
        
        if self.errors:
            print("❌ 配置验证失败:")
            for error in self.errors:
                print(f"  - {error}")
        
        if self.warnings:
            print("⚠️  配置警告:")
            for warning in self.warnings:
                print(f"  - {warning}")


def validate_config_file(config_path: Optional[Union[str, Path]] = None) -> bool:
    """验证配置文件"""
    config_loader = ConfigLoader(config_path)
    validator = ConfigValidator(config_loader)
    
    if validator.validate_config():
        validator.print_validation_result()
        return True
    else:
        validator.print_validation_result()
        return False
