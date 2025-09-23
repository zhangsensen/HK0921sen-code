"""配置文件加载器模块"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml

from config import CombinerConfig
from utils.validation import validate_symbol
from utils.monitoring.config import MonitorConfig


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = (REPO_ROOT / "benchmark_results" / "hk_factor_results.sqlite").resolve()


class ConfigLoader:
    """配置文件加载器，支持YAML格式和环境变量替换"""

    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        self.config_path = Path(config_path) if config_path else Path("config.yaml")
        self._config_cache: Optional[Dict[str, Any]] = None

    def load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        if self._config_cache is not None:
            return self._config_cache

        if not self.config_path.exists():
            # 如果配置文件不存在，返回默认配置
            self._config_cache = self._get_default_config()
            return self._config_cache

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # 处理环境变量替换
            config = self._process_env_vars(config)
            
            self._config_cache = config
            return config
        except Exception as e:
            raise RuntimeError(f"加载配置文件失败: {e}")

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "data": {
                "root": "./data",
                "cache_ttl": 300,
                "async_batch_size": 8
            },
            "database": {
                "path": str(DEFAULT_DB_PATH)
            },
            "logging": {
                "level": "INFO"
            },
            "parallel": {
                "mode": "off",
                "max_workers": None,
                "memory_limit_mb": None
            },
            "combiner": {
                "top_n": 20,
                "max_factors": 3,
                "max_combinations": 1000,
                "min_sharpe": 0.0,
                "min_information_coefficient": 0.0
            },
            "monitoring": {
                "enabled": False,
                "log_dir": "logs/performance",
                "database_path": "monitoring/performance.db"
            },
            "app": {
                "reset": False,
                "default_phase": "both"
            }
        }

    def _process_env_vars(self, config: Any) -> Any:
        """递归处理配置中的环境变量替换"""
        if isinstance(config, dict):
            return {k: self._process_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._process_env_vars(item) for item in config]
        elif isinstance(config, str):
            return self._replace_env_vars(config)
        else:
            return config

    def _replace_env_vars(self, text: str) -> str:
        """替换字符串中的环境变量"""
        def replace_match(match):
            var_expr = match.group(1)
            if ":-" in var_expr:
                var_name, default_value = var_expr.split(":-", 1)
                return os.environ.get(var_name, default_value)
            else:
                return os.environ.get(var_expr, "")
        
        # 匹配 ${VAR_NAME} 或 ${VAR_NAME:-default_value}
        pattern = r'\$\{([^}]+)\}'
        return re.sub(pattern, replace_match, text)

    def get_data_root(self) -> Optional[Path]:
        """获取数据根目录配置"""
        config = self.load_config()
        data_root = config.get("data", {}).get("root")
        return Path(data_root).expanduser() if data_root else None

    def get_database_path(self) -> Path:
        """获取数据库路径配置"""
        config = self.load_config()
        db_path = config.get("database", {}).get("path", str(DEFAULT_DB_PATH))
        return Path(db_path).expanduser()

    def get_log_level(self) -> str:
        """获取日志级别配置"""
        config = self.load_config()
        return config.get("logging", {}).get("level", "INFO")

    def get_cache_ttl(self) -> int:
        """获取缓存TTL配置"""
        config = self.load_config()
        return config.get("data", {}).get("cache_ttl", 300)

    def get_async_batch_size(self) -> int:
        """获取异步批处理大小配置"""
        config = self.load_config()
        return config.get("data", {}).get("async_batch_size", 8)

    def get_parallel_config(self) -> Dict[str, Any]:
        """获取并行处理配置"""
        config = self.load_config()
        return config.get("parallel", {
            "mode": "off",
            "max_workers": None,
            "memory_limit_mb": None
        })

    def get_combiner_config(self) -> CombinerConfig:
        """获取因子组合器配置"""
        config = self.load_config()
        combiner_config = config.get("combiner", {})
        return CombinerConfig(
            top_n=combiner_config.get("top_n", 20),
            max_factors=combiner_config.get("max_factors", 3),
            max_combinations=combiner_config.get("max_combinations", 1000),
            min_sharpe=combiner_config.get("min_sharpe", 0.0),
            min_information_coefficient=combiner_config.get("min_information_coefficient", 0.0)
        )

    def get_monitoring_config(self) -> Optional[MonitorConfig]:
        """获取性能监控配置"""
        config = self.load_config()
        monitoring_config = config.get("monitoring", {})
        
        if not monitoring_config.get("enabled", False):
            return None
            
        return MonitorConfig(
            enabled=monitoring_config.get("enabled", False),
            log_dir=str(Path(monitoring_config.get("log_dir", "logs/performance")).expanduser()),
            database_path=str(Path(monitoring_config.get("database_path", "monitoring/performance.db")).expanduser())
        )

    def get_app_config(self) -> Dict[str, Any]:
        """获取应用配置"""
        config = self.load_config()
        return config.get("app", {
            "reset": False,
            "default_phase": "both"
        })


def create_config_loader(config_path: Optional[Union[str, Path]] = None) -> ConfigLoader:
    """创建配置加载器实例"""
    return ConfigLoader(config_path)
