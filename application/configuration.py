"""Runtime configuration models and helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, TypeVar

from config import CombinerConfig
from utils.validation import validate_symbol
from utils.monitoring.config import MonitorConfig
from .config_loader import create_config_loader


Numeric = TypeVar("Numeric", int, float)


@dataclass(frozen=True)
class AppSettings:
    """Top level configuration for the discovery workflow."""

    symbol: str
    phase: str
    reset: bool
    data_root: Optional[Path]
    db_path: Path
    log_level: str = "INFO"
    cache_ttl: int = 300
    async_batch_size: int = 8
    parallel_mode: str = "off"
    max_workers: Optional[int] = None
    memory_limit_mb: Optional[int] = None
    combiner: CombinerConfig = field(default_factory=CombinerConfig)
    monitoring: Optional[MonitorConfig] = None

    @staticmethod
    def _to_bool(value: object, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @classmethod
    def from_cli_args(cls, args: object) -> "AppSettings":
        # 创建配置加载器
        config_loader = create_config_loader(getattr(args, "config_file", None))
        
        # 优先级：命令行参数 > 配置文件 > 环境变量 > 默认值
        
        # 数据根目录配置
        data_root = None
        if hasattr(args, "data_root") and getattr(args, "data_root", None):
            data_root = Path(getattr(args, "data_root")).expanduser()
        else:
            data_root = config_loader.get_data_root()
        
        # 数据库路径配置
        db_override = getattr(args, "db_path", None)
        db_env = os.environ.get("HK_DISCOVERY_DB")
        if db_override:
            db_path = Path(db_override)
        elif db_env:
            db_path = Path(db_env)
        else:
            db_path = config_loader.get_database_path()
        db_path = db_path.expanduser().resolve()
        
        symbol = validate_symbol(args.symbol)
        
        # 阶段配置
        phase = getattr(args, "phase", None) or config_loader.get_app_config().get("default_phase", "both")
        
        # 并行处理配置
        parallel_config = config_loader.get_parallel_config()
        parallel_mode = getattr(args, "parallel_mode", None) or parallel_config["mode"]
        max_workers_arg = getattr(args, "max_workers", None)
        max_workers = int(max_workers_arg) if max_workers_arg is not None else parallel_config["max_workers"]
        
        # 内存限制配置
        memory_limit_arg = getattr(args, "memory_limit_mb", None)
        memory_limit_env = os.environ.get("HK_DISCOVERY_MEMORY_LIMIT_MB")
        memory_limit = (
            int(memory_limit_arg)
            if memory_limit_arg is not None
            else (int(memory_limit_env) if memory_limit_env else parallel_config["memory_limit_mb"])
        )

        # 性能监控配置
        monitoring_enabled_arg = getattr(args, "enable_monitoring", False)
        monitoring_enabled_env = cls._to_bool(os.environ.get("HK_DISCOVERY_MONITORING_ENABLED"))
        monitoring_enabled = cls._to_bool(monitoring_enabled_arg) or monitoring_enabled_env

        monitor_log_dir_arg = getattr(args, "monitor_log_dir", None)
        monitor_db_path_arg = getattr(args, "monitor_db_path", None)
        monitor_log_dir_env = os.environ.get("HK_DISCOVERY_MONITOR_LOG_DIR")
        monitor_db_path_env = os.environ.get("HK_DISCOVERY_MONITOR_DB_PATH")

        monitor_log_dir_value = monitor_log_dir_arg or monitor_log_dir_env
        monitor_db_path_value = monitor_db_path_arg or monitor_db_path_env

        monitoring_requested = bool(
            monitoring_enabled
            or monitor_log_dir_value
            or monitor_db_path_value
        )

        monitoring_config: Optional[MonitorConfig]
        if monitoring_requested:
            # 优先使用命令行参数，其次使用配置文件，最后使用默认值
            config_monitoring = config_loader.get_monitoring_config()
            
            if monitor_log_dir_value:
                resolved_log_dir = Path(monitor_log_dir_value).expanduser()
            elif config_monitoring and config_monitoring.log_dir:
                resolved_log_dir = Path(config_monitoring.log_dir).expanduser()
            else:
                resolved_log_dir = Path("logs/performance").expanduser()
                
            if monitor_db_path_value:
                resolved_db_path = Path(monitor_db_path_value).expanduser()
            elif config_monitoring and config_monitoring.database_path:
                resolved_db_path = Path(config_monitoring.database_path).expanduser()
            else:
                resolved_db_path = Path("monitoring/performance.db").expanduser()
            monitoring_config = MonitorConfig(
                enabled=monitoring_enabled,
                log_dir=str(resolved_log_dir),
                database_path=str(resolved_db_path),
            )
        else:
            # 使用配置文件中的监控配置
            monitoring_config = config_loader.get_monitoring_config()

        # 因子组合器配置
        combiner_defaults = CombinerConfig()

        def resolve_combiner_value(
            arg_name: str,
            env_name: str,
            caster: Callable[[object], Numeric],
            default: Numeric,
        ) -> Numeric:
            arg_value = getattr(args, arg_name, None)
            provided_flag = getattr(args, f"_{arg_name}_provided", False)
            has_attribute = hasattr(args, arg_name)
            if provided_flag or (has_attribute and arg_value is not None and arg_value != default):
                return caster(arg_value)
            env_value = os.environ.get(env_name)
            if env_value is not None:
                return caster(env_value)
            return default

        combiner_kwargs = {}
        for arg_name, env_name, caster, field_name in [
            ("combiner_top_n", "HK_DISCOVERY_COMBINER_TOP_N", int, "top_n"),
            ("combiner_max_factors", "HK_DISCOVERY_COMBINER_MAX_FACTORS", int, "max_factors"),
            ("combiner_min_sharpe", "HK_DISCOVERY_COMBINER_MIN_SHARPE", float, "min_sharpe"),
            (
                "combiner_min_ic",
                "HK_DISCOVERY_COMBINER_MIN_IC",
                float,
                "min_information_coefficient",
            ),
        ]:
            default_value = getattr(combiner_defaults, field_name)
            combiner_kwargs[field_name] = resolve_combiner_value(
                arg_name,
                env_name,
                caster,
                default_value,
            )

        # 如果命令行没有提供组合器配置且没有相应环境变量，使用配置文件中的配置
        config_combiner = config_loader.get_combiner_config()
        
        # 只有当命令行参数未提供且环境变量也未设置时，才使用配置文件的值
        env_vars = [
            "HK_DISCOVERY_COMBINER_TOP_N",
            "HK_DISCOVERY_COMBINER_MAX_FACTORS", 
            "HK_DISCOVERY_COMBINER_MIN_SHARPE",
            "HK_DISCOVERY_COMBINER_MIN_IC"
        ]
        arg_names = ["combiner_top_n", "combiner_max_factors", "combiner_min_sharpe", "combiner_min_ic"]
        
        # 检查是否有任何命令行参数或环境变量被设置
        has_cli_or_env = any(
            getattr(args, f"_{arg_name}_provided", False) or os.environ.get(env_name) is not None
            for arg_name, env_name in zip(arg_names, env_vars)
        )
        
        if not has_cli_or_env:
            # 只有在没有任何CLI或环境变量配置时，才使用配置文件
            combiner_kwargs.update({
                "top_n": config_combiner.top_n,
                "max_factors": config_combiner.max_factors,
                "max_combinations": config_combiner.max_combinations,
                "min_sharpe": config_combiner.min_sharpe,
                "min_information_coefficient": config_combiner.min_information_coefficient
            })
        else:
            # 如果有CLI或环境变量，只更新max_combinations（因为它没有环境变量）
            combiner_kwargs["max_combinations"] = config_combiner.max_combinations

        combiner_config = CombinerConfig(**combiner_kwargs)

        # 应用配置
        app_config = config_loader.get_app_config()
        reset = getattr(args, "reset", None) if hasattr(args, "reset") else app_config.get("reset", False)

        return cls(
            symbol=symbol,
            phase=phase,
            reset=bool(reset),
            data_root=data_root,
            db_path=db_path,
            log_level=(getattr(args, "log_level", None) or config_loader.get_log_level()),
            cache_ttl=config_loader.get_cache_ttl(),
            async_batch_size=config_loader.get_async_batch_size(),
            parallel_mode=parallel_mode,
            max_workers=max_workers,
            memory_limit_mb=memory_limit,
            combiner=combiner_config,
            monitoring=monitoring_config,
        )


__all__ = ["AppSettings"]
