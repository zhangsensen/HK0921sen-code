"""Runtime configuration models and helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, TypeVar

from config import CombinerConfig
from utils.validation import validate_symbol
from utils.monitoring import MonitorConfig


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
        data_root = Path(args.data_root).expanduser() if getattr(args, "data_root", None) else None
        db_override = getattr(args, "db_path", None)
        db_env = os.environ.get("HK_DISCOVERY_DB")
        if db_override:
            db_path = Path(db_override).expanduser()
        elif db_env:
            db_path = Path(db_env).expanduser()
        else:
            db_path = Path(".local_results/hk_factor_results.sqlite")
        symbol = validate_symbol(args.symbol)
        phase = args.phase
        parallel_mode = getattr(args, "parallel_mode", "off")
        max_workers_arg = getattr(args, "max_workers", None)
        max_workers = int(max_workers_arg) if max_workers_arg is not None else None
        memory_limit_arg = getattr(args, "memory_limit_mb", None)
        memory_limit_env = os.environ.get("HK_DISCOVERY_MEMORY_LIMIT_MB")
        memory_limit = (
            int(memory_limit_arg)
            if memory_limit_arg is not None
            else (int(memory_limit_env) if memory_limit_env else None)
        )

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
            resolved_log_dir = Path(monitor_log_dir_value or "logs/performance").expanduser()
            resolved_db_path = Path(monitor_db_path_value or "monitoring/performance.db").expanduser()
            monitoring_config = MonitorConfig(
                enabled=monitoring_enabled,
                log_dir=str(resolved_log_dir),
                database_path=str(resolved_db_path),
            )
        else:
            monitoring_config = None

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

        combiner_config = CombinerConfig(**combiner_kwargs)

        return cls(
            symbol=symbol,
            phase=phase,
            reset=bool(args.reset),
            data_root=data_root,
            db_path=db_path,
            log_level=(getattr(args, "log_level", None) or os.environ.get("HK_DISCOVERY_LOG_LEVEL", "INFO")),
            cache_ttl=int(os.environ.get("HK_DISCOVERY_CACHE_TTL", "300")),
            async_batch_size=int(os.environ.get("HK_DISCOVERY_ASYNC_BATCH", "8")),
            parallel_mode=parallel_mode,
            max_workers=max_workers,
            memory_limit_mb=memory_limit,
            combiner=combiner_config,
            monitoring=monitoring_config,
        )


__all__ = ["AppSettings"]
