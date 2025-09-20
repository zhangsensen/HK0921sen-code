"""Runtime configuration models and helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from ..utils.validation import validate_symbol


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
        )


__all__ = ["AppSettings"]
