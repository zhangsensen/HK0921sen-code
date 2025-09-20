"""Runtime configuration models and helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
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
    data_cache_dir: Optional[Path] = field(default_factory=lambda: Path(".cache") / "hk_data")
    loader_max_workers: int = 4
    enable_preload: bool = True
    enable_batch_loading: bool = True
    enable_disk_cache: bool = True

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

        def _env_bool(name: str, default: bool) -> bool:
            value = os.environ.get(name)
            if value is None:
                return default
            return value.strip().lower() not in {"0", "false", "no"}

        cache_dir_env = os.environ.get("HK_DISCOVERY_DATA_CACHE")
        cache_dir = (
            Path(cache_dir_env).expanduser()
            if cache_dir_env
            else Path(".cache") / "hk_data"
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
            data_cache_dir=cache_dir,
            loader_max_workers=int(os.environ.get("HK_DISCOVERY_LOADER_WORKERS", "4")),
            enable_preload=_env_bool("HK_DISCOVERY_ENABLE_PRELOAD", True),
            enable_batch_loading=_env_bool("HK_DISCOVERY_ENABLE_BATCH", True),
            enable_disk_cache=_env_bool("HK_DISCOVERY_ENABLE_DISK_CACHE", True),
        )


__all__ = ["AppSettings"]
