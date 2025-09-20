"""Logging utilities with consistent formatting."""
from __future__ import annotations

import logging
from typing import Optional

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"


def configure(level: str = "INFO") -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format=_LOG_FORMAT)


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    if level:
        configure(level)
    logger = logging.getLogger(name)
    return logger


__all__ = ["configure", "get_logger"]
