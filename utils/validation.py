"""Validation helpers for user provided input."""
from __future__ import annotations

import re

_SYMBOL_PATTERN = re.compile(r"^[0-9A-Z]{3,5}\.HK$")


def validate_symbol(symbol: str) -> str:
    """Validate HK symbol format (e.g. 0700.HK)."""

    if not isinstance(symbol, str):
        raise TypeError("symbol must be a string")
    candidate = symbol.strip().upper()
    if not _SYMBOL_PATTERN.match(candidate):
        raise ValueError(
            "Invalid symbol format. Expected something like 0700.HK with digits and .HK suffix"
        )
    return candidate


__all__ = ["validate_symbol"]
