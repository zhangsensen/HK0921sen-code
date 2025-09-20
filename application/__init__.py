"""Compatibility layer for legacy application imports."""
from __future__ import annotations

from importlib import import_module
from typing import Any, Dict, Tuple

__all__ = ["AppSettings", "ServiceContainer", "DiscoveryOrchestrator"]

_LOOKUP: Dict[str, Tuple[str, str]] = {
    "AppSettings": ("hk_factor_discovery.application.configuration", "AppSettings"),
    "ServiceContainer": ("hk_factor_discovery.application.container", "ServiceContainer"),
    "DiscoveryOrchestrator": ("hk_factor_discovery.application.services", "DiscoveryOrchestrator"),
}


def __getattr__(name: str) -> Any:  # pragma: no cover
    try:
        module_name, attr_name = _LOOKUP[name]
    except KeyError as exc:  # pragma: no cover
        raise AttributeError(name) from exc
    module = import_module(module_name)
    return getattr(module, attr_name)


__all__ = sorted(__all__)
