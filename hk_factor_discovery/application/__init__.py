"""Application layer for orchestrating the discovery workflow."""
from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["AppSettings", "ServiceContainer", "DiscoveryOrchestrator"]


def __getattr__(name: str) -> Any:  # pragma: no cover - thin wrapper
    if name == "AppSettings":
        return import_module("hk_factor_discovery.application.configuration").AppSettings
    if name == "ServiceContainer":
        return import_module("hk_factor_discovery.application.container").ServiceContainer
    if name == "DiscoveryOrchestrator":
        return import_module("hk_factor_discovery.application.services").DiscoveryOrchestrator
    raise AttributeError(name)
