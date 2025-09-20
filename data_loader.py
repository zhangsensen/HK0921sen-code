"""Re-export HistoricalDataLoader for backwards compatibility."""
from __future__ import annotations

from hk_factor_discovery.data_loader import HistoricalDataLoader
from hk_factor_discovery.data_loader_optimized import OptimizedDataLoader

__all__ = ["HistoricalDataLoader", "OptimizedDataLoader"]