"""Factor registry and base classes."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List

import numpy as np

try:  # pragma: no cover - optional dependency guard
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled via runtime error
    pd = None

IndicatorFunc = Callable[["pd.DataFrame"], "pd.Series"]


class FactorCalculator(ABC):
    """Abstract factor calculator defining the computation contract."""

    name: str
    category: str

    def prepare_data(self, data: "pd.DataFrame") -> "pd.DataFrame":
        """Override to pre-process raw data before indicator calculation."""

        return data

    @abstractmethod
    def compute_indicator(self, data: "pd.DataFrame") -> "pd.Series":
        raise NotImplementedError

    def generate_signals(self, symbol: str, timeframe: str, data: "pd.DataFrame") -> "pd.Series":
        if pd is None:
            raise ModuleNotFoundError("pandas is required for factor computation")
        prepared = self.prepare_data(data)
        indicator = self.compute_indicator(prepared)
        indicator = indicator.reindex(data.index)
        price = data["close"]
        diff = price - indicator
        signals = np.where(diff > 0, 1, np.where(diff < 0, -1, 0))
        return pd.Series(signals, index=data.index, name=self.name).fillna(0)


@dataclass
class BaseFactor(FactorCalculator):
    name: str
    category: str

    def compute_indicator(self, data: "pd.DataFrame") -> "pd.Series":  # pragma: no cover - interface
        raise NotImplementedError


class GenericFactor(BaseFactor):
    def __init__(self, name: str, category: str, indicator_func: IndicatorFunc):
        super().__init__(name, category)
        self._indicator_func = indicator_func

    def compute_indicator(self, data: "pd.DataFrame") -> "pd.Series":
        return self._indicator_func(data)


class FactorRegistry:
    def __init__(self) -> None:
        self._registry: Dict[str, FactorCalculator] = {}

    def register(self, factor: FactorCalculator) -> None:
        if factor.name in self._registry:
            raise ValueError(f"Factor {factor.name} already registered")
        self._registry[factor.name] = factor

    def get(self, name: str) -> FactorCalculator:
        return self._registry[name]

    def all(self) -> List[FactorCalculator]:
        return list(self._registry.values())

    def names(self) -> Iterable[str]:
        return self._registry.keys()


REGISTRY = FactorRegistry()


def register_factor(name: str, category: str, indicator_func: IndicatorFunc) -> None:
    REGISTRY.register(GenericFactor(name, category, indicator_func))


def all_factors() -> List[FactorCalculator]:
    return REGISTRY.all()
