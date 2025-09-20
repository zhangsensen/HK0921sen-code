"""Factor package exposing the 72-factor universe."""
from __future__ import annotations

from .base_factor import BaseFactor, FactorCalculator, all_factors

# Import modules for registration side effects.
from . import cycle_factors  # noqa: F401
from . import enhanced_factors  # noqa: F401
from . import microstructure_factors  # noqa: F401
from . import momentum_factors  # noqa: F401
from . import statistical_factors  # noqa: F401
from . import trend_factors  # noqa: F401
from . import volatility_factors  # noqa: F401
from . import volume_factors  # noqa: F401

__all__ = ["BaseFactor", "FactorCalculator", "all_factors"]
