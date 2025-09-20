"""Console entry point compatibility wrapper."""
from __future__ import annotations

from hk_factor_discovery.main import main

__all__ = ["main"]


if __name__ == "__main__":  # pragma: no cover
    main()
