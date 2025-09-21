"""Tests for combination limiting behaviour in MultiFactorCombiner."""

from math import comb

import pytest

pytest.importorskip("numpy")

from config import CombinerConfig
from phase2.combiner import MultiFactorCombiner


def _make_factors(count: int):
    return [
        {
            "factor": f"f{i}",
            "timeframe": "1d",
            "sharpe_ratio": 1.0 - i * 0.01,
            "information_coefficient": 0.1,
        }
        for i in range(count)
    ]


def test_generate_combinations_default_limit_allows_full_space():
    config = CombinerConfig(top_n=4, max_factors=3)
    combiner = MultiFactorCombiner("0700.HK", {}, config=config)

    combos = combiner.generate_combinations(_make_factors(4))

    expected = comb(4, 2) + comb(4, 3)
    assert len(combos) == expected


def test_generate_combinations_handles_exact_threshold():
    config = CombinerConfig(top_n=5, max_factors=3, max_combinations=20)
    combiner = MultiFactorCombiner("0700.HK", {}, config=config)

    combos = combiner.generate_combinations(_make_factors(6))

    expected = comb(5, 2) + comb(5, 3)
    assert len(combos) == expected


def test_generate_combinations_raises_when_limit_exceeded():
    config = CombinerConfig(top_n=6, max_factors=3, max_combinations=15)
    combiner = MultiFactorCombiner("0700.HK", {}, config=config)

    with pytest.raises(ValueError) as excinfo:
        combiner.generate_combinations(_make_factors(6))

    message = str(excinfo.value)
    assert "max_combinations" in message
    assert "top_n=6" in message
    assert "Reduce top_n or max_factors" in message
