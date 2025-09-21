"""Tests for combination limiting behaviour in MultiFactorCombiner."""

from config import CombinerConfig
from phase2.combiner import MultiFactorCombiner


def _make_factors(count: int):
    return [{"factor": f"f{i}", "timeframe": "1d"} for i in range(count)]


def test_generate_combinations_respects_threshold_without_exceeding():
    config = CombinerConfig(top_n=4, max_factors=3, max_combinations=10)
    combiner = MultiFactorCombiner("0700.HK", {}, config=config)

    combos = combiner.generate_combinations(_make_factors(4))

    assert len(combos) == 10  # C(4,2) + C(4,3)
    assert {item["factor"] for combo in combos for item in combo} == {
        "f0",
        "f1",
        "f2",
        "f3",
    }


def test_generate_combinations_truncates_and_logs_when_threshold_exceeded(caplog):
    config = CombinerConfig(top_n=5, max_factors=3, max_combinations=5)
    combiner = MultiFactorCombiner("0700.HK", {}, config=config)

    with caplog.at_level("WARNING"):
        combos = combiner.generate_combinations(_make_factors(6))

    assert len(combos) == 5
    assert {item["factor"] for combo in combos for item in combo} == {
        "f0",
        "f1",
        "f2",
        "f3",
        "f4",
    }
    warning_messages = [record.message for record in caplog.records]
    assert any("exceeds limit" in message for message in warning_messages)
    assert any("top_n=5" in message for message in warning_messages)
