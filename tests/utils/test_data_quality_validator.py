import pytest

from utils.data_quality import DataQualityValidator


def test_factor_validator_preserves_extreme_values():
    result = {
        "sharpe_ratio": 18.0,
        "profit_factor": 25.0,
        "win_rate": 0.0,
        "trades_count": 12,
    }
    cleaned = DataQualityValidator.validate_factor_result(result)
    assert cleaned["sharpe_ratio"] == 18.0
    assert cleaned["profit_factor"] == 25.0
    assert cleaned["win_rate"] == 0.0
    assert cleaned["trades_count"] == 12
    violations = cleaned.get("_validation_violations", [])
    assert any("sharpe_ratio" in warning for warning in violations)
    assert any("profit_factor" in warning for warning in violations)


def test_combination_validator_detects_timeframe_mismatch():
    combo = {
        "symbol": "0700.HK",
        "strategy_name": "f1+f2",
        "factors": ["f1", "f2"],
        "timeframes": ["1m"],
        "sharpe_ratio": 2.0,
        "stability": 0.5,
        "trades_count": 10,
        "win_rate": 0.6,
        "profit_factor": 1.2,
        "max_drawdown": 0.2,
        "average_information_coefficient": 0.1,
        "creation_date": "2024-01-01 00:00:00",
    }
    cleaned = DataQualityValidator.validate_combination_strategy(combo)
    assert cleaned["timeframes"] == ["1m"]
    violations = cleaned.get("_validation_violations", [])
    assert any("timeframe_count_mismatch" in warning for warning in violations)
