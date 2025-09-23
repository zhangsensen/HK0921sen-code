"""Data quality validation utilities for factor discovery system."""
from __future__ import annotations

import numpy as np
from typing import Dict, Any, Optional, Union
try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None


class DataQualityValidator:
    """Validate data quality for factor discovery results."""

    # Reasonable bounds for financial metrics
    METRIC_BOUNDS = {
        'sharpe_ratio': (-100.0, 100.0),
        'stability': (-1.0, 1.0),
        'win_rate': (0.0, 1.0),
        'profit_factor': (0.0, 500.0),
        'max_drawdown': (0.0, 1.0),
        'information_coefficient': (-1.0, 1.0),
        'average_information_coefficient': (-1.0, 1.0),
        'trades_count': (0, 10000),
    }

    # Extreme value detection thresholds
    EXTREME_VALUE_THRESHOLDS = {
        'sharpe_ratio': 15.0,
        'profit_factor': 20.0,
        'win_rate': 0.9,
    }

    @classmethod
    def validate_factor_result(cls, result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean a single factor result.

        Args:
            result: Raw factor result dictionary

        Returns:
            Cleaned result with invalid values set to safe defaults
        """
        cleaned = result.copy()
        violations = []

        # Validate each metric
        for metric, value in result.items():
            if metric in cls.METRIC_BOUNDS:
                cleaned_value, violation = cls._validate_metric(metric, value)
                cleaned[metric] = cleaned_value
                if violation:
                    violations.append(violation)

        # Additional cross-metric validation
        cross_violations = cls._validate_cross_metrics(cleaned)
        violations.extend(cross_violations)

        # Add validation metadata
        if violations:
            cleaned['_validation_violations'] = violations
            cleaned['_validation_status'] = 'warning'
        else:
            cleaned['_validation_status'] = 'valid'

        return cleaned

    @classmethod
    def validate_combination_strategy(cls, result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean a combination strategy result.

        Args:
            result: Raw combination strategy result dictionary

        Returns:
            Cleaned result with invalid values set to safe defaults
        """
        cleaned = result.copy()
        violations = []

        # Validate standard metrics
        for metric, value in result.items():
            if metric in cls.METRIC_BOUNDS:
                cleaned_value, violation = cls._validate_metric(metric, value)
                cleaned[metric] = cleaned_value
                if violation:
                    violations.append(violation)

        # Combination-specific validations
        combination_violations = cls._validate_combination_specific(cleaned)
        violations.extend(combination_violations)

        # Add validation metadata
        if violations:
            cleaned['_validation_violations'] = violations
            cleaned['_validation_status'] = 'warning'
        else:
            cleaned['_validation_status'] = 'valid'

        return cleaned

    @classmethod
    def _validate_metric(cls, metric: str, value: Any) -> tuple[Any, Optional[str]]:
        """Validate a single metric value.

        Args:
            metric: Metric name
            value: Metric value

        Returns:
            Tuple of (cleaned_value, violation_message or None)
        """
        if value is None:
            # Set safe defaults for None values
            defaults = {
                'sharpe_ratio': 0.0,
                'stability': 0.0,
                'win_rate': 0.0,
                'profit_factor': 0.0,
                'max_drawdown': 0.0,
                'information_coefficient': 0.0,
                'average_information_coefficient': 0.0,
                'trades_count': 0,
            }
            return defaults.get(metric, 0.0), f"{metric} was None, set to default"

        # Handle special cases
        if metric == 'trades_count':
            try:
                trades = int(value)
                if trades < 0:
                    return 0, f"{metric} was negative ({value}), set to 0"
                if trades > cls.METRIC_BOUNDS[metric][1]:
                    return cls.METRIC_BOUNDS[metric][1], f"{metric} exceeded maximum ({value}), capped"
                return trades, None
            except (ValueError, TypeError):
                return 0, f"{metric} was invalid ({value}), set to 0"

        # Handle numeric metrics
        try:
            numeric_value = float(value)

            # Check for NaN or infinity
            if np.isnan(numeric_value) or np.isinf(numeric_value):
                default_value = 0.0 if metric != 'max_drawdown' else 0.0
                return default_value, f"{metric} was NaN/inf ({value}), set to default"

            # Check bounds (hard safety net)
            min_val, max_val = cls.METRIC_BOUNDS[metric]
            cleaned_value = numeric_value
            violation: Optional[str] = None
            if numeric_value < min_val:
                cleaned_value = min_val
                violation = f"{metric} below minimum ({value}), clipped to {min_val}"
            elif numeric_value > max_val:
                cleaned_value = max_val
                violation = f"{metric} exceeded maximum ({value}), clipped to {max_val}"

            # Advisory thresholds – record warning but retain value
            advisory_violation = None
            if metric in cls.EXTREME_VALUE_THRESHOLDS:
                threshold = cls.EXTREME_VALUE_THRESHOLDS[metric]
                if abs(numeric_value) > threshold:
                    advisory_violation = (
                        f"{metric} beyond advisory threshold ({value} vs {threshold})"
                    )

            final_value = cleaned_value
            messages = []
            if violation:
                messages.append(violation)
            if advisory_violation:
                messages.append(advisory_violation)

            violation_message = "; ".join(messages) if messages else None
            return final_value, violation_message

        except (ValueError, TypeError):
            default_value = 0.0 if metric != 'max_drawdown' else 0.0
            return default_value, f"{metric} was invalid ({value}), set to default"

    @classmethod
    def _validate_cross_metrics(cls, result: Dict[str, Any]) -> list[str]:
        """Validate cross-metric relationships."""
        violations = []

        # Check for impossible combinations
        trades_count = result.get('trades_count', 0)
        win_rate = result.get('win_rate', 0.0)
        profit_factor = result.get('profit_factor', 0.0)

        if trades_count == 0 and win_rate > 0:
            violations.append("win_rate > 0 with trades_count = 0")

        if trades_count == 0 and profit_factor > 0:
            violations.append("profit_factor > 0 with trades_count = 0")

        if win_rate == 0 and profit_factor > 1:
            violations.append("profit_factor > 1 with win_rate = 0")

        return violations

    @classmethod
    def _validate_combination_specific(cls, result: Dict[str, Any]) -> list[str]:
        """Validate combination-specific constraints."""
        violations = []

        # Check factor count
        factors = result.get('factors', [])
        if len(factors) < 2:
            violations.append("combination must have at least 2 factors")

        # Validate timeframe alignment when available
        timeframes = result.get('timeframes', []) or []
        if timeframes and len(timeframes) != len(factors):
            violations.append(
                f"timeframe_count_mismatch({len(timeframes)} vs {len(factors)})"
            )

        # Check that combination name matches factors
        strategy_name = result.get('strategy_name', '')
        if strategy_name and factors:
            expected_name = "+".join(factors)
            if strategy_name != expected_name:
                violations.append(f"strategy_name mismatch: {strategy_name} != {expected_name}")
                result['strategy_name'] = expected_name

        return violations

    @classmethod
    def validate_returns_series(cls, returns: Union[pd.Series, np.ndarray, None]) -> tuple[Union[pd.Series, np.ndarray, None], list[str]]:
        """Validate a returns series for mathematical correctness.

        Args:
            returns: Returns series to validate

        Returns:
            Tuple of (cleaned_returns, violation_messages)
        """
        violations = []

        if returns is None:
            return None, ["returns series is None"]

        # Convert to numpy array for consistent processing
        if pd is not None and isinstance(returns, pd.Series):
            returns_array = returns.to_numpy()
            has_index = True
        else:
            returns_array = np.asarray(returns)
            has_index = False

        # Check array properties
        if returns_array.size == 0:
            return None, ["returns series is empty"]

        # Check for NaN or infinity
        nan_count = np.sum(np.isnan(returns_array))
        inf_count = np.sum(np.isinf(returns_array))

        if nan_count > 0:
            violations.append(f"returns contains {nan_count} NaN values")

        if inf_count > 0:
            violations.append(f"returns contains {inf_count} infinite values")

        # Check for extreme values
        extreme_threshold = 1.0  # 100% return in a single period is suspicious
        extreme_count = np.sum(np.abs(returns_array) > extreme_threshold)
        if extreme_count > 0:
            violations.append(f"returns contains {extreme_count} extreme values (> {extreme_threshold * 100}%)")

        # Clean the returns if needed
        if violations:
            # Replace NaN/inf with 0
            cleaned_array = returns_array.copy()
            cleaned_array[np.isnan(cleaned_array)] = 0.0
            cleaned_array[np.isinf(cleaned_array)] = 0.0

            # Cap extreme values
            cleaned_array = np.clip(cleaned_array, -extreme_threshold, extreme_threshold)

            if has_index and pd is not None:
                cleaned_returns = pd.Series(cleaned_array, index=returns.index)
            else:
                cleaned_returns = cleaned_array

            return cleaned_returns, violations

        return returns, violations

    @classmethod
    def generate_quality_report(cls, results: list[Dict[str, Any]], result_type: str = 'factor') -> Dict[str, Any]:
        """Generate a quality report for a batch of results.

        Args:
            results: List of result dictionaries
            result_type: Type of results ('factor' or 'combination')

        Returns:
            Quality report dictionary
        """
        report = {
            'total_results': len(results),
            'valid_results': 0,
            'warning_results': 0,
            'violations_by_type': {},
            'metric_statistics': {},
        }

        violations_count = {}
        metric_values = {}

        for result in results:
            # Count validation status
            status = result.get('_validation_status', 'unknown')
            if status == 'valid':
                report['valid_results'] += 1
            elif status == 'warning':
                report['warning_results'] += 1

            # Count violations
            violations = result.get('_validation_violations', [])
            for violation in violations:
                violation_type = violation.split(':')[0] if ':' in violation else violation
                violations_count[violation_type] = violations_count.get(violation_type, 0) + 1

            # Collect metric statistics
            for metric, bounds in cls.METRIC_BOUNDS.items():
                if metric in result:
                    if metric not in metric_values:
                        metric_values[metric] = []
                    metric_values[metric].append(result[metric])

        # Calculate violation statistics
        report['violations_by_type'] = violations_count

        # Calculate metric statistics
        for metric, values in metric_values.items():
            if values:
                report['metric_statistics'][metric] = {
                    'min': min(values),
                    'max': max(values),
                    'mean': sum(values) / len(values),
                    'count': len(values),
                }

        return report
