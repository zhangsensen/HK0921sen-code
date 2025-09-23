"""Multi-factor combination optimizer."""
from __future__ import annotations

from itertools import combinations
from math import comb
from typing import Dict, List, Mapping, Optional, Sequence

import numpy as np

try:  # pragma: no cover - optional dependency guard
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from config import CombinerConfig
from utils.performance_metrics import PerformanceMetrics

class MultiFactorCombiner:
    """Combine top-performing single factors into strategies."""

    def __init__(
        self,
        symbol: str,
        phase1_results: Mapping[str, Mapping[str, object]],
        *,
        config: CombinerConfig | None = None,
    ) -> None:
        self.symbol = symbol
        self.phase1_results = phase1_results
        self.config = config or CombinerConfig()
        self._last_selected_factors: List[Mapping[str, object]] = []

    def select_top_factors(self, top_n: Optional[int] = None) -> List[Mapping[str, object]]:
        top_n = top_n or self.config.top_n
        sortable = []
        for res in self.phase1_results.values():
            sharpe = float(res.get("sharpe_ratio", 0.0) or 0.0)
            information_coefficient = float(res.get("information_coefficient", 0.0) or 0.0)
            if sharpe < self.config.min_sharpe:
                continue
            if abs(information_coefficient) < self.config.min_information_coefficient:
                continue
            sortable.append(res)

        sortable.sort(
            key=lambda r: (
                float(r.get("sharpe_ratio", 0.0) or 0.0),
                abs(float(r.get("information_coefficient", 0.0) or 0.0)),
            ),
            reverse=True,
        )
        return sortable[:top_n]

    def generate_combinations(
        self,
        factors: Sequence[Mapping[str, object]],
        max_factors: Optional[int] = None,
    ) -> List[Sequence[Mapping[str, object]]]:
        if len(factors) < 2:
            return []

        available = min(len(factors), self.config.top_n)
        if available < 2:
            return []

        limit_candidate = max_factors or self.config.max_factors
        limit = min(limit_candidate, available)
        if limit < 2:
            return []

        factors_to_use = list(factors)[:available]
        theoretical_total = sum(comb(available, r) for r in range(2, limit + 1))
        threshold = self.config.max_combinations

        if threshold is not None:
            if threshold <= 0:
                raise ValueError(
                    "max_combinations must be a positive integer; "
                    "consider increasing the limit or adjusting top_n/max_factors."
                )
            if theoretical_total > threshold:
                raise ValueError(
                    (
                        "Requested %s combinations (top_n=%s, max_factors=%s) exceeds "
                        "configured max_combinations=%s. Reduce top_n or max_factors, "
                        "or increase the limit to continue."
                    )
                    % (theoretical_total, available, limit, threshold)
                )

        combos: List[Sequence[Mapping[str, object]]] = []
        for r in range(2, limit + 1):
            combos.extend(combinations(factors_to_use, r))
        return combos

    def backtest_combination(self, combo: Sequence[Mapping[str, object]]) -> Dict[str, object]:
        # Validate combination has sufficient factors
        if len(combo) < 2:
            return {
                "symbol": self.symbol,
                "strategy_name": "invalid_combination",
                "factors": [],
                "timeframes": [],
                "sharpe_ratio": 0.0,
                "stability": 0.0,
                "trades_count": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "max_drawdown": 0.0,
                "average_information_coefficient": 0.0,
                "error": "Insufficient factors for combination",
            }

        series_list = []
        arrays: List[np.ndarray] = []
        factor_names = []
        timeframes = []
        ics = []

        for factor in combo:
            returns = factor.get("returns")
            if returns is None:
                continue

            # Validate factor has sufficient data and trades
            trades_count = factor.get("trades_count", 0)
            if trades_count < 1:  # Reduced threshold to allow more combinations
                continue

            sharpe_ratio = factor.get("sharpe_ratio", 0.0)
            if abs(sharpe_ratio) > 10:  # Filter extreme values
                continue

            if pd is not None:
                if isinstance(returns, pd.Series):
                    series = returns.astype(float)
                else:
                    index_data = factor.get("index")
                    if index_data is None:
                        index_data = factor.get("timestamps")
                    if index_data is not None and not isinstance(index_data, pd.Index):
                        index_data = pd.Index(index_data)
                    if index_data is not None and getattr(index_data.dtype, "kind", None) in {"O", "U", "S"}:
                        try:
                            index_data = pd.to_datetime(index_data)
                        except (TypeError, ValueError):
                            pass
                    series = pd.Series(np.asarray(returns, dtype=float), index=index_data)
                series = series.sort_index()
                series.name = factor.get("factor", series.name)
                series_list.append(series)
            else:  # pragma: no cover - pandas optional fallback
                arrays.append(np.asarray(returns, dtype=float))

            factor_names.append(factor.get("factor", "unknown"))
            timeframes.append(factor.get("timeframe", "unknown"))
            ic = factor.get("information_coefficient", 0.0)
            if ic is not None:
                ics.append(float(ic))

        # Check if we have enough valid factors
        if len(series_list) < 2 and len(arrays) < 2:
            return {
                "symbol": self.symbol,
                "strategy_name": "+".join(factor_names) or "invalid_factors",
                "factors": factor_names,
                "timeframes": timeframes,
                "sharpe_ratio": 0.0,
                "stability": 0.0,
                "trades_count": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "max_drawdown": 0.0,
                "average_information_coefficient": float(np.mean(ics)) if ics else 0.0,
                "error": "Insufficient valid factors after quality filtering",
            }

        combined_series = None
        if series_list:
            combined_frame = pd.concat(series_list, axis=1, join="inner")
            combined_frame = combined_frame.dropna(how="all")
            if combined_frame.empty:
                return {
                    "symbol": self.symbol,
                    "strategy_name": "+".join(factor_names),
                    "factors": factor_names,
                    "timeframes": timeframes,
                    "sharpe_ratio": 0.0,
                    "stability": 0.0,
                    "trades_count": 0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "max_drawdown": 0.0,
                    "average_information_coefficient": float(np.mean(ics)) if ics else 0.0,
                    "error": "No overlapping data periods for combination",
                }
            combined_series = combined_frame.mean(axis=1, skipna=True).astype(float)

            # Validate combined series has sufficient data
            if len(combined_series) < 20:
                return {
                    "symbol": self.symbol,
                    "strategy_name": "+".join(factor_names),
                    "factors": factor_names,
                    "timeframes": timeframes,
                    "sharpe_ratio": 0.0,
                    "stability": 0.0,
                    "trades_count": 0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "max_drawdown": 0.0,
                    "average_information_coefficient": float(np.mean(ics)) if ics else 0.0,
                    "error": f"Insufficient combined data: {len(combined_series)} < 20 required",
                }

            combined_returns = combined_series.to_numpy(dtype=float)
        else:
            if not arrays:
                return {
                    "symbol": self.symbol,
                    "strategy_name": "+".join(factor_names),
                    "factors": factor_names,
                    "timeframes": timeframes,
                    "sharpe_ratio": 0.0,
                    "stability": 0.0,
                    "trades_count": 0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "max_drawdown": 0.0,
                    "average_information_coefficient": float(np.mean(ics)) if ics else 0.0,
                    "error": "No valid return data for combination",
                }
            min_length = min(len(arr) for arr in arrays)
            if min_length < 20:
                return {
                    "symbol": self.symbol,
                    "strategy_name": "+".join(factor_names),
                    "factors": factor_names,
                    "timeframes": timeframes,
                    "sharpe_ratio": 0.0,
                    "stability": 0.0,
                    "trades_count": 0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "max_drawdown": 0.0,
                    "average_information_coefficient": float(np.mean(ics)) if ics else 0.0,
                    "error": f"Insufficient array data: {min_length} < 20 required",
                }
            aligned = np.array([arr[-min_length:] for arr in arrays])
            combined_returns = aligned.mean(axis=0)

        if combined_returns.size == 0:
            return {
                "symbol": self.symbol,
                "strategy_name": "+".join(factor_names),
                "factors": factor_names,
                "timeframes": timeframes,
                "sharpe_ratio": 0.0,
                "stability": 0.0,
                "trades_count": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "max_drawdown": 0.0,
                "average_information_coefficient": float(np.mean(ics)) if ics else 0.0,
                "error": "Empty combined returns",
            }

        # Check for all-zero returns
        if np.all(combined_returns == 0):
            return {
                "symbol": self.symbol,
                "strategy_name": "+".join(factor_names),
                "factors": factor_names,
                "timeframes": timeframes,
                "sharpe_ratio": 0.0,
                "stability": 0.0,
                "trades_count": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "max_drawdown": 0.0,
                "average_information_coefficient": float(np.mean(ics)) if ics else 0.0,
                "error": "Combined strategy produces no trading signals",
            }

        # Calculate performance metrics with validation
        combined_returns_clean = combined_returns.copy()
        combined_returns_clean = combined_returns_clean[~np.isinf(combined_returns_clean) & ~np.isnan(combined_returns_clean)]

        if len(combined_returns_clean) == 0:
            sharpe = 0.0
            stability = 0.0
            profit_factor = 0.0
            win_rate = 0.0
            max_drawdown = 0.0
        else:
            sharpe = PerformanceMetrics.calculate_sharpe_ratio(combined_returns_clean)
            stability = PerformanceMetrics.calculate_stability(combined_returns_clean)

            if combined_series is not None:
                gains = combined_series[combined_series > 0].to_numpy(dtype=float)
                losses = combined_series[combined_series < 0].to_numpy(dtype=float)
                win_rate = float((combined_series > 0).mean())
            else:  # pragma: no cover - pandas optional fallback
                gains = combined_returns_clean[combined_returns_clean > 0]
                losses = combined_returns_clean[combined_returns_clean < 0]
                win_rate = float((combined_returns_clean > 0).mean())
            profit_factor = PerformanceMetrics.calculate_profit_factor(gains, losses)

            # Calculate equity curve using consistent methodology
            equity_curve = np.cumprod(1 + combined_returns_clean)
            max_drawdown = PerformanceMetrics.calculate_max_drawdown(equity_curve)

            # Note: Removed automatic zeroing to prevent double protection
            # Extreme values are now handled by DataQualityValidator during persistence

        # Count trades using consistent methodology
        if combined_series is not None:
            trades_count = int(np.count_nonzero(np.diff(np.sign(combined_series))))
        else:
            trades_count = int(np.count_nonzero(np.diff(np.sign(combined_returns_clean))))

        # Note: Removed automatic zeroing for minimum trades to prevent double protection
        # Trade count validation is now handled by DataQualityValidator during persistence

        avg_ic = float(np.mean(ics)) if ics else 0.0
        strategy_name = "+".join(factor_names)
        result = {
            "symbol": self.symbol,
            "strategy_name": strategy_name,
            "factors": factor_names,
            "timeframes": timeframes,
            "sharpe_ratio": sharpe,
            "stability": stability,
            "trades_count": trades_count,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "max_drawdown": max_drawdown,
            "average_information_coefficient": avg_ic,
        }
        if combined_series is not None:
            result["returns"] = combined_series
        return result

    def discover_strategies(self) -> List[Dict[str, object]]:
        top_factors = self.select_top_factors()
        self._last_selected_factors = top_factors
        combos = self.generate_combinations(top_factors)
        strategies: List[Dict[str, object]] = []
        for combo in combos:
            result = self.backtest_combination(combo)
            if result and result["sharpe_ratio"] >= self.config.min_sharpe:
                strategies.append(result)
        strategies.sort(key=lambda r: r["sharpe_ratio"], reverse=True)
        return strategies

    @property
    def last_selected_factors(self) -> List[Mapping[str, object]]:
        """Expose the most recent factor shortlist for reporting."""

        return self._last_selected_factors
