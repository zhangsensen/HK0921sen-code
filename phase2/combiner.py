"""Multi-factor combination optimizer."""
from __future__ import annotations

from itertools import combinations
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

import numpy as np

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
        timeframes: Optional[Iterable[str]] = None,
        data_loader=None,
    ) -> None:
        self.symbol = symbol
        self.phase1_results = phase1_results
        self.config = config or CombinerConfig()
        self.timeframes = list(timeframes) if timeframes is not None else []
        self.data_loader = data_loader
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

    def generate_combinations(self, factors: Sequence[Mapping[str, object]], max_factors: Optional[int] = None) -> List[Sequence[Mapping[str, object]]]:
        limit = max_factors or self.config.max_factors
        if len(factors) < 2:
            return []

        combos: List[Sequence[Mapping[str, object]]] = []
        for r in range(2, limit + 1):
            combos.extend(combinations(factors, r))
        return combos

    def backtest_combination(self, combo: Sequence[Mapping[str, object]]) -> Dict[str, object]:
        returns_arrays = []
        for factor in combo:
            returns = factor.get("returns")
            if returns is None:
                continue
            returns_arrays.append(np.asarray(returns, dtype=float))
        if not returns_arrays:
            return {}
        min_length = min(len(arr) for arr in returns_arrays)
        aligned = np.array([arr[-min_length:] for arr in returns_arrays])
        combined_returns = aligned.mean(axis=0)

        sharpe = PerformanceMetrics.calculate_sharpe_ratio(combined_returns)
        stability = PerformanceMetrics.calculate_stability(combined_returns)
        gains = combined_returns[combined_returns > 0]
        losses = combined_returns[combined_returns < 0]
        profit_factor = PerformanceMetrics.calculate_profit_factor(gains, losses)
        equity_curve = np.cumprod(1 + combined_returns)
        max_drawdown = PerformanceMetrics.calculate_max_drawdown(equity_curve)
        trades_count = int(np.count_nonzero(np.diff(np.sign(combined_returns))))
        win_rate = float((combined_returns > 0).mean())

        factor_names = [f["factor"] for f in combo]
        timeframes = [f["timeframe"] for f in combo]
        avg_ic = float(np.mean([float(f.get("information_coefficient", 0.0) or 0.0) for f in combo]))
        strategy_name = "+".join(factor_names)
        return {
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
