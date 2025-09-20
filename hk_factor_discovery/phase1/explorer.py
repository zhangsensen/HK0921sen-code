"""Single factor exploration workflow."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Mapping, Optional

try:  # pragma: no cover
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover
    pd = None

from ..config import DEFAULT_TIMEFRAMES
from ..data_loader import HistoricalDataLoader
from ..factors import FactorCalculator, all_factors
from .backtest_engine import SimpleBacktestEngine


class SingleFactorExplorer:
    """Explore 72 factors across multiple timeframes."""

    def __init__(
        self,
        symbol: str,
        timeframes: Optional[Iterable[str]] = None,
        factors: Optional[Iterable[FactorCalculator]] = None,
        data_loader: Optional[HistoricalDataLoader] = None,
        backtest_engine: Optional[SimpleBacktestEngine] = None,
    ) -> None:
        if pd is None:
            raise ModuleNotFoundError("pandas is required for factor exploration")
        self.symbol = symbol
        self.timeframes = list(timeframes) if timeframes is not None else list(DEFAULT_TIMEFRAMES)
        self.factors: List[FactorCalculator] = list(factors) if factors is not None else all_factors()
        self.data_loader = data_loader
        if self.data_loader is None:
            raise ValueError("data_loader must be provided for SingleFactorExplorer")
        self.backtest_engine = backtest_engine or SimpleBacktestEngine(symbol)

    def explore_all_factors(self) -> Dict[str, Dict[str, object]]:
        results: Dict[str, Dict[str, object]] = {}
        for timeframe in self.timeframes:
            data = self.data_loader.load(self.symbol, timeframe)
            for factor in self.factors:
                key = f"{timeframe}_{factor.name}"
                results[key] = self.explore_single_factor(timeframe, factor, data)
        return results

    async def explore_all_factors_async(self, batch_size: int = 8) -> Dict[str, Dict[str, object]]:
        results: Dict[str, Dict[str, object]] = {}
        loop = asyncio.get_running_loop()

        for timeframe in self.timeframes:
            data = self.data_loader.load(self.symbol, timeframe)
            semaphore = asyncio.Semaphore(max(1, batch_size))

            async def run_factor(factor: FactorCalculator) -> tuple[str, Dict[str, object]]:
                async with semaphore:
                    result = await loop.run_in_executor(
                        None, self.explore_single_factor, timeframe, factor, data
                    )
                    return f"{timeframe}_{factor.name}", result

            tasks = [asyncio.create_task(run_factor(factor)) for factor in self.factors]
            for key, value in await asyncio.gather(*tasks):
                results[key] = value
        return results

    def explore_single_factor(self, timeframe: str, factor, data: Optional["pd.DataFrame"] = None) -> Dict[str, object]:
        if data is None:
            data = self.data_loader.load(self.symbol, timeframe)
        signals = factor.generate_signals(self.symbol, timeframe, data)
        backtest = self.backtest_engine.backtest_factor(data, signals)
        return {
            "symbol": self.symbol,
            "timeframe": timeframe,
            "factor": factor.name,
            "sharpe_ratio": backtest["sharpe_ratio"],
            "stability": backtest["stability"],
            "trades_count": backtest["trades_count"],
            "win_rate": backtest["win_rate"],
            "profit_factor": backtest["profit_factor"],
            "max_drawdown": backtest["max_drawdown"],
            "information_coefficient": backtest["information_coefficient"],
            "returns": backtest["returns"],
            "equity_curve": backtest["equity_curve"],
            "exploration_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
