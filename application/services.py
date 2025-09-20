"""Application services orchestrating the discovery workflow."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List

from ..database import FactorResult
from .configuration import AppSettings
from .container import ServiceContainer


@dataclass
class PhaseResult:
    phase1: Dict[str, Dict[str, object]]
    strategies: List[Dict[str, object]]


class DiscoveryOrchestrator:
    """Coordinate the two stage discovery workflow."""

    def __init__(self, settings: AppSettings, container: ServiceContainer) -> None:
        self.settings = settings
        self.container = container
        self.logger = container.logger()

    # ------------------------------------------------------------------
    async def run_async(self) -> PhaseResult:
        if self.settings.reset:
            self.container.database().reset_database()
            self.logger.info("数据库已重置")

        db = self.container.database()

        phase1_results: Dict[str, Dict[str, object]] = {}
        if self.settings.phase in {"phase1", "both"}:
            self.logger.info("开始阶段1: 单因子探索")
            explorer = self.container.factor_explorer()
            phase1_results = await explorer.explore_all_factors_async(
                batch_size=self.settings.async_batch_size
            )
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            for row in phase1_results.values():
                row["exploration_date"] = timestamp
            db.save_exploration_results(phase1_results.values())
            self.logger.info("完成 %s 个因子探索", len(phase1_results))
        else:
            stored = db.load_exploration_results(self.settings.symbol)
            if not stored:
                raise RuntimeError("没有阶段1结果，请先运行 phase1")
            phase1_results = await self._rehydrate_phase1(stored)

        strategies: List[Dict[str, object]] = []
        if self.settings.phase in {"phase2", "both"}:
            self.logger.info("开始阶段2: 多因子组合")
            combiner = self.container.factor_combiner(phase1_results)
            strategies = combiner.discover_strategies()
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            for strategy in strategies:
                strategy["creation_date"] = timestamp
            db.save_combination_strategies(strategies)
            self.logger.info("完成阶段2组合，发现 %s 个策略", len(strategies))

        return PhaseResult(phase1=phase1_results, strategies=strategies)

    async def _rehydrate_phase1(self, rows: Iterable[FactorResult]) -> Dict[str, Dict[str, object]]:
        explorer = self.container.factor_explorer()
        loader = self.container.data_loader()
        results: Dict[str, Dict[str, object]] = {}
        for row in rows:
            data = loader.load(row.symbol, row.timeframe)
            factor = next((f for f in explorer.factors if f.name == row.factor_name), None)
            if factor is None:
                continue
            key = f"{row.timeframe}_{row.factor_name}"
            results[key] = explorer.explore_single_factor(row.timeframe, factor, data)
        return results

    # ------------------------------------------------------------------
    def run(self) -> PhaseResult:
        return asyncio.run(self.run_async())


__all__ = ["DiscoveryOrchestrator", "PhaseResult"]
