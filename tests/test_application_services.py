import asyncio
import logging
from pathlib import Path

from hk_factor_discovery.application.configuration import AppSettings
from hk_factor_discovery.application.services import DiscoveryOrchestrator


class StubDatabase:
    def __init__(self) -> None:
        self.reset_called = False
        self.exploration_saved = []
        self.combinations_saved = []

    def reset_database(self) -> None:
        self.reset_called = True

    def save_exploration_results(self, results):
        self.exploration_saved = list(results)

    def load_exploration_results(self, symbol: str):
        return []

    def save_combination_strategies(self, strategies):
        self.combinations_saved = list(strategies)

    def load_combination_strategies(self, symbol: str):
        return []


class StubExplorer:
    def __init__(self) -> None:
        self.factors = []

    async def explore_all_factors_async(self, batch_size: int = 1):
        return {
            "1m_demo": {
                "symbol": "0700.HK",
                "timeframe": "1m",
                "factor": "demo",
                "sharpe_ratio": 1.0,
                "stability": 0.5,
                "trades_count": 5,
                "win_rate": 0.6,
                "profit_factor": 1.1,
                "max_drawdown": 0.1,
                "information_coefficient": 0.05,
                "exploration_date": "2024-01-01 00:00:00",
            }
        }

    def explore_single_factor(self, timeframe, factor, data=None):  # pragma: no cover - unused
        return {}


class StubCombiner:
    def discover_strategies(self):
        return [
            {
                "symbol": "0700.HK",
                "strategy_name": "demo",
                "factors": ["demo"],
                "sharpe_ratio": 1.0,
                "stability": 0.5,
                "trades_count": 1,
                "win_rate": 0.6,
                "profit_factor": 1.2,
                "max_drawdown": 0.1,
                "average_information_coefficient": 0.04,
            }
        ]


class StubContainer:
    def __init__(self) -> None:
        self.db = StubDatabase()
        self.explorer = StubExplorer()
        self.combiner = StubCombiner()

    def database(self):
        return self.db

    def factor_explorer(self):
        return self.explorer

    def factor_combiner(self, phase1_results):
        return self.combiner

    def data_loader(self):  # pragma: no cover - unused
        raise AssertionError("data_loader should not be used in this scenario")

    def logger(self):
        return logging.getLogger("test")


def test_orchestrator_runs_both_phases():
    settings = AppSettings(
        symbol="0700.HK",
        phase="both",
        reset=True,
        data_root=None,
        db_path=Path("/tmp/test.sqlite"),
    )
    container = StubContainer()
    orchestrator = DiscoveryOrchestrator(settings, container)
    result = asyncio.run(orchestrator.run_async())
    assert container.db.reset_called is True
    assert result.phase1
    assert container.db.exploration_saved
    assert container.db.combinations_saved
