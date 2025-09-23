import asyncio
import logging
from argparse import Namespace
from pathlib import Path

from application.configuration import AppSettings
from application.container import ServiceContainer
from application.services import DiscoveryOrchestrator
from config import CombinerConfig
from utils.monitoring.config import MonitorConfig
from utils.monitoring.runtime import PerformanceMonitor


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
                "timeframes": ["1m"],
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
    def __init__(self, monitor: PerformanceMonitor | None = None) -> None:
        self.db = StubDatabase()
        self.explorer = StubExplorer()
        self.combiner = StubCombiner()
        self._monitor = monitor

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

    def performance_monitor(self):
        return self._monitor


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


def test_service_container_passes_combiner_config(monkeypatch):
    import sys
    import types

    captured = {}

    class CaptureCombiner:
        def __init__(self, *, symbol, phase1_results, config):
            captured["symbol"] = symbol
            captured["phase1_results"] = phase1_results
            captured["config"] = config
            captured["instance"] = self

    combiner_module = types.ModuleType("phase2.combiner")
    combiner_module.MultiFactorCombiner = CaptureCombiner
    phase2_module = types.ModuleType("phase2")
    phase2_module.combiner = combiner_module
    monkeypatch.setitem(sys.modules, "phase2", phase2_module)
    monkeypatch.setitem(sys.modules, "phase2.combiner", combiner_module)

    settings = AppSettings(
        symbol="0700.HK",
        phase="phase2",
        reset=False,
        data_root=None,
        db_path=Path("/tmp/test.sqlite"),
        combiner=CombinerConfig(top_n=7, max_factors=2, min_sharpe=0.8, min_information_coefficient=0.02),
    )
    container = ServiceContainer(settings)

    def fail_data_loader():
        raise AssertionError("data_loader should not be used when initialising combiner")

    monkeypatch.setattr(container, "data_loader", fail_data_loader)

    phase1_results = {"demo": {"sharpe_ratio": 1.0}}
    combiner = container.factor_combiner(phase1_results)

    assert combiner is captured["instance"]
    assert captured["symbol"] == "0700.HK"
    assert captured["phase1_results"] is phase1_results
    assert captured["config"] == settings.combiner


def test_service_container_uses_appsettings_combiner(monkeypatch, tmp_path):
    import sys
    import types

    captured = {}

    class CaptureCombiner:
        def __init__(self, *, symbol, phase1_results, config):
            captured["symbol"] = symbol
            captured["phase1_results"] = phase1_results
            captured["config"] = config
            captured["instance"] = self

    combiner_module = types.ModuleType("phase2.combiner")
    combiner_module.MultiFactorCombiner = CaptureCombiner
    phase2_module = types.ModuleType("phase2")
    phase2_module.combiner = combiner_module
    monkeypatch.setitem(sys.modules, "phase2", phase2_module)
    monkeypatch.setitem(sys.modules, "phase2.combiner", combiner_module)

    args = Namespace(
        symbol="0700.HK",
        phase="phase2",
        reset=False,
        data_root=str(tmp_path / "dataset"),
        db_path=str(tmp_path / "db.sqlite"),
        log_level="INFO",
        combiner_top_n=11,
        combiner_max_factors=4,
        combiner_min_sharpe=1.05,
        combiner_min_ic=0.06,
        # 设置提供标志，表示这些参数是通过命令行提供的
        _combiner_top_n_provided=True,
        _combiner_max_factors_provided=True,
        _combiner_min_sharpe_provided=True,
        _combiner_min_ic_provided=True,
    )
    settings = AppSettings.from_cli_args(args)
    container = ServiceContainer(settings)

    def fail_data_loader():
        raise AssertionError("data_loader should not be used when initialising combiner")

    monkeypatch.setattr(container, "data_loader", fail_data_loader)

    phase1_results = {"demo": {"sharpe_ratio": 1.0}}
    combiner = container.factor_combiner(phase1_results)

    assert combiner is captured["instance"]
    assert captured["config"] is settings.combiner
    assert captured["config"].top_n == 11
    assert captured["config"].max_factors == 4
    assert captured["config"].min_sharpe == 1.05
    assert captured["config"].min_information_coefficient == 0.06


def test_service_container_supports_combiner_config_alias(monkeypatch):
    import sys
    import types

    captured = {}

    class CaptureCombiner:
        def __init__(self, *, symbol, phase1_results, config):
            captured["symbol"] = symbol
            captured["phase1_results"] = phase1_results
            captured["config"] = config
            captured["instance"] = self

    combiner_module = types.ModuleType("phase2.combiner")
    combiner_module.MultiFactorCombiner = CaptureCombiner
    phase2_module = types.ModuleType("phase2")
    phase2_module.combiner = combiner_module
    monkeypatch.setitem(sys.modules, "phase2", phase2_module)
    monkeypatch.setitem(sys.modules, "phase2.combiner", combiner_module)

    class LegacySettings:
        def __init__(self) -> None:
            self.symbol = "0700.HK"
            self.phase = "phase2"
            self.reset = False
            self.data_root = None
            self.db_path = Path("/tmp/test.sqlite")
            self.log_level = "INFO"
            self.cache_ttl = 300
            self.async_batch_size = 8
            self.parallel_mode = "off"
            self.max_workers = None
            self.memory_limit_mb = None
            self.combiner_config = CombinerConfig(
                top_n=5,
                max_factors=2,
                min_sharpe=0.7,
                min_information_coefficient=0.01,
            )

    container = ServiceContainer(LegacySettings())

    def fail_data_loader():
        raise AssertionError("data_loader should not be used when initialising combiner")

    monkeypatch.setattr(container, "data_loader", fail_data_loader)

    phase1_results = {"demo": {"sharpe_ratio": 1.0}}
    combiner = container.factor_combiner(phase1_results)

    assert combiner is captured["instance"]
    assert captured["symbol"] == "0700.HK"
    assert captured["phase1_results"] is phase1_results
    assert hasattr(container.settings, "combiner")
    assert captured["config"] == container.settings.combiner
    assert captured["config"] == container.settings.combiner_config


def test_service_container_creates_performance_monitor(tmp_path):
    monitor_config = MonitorConfig(
        enabled=False,
        log_dir=str(tmp_path / "logs"),
        database_path=str(tmp_path / "monitor.db"),
        enable_system_metrics=False,
        enable_alerting=False,
    )
    settings = AppSettings(
        symbol="0700.HK",
        phase="phase1",
        reset=False,
        data_root=None,
        db_path=tmp_path / "db.sqlite",
        monitoring=monitor_config,
    )
    container = ServiceContainer(settings)

    monitor = container.performance_monitor()

    assert isinstance(monitor, PerformanceMonitor)
    assert container.performance_monitor() is monitor

    monitor.stop()


def test_orchestrator_records_monitor_metrics(tmp_path):
    monitor_config = MonitorConfig(
        enabled=False,
        log_dir=str(tmp_path / "logs"),
        database_path=str(tmp_path / "monitor.db"),
        enable_system_metrics=False,
        enable_alerting=False,
    )
    settings = AppSettings(
        symbol="0700.HK",
        phase="both",
        reset=False,
        data_root=None,
        db_path=tmp_path / "db.sqlite",
        monitoring=monitor_config,
    )

    monitor = PerformanceMonitor(monitor_config)
    container = StubContainer(monitor=monitor)
    orchestrator = DiscoveryOrchestrator(settings, container)

    result = asyncio.run(orchestrator.run_async())

    stats = monitor.get_operation_statistics()
    assert stats["discovery_phase1"]["count"] == 1
    assert stats["discovery_phase2"]["count"] == 1

    phase1_metric = monitor.metrics_history["discovery_phase1_result_count"][-1]
    assert phase1_metric.value == len(result.phase1)
    assert phase1_metric.tags["symbol"] == "0700.HK"

    phase2_metric = monitor.metrics_history["discovery_phase2_result_count"][-1]
    assert phase2_metric.value == len(result.strategies)
    assert phase2_metric.metadata["executed"] is True

    monitor.stop()
