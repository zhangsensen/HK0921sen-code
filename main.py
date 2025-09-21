"""Command line entry for the HK factor discovery workflow."""
from __future__ import annotations

import argparse
from typing import Iterable, Sequence

from config import CombinerConfig

try:  # pragma: no cover - optional dependency guard
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled via runtime error
    pd = None

from application.configuration import AppSettings
from application.container import ServiceContainer
from application.services import DiscoveryOrchestrator, PhaseResult
from utils.logging import configure


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="港股因子探索系统")
    combiner_defaults = CombinerConfig()

    class _CombinerOptionAction(argparse.Action):
        """Track whether a combiner option was explicitly provided via CLI."""

        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, values)
            setattr(namespace, f"_{self.dest}_provided", True)

    def _add_combiner_option(
        option: str,
        *,
        dest: str,
        value_type,
        default,
        help_text: str,
    ) -> None:
        flag_name = f"_{dest}_provided"
        parser.set_defaults(**{flag_name: False})
        parser.add_argument(
            option,
            dest=dest,
            type=value_type,
            default=default,
            action=_CombinerOptionAction,
            help=help_text,
        )
    parser.add_argument("--symbol", required=True, help="股票代码，如: 0700.HK")
    parser.add_argument("--phase", choices=["phase1", "phase2", "both"], default="both")
    parser.add_argument("--reset", action="store_true", help="重置数据库")
    parser.add_argument(
        "--data-root",
        help="可选的本地数据目录，支持 symbol/timeframe.parquet 或 timeframe/symbol.parquet 布局",
    )
    parser.add_argument(
        "--db-path",
        help="SQLite 数据库路径；也可通过 HK_DISCOVERY_DB 环境变量配置",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="日志级别 (DEBUG/INFO/WARNING/ERROR)",
    )
    parser.add_argument(
        "--enable-monitoring",
        action="store_true",
        help="启用性能监控（也可通过环境变量 HK_DISCOVERY_MONITORING_ENABLED 控制）",
    )
    parser.add_argument(
        "--monitor-log-dir",
        help="性能监控日志目录（默认: logs/performance）",
    )
    parser.add_argument(
        "--monitor-db-path",
        help="性能监控 SQLite 数据库路径（默认: monitoring/performance.db）",
    )
    parser.add_argument(
        "--parallel-mode",
        choices=["off", "process"],
        default="off",
        help="并行执行模式: off 表示单进程，process 使用多进程",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="并行模式下的最大进程数；默认使用 CPU 核心数减一",
    )
    parser.add_argument(
        "--memory-limit-mb",
        type=int,
        help="内存监控阈值，超过后输出警告信息",
    )
    _add_combiner_option(
        "--combiner-top-n",
        dest="combiner_top_n",
        value_type=int,
        default=combiner_defaults.top_n,
        help_text=(
            "阶段2中纳入多因子组合评估的顶尖单因子数量"
            f"（默认: {combiner_defaults.top_n}；"
            "可通过环境变量 HK_DISCOVERY_COMBINER_TOP_N 配置）"
        ),
    )
    _add_combiner_option(
        "--combiner-max-factors",
        dest="combiner_max_factors",
        value_type=int,
        default=combiner_defaults.max_factors,
        help_text=(
            "组合生成时每个策略允许的最大因子个数"
            f"（默认: {combiner_defaults.max_factors}；"
            "可通过环境变量 HK_DISCOVERY_COMBINER_MAX_FACTORS 配置）"
        ),
    )
    _add_combiner_option(
        "--combiner-min-sharpe",
        dest="combiner_min_sharpe",
        value_type=float,
        default=combiner_defaults.min_sharpe,
        help_text=(
            "筛选阶段2因子及策略时所需的最低夏普比率"
            f"（默认: {combiner_defaults.min_sharpe}；"
            "可通过环境变量 HK_DISCOVERY_COMBINER_MIN_SHARPE 配置）"
        ),
    )
    _add_combiner_option(
        "--combiner-min-ic",
        dest="combiner_min_ic",
        value_type=float,
        default=combiner_defaults.min_information_coefficient,
        help_text=(
            "筛选因子时所需的最小信息系数绝对值"
            f"（默认: {combiner_defaults.min_information_coefficient}；"
            "可通过环境变量 HK_DISCOVERY_COMBINER_MIN_IC 配置）"
        ),
    )
    return parser


def _summarise_phase1(result: PhaseResult) -> str:
    total = len(result.phase1)
    return f"✅ 完成 {total} 个因子探索"


def _summarise_phase2(strategies: Sequence[dict[str, object]]) -> str:
    count = len(strategies)
    return f"✅ 发现 {count} 个优质策略"


def _run_workflow(args) -> PhaseResult:
    settings = AppSettings.from_cli_args(args)
    configure(settings.log_level)
    container = ServiceContainer(settings)
    orchestrator = DiscoveryOrchestrator(settings, container)
    return orchestrator.run()


def main(argv: list[str] | None = None) -> int:
    if pd is None:
        raise ModuleNotFoundError("pandas is required to run the discovery workflow")

    parser = _build_parser()
    args = parser.parse_args(argv)

    result = _run_workflow(args)
    if args.phase in {"phase1", "both"}:
        print(_summarise_phase1(result))
    if args.phase in {"phase2", "both"}:
        print(_summarise_phase2(result.strategies))

    print("🎉 系统运行完成！")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
