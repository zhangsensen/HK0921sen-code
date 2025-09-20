"""Command line entry for the HK factor discovery workflow."""
from __future__ import annotations

import argparse
from typing import Iterable, Sequence

try:  # pragma: no cover - optional dependency guard
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - handled via runtime error
    pd = None

from .application import AppSettings, DiscoveryOrchestrator, ServiceContainer
from .application.services import PhaseResult
from .utils.logging import configure


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="港股因子探索系统")
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
