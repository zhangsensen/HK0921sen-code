"""Benchmark the two-stage discovery workflow with monitoring enabled."""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from application.configuration import AppSettings
from application.container import ServiceContainer
from application.services import DiscoveryOrchestrator
from utils.logging import configure
from utils.monitoring.models import MetricCategory
from utils.monitoring.runtime import PerformanceMonitor


@dataclass
class RunResult:
    """Represent the outcome for a single benchmark sample."""

    index: int
    duration: float
    success: bool
    error: str | None = None
    cpu_time: float | None = None
    start_memory_mb: float | None = None
    end_memory_mb: float | None = None


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser, reusing the main discovery options."""

    from main import _build_parser as build_discovery_parser  # Avoid heavy imports at module load

    parser = build_discovery_parser()
    parser.description = (
        "Run multiple discovery workflow samples (phase 1/2) and export monitoring metrics."
    )

    parser.add_argument(
        "--samples",
        type=int,
        default=3,
        help="Number of workflow executions to benchmark (default: 3).",
    )
    parser.add_argument(
        "--export-dir",
        default="runtime/benchmark/exports",
        help="Directory for exported monitoring snapshots (default: runtime/benchmark/exports).",
    )
    parser.add_argument(
        "--export-formats",
        nargs="+",
        choices=["json", "csv"],
        default=["json", "csv"],
        help="Metric export formats produced via PerformanceMonitor.export_metrics (default: json csv).",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Abort on the first failing sample instead of completing all runs.",
    )

    # Force monitoring to be enabled with benchmark-friendly directories by default.
    parser.set_defaults(enable_monitoring=True)
    parser.set_defaults(monitor_log_dir="runtime/benchmark/logs")
    parser.set_defaults(monitor_db_path="runtime/benchmark/monitoring/performance.db")

    return parser


def _average(values: Iterable[float]) -> float | None:
    """Return the arithmetic mean for a non-empty iterable."""

    data = list(values)
    if not data:
        return None
    return statistics.mean(data)


def _memory_usage_mb() -> float | None:
    """Return current RSS in megabytes when possible."""

    try:
        import psutil  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        psutil = None  # type: ignore[assignment]
    if psutil is not None:
        try:
            return psutil.Process().memory_info().rss / (1024 * 1024)
        except Exception:  # pragma: no cover - defensive guard
            pass

    try:
        import resource  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        return None
    except Exception:  # pragma: no cover - defensive guard
        return None

    try:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:  # pragma: no cover - defensive guard
        return None
    if sys.platform == "darwin":
        return usage / (1024 * 1024)
    return usage / 1024


def _peak_memory_value(start: float | None, end: float | None) -> float | None:
    values = [value for value in (start, end) if value is not None]
    return max(values) if values else None


def _format_run_resources(
    cpu_time: float | None, start_memory: float | None, end_memory: float | None
) -> str:
    parts: List[str] = []
    if cpu_time is not None:
        parts.append(f"CPU {cpu_time:.2f}s")
    peak = _peak_memory_value(start_memory, end_memory)
    if peak is not None:
        if start_memory is not None and end_memory is not None:
            delta = end_memory - start_memory
            parts.append(f"RSS {peak:.1f}MB (Δ {delta:+.1f}MB)")
        else:
            parts.append(f"RSS {peak:.1f}MB")
    if not parts:
        return ""
    return " (" + ", ".join(parts) + ")"


def _format_seconds(value: float | None) -> str:
    """Format seconds with a two-decimal representation."""

    if value is None:
        return "n/a"
    return f"{value:.2f}s"


def _ensure_monitor(settings: AppSettings, container: ServiceContainer) -> PerformanceMonitor:
    monitor = container.performance_monitor()
    if monitor is None:
        raise RuntimeError("Benchmarking requires monitoring to be enabled.")
    return monitor


def _run_samples(
    settings: AppSettings,
    container: ServiceContainer,
    samples: int,
    fail_fast: bool,
) -> List[RunResult]:
    results: List[RunResult] = []
    for index in range(1, samples + 1):
        orchestrator = DiscoveryOrchestrator(settings, container)
        start = time.perf_counter()
        start_cpu = time.process_time()
        start_memory = _memory_usage_mb()
        try:
            orchestrator.run()
        except Exception as exc:  # pragma: no cover - surfaced to CLI output
            duration = time.perf_counter() - start
            cpu_time = time.process_time() - start_cpu
            end_memory = _memory_usage_mb()
            message = str(exc)
            results.append(
                RunResult(
                    index=index,
                    duration=duration,
                    success=False,
                    error=message,
                    cpu_time=cpu_time,
                    start_memory_mb=start_memory,
                    end_memory_mb=end_memory,
                )
            )
            resources = _format_run_resources(cpu_time, start_memory, end_memory)
            print(f"[{index}/{samples}] ❌ Failure after {duration:.2f}s{resources}: {message}")
            if fail_fast:
                break
        else:
            duration = time.perf_counter() - start
            cpu_time = time.process_time() - start_cpu
            end_memory = _memory_usage_mb()
            results.append(
                RunResult(
                    index=index,
                    duration=duration,
                    success=True,
                    cpu_time=cpu_time,
                    start_memory_mb=start_memory,
                    end_memory_mb=end_memory,
                )
            )
            resources = _format_run_resources(cpu_time, start_memory, end_memory)
            print(f"[{index}/{samples}] ✅ Completed in {duration:.2f}s{resources}")
    return results


def _summarise_runs(results: Sequence[RunResult]) -> None:
    total = len(results)
    if total == 0:
        print("No benchmark samples were executed.")
        return

    successes = sum(1 for result in results if result.success)
    success_rate = successes / total if total else 0.0
    avg_all = _average(result.duration for result in results)
    avg_success = _average(result.duration for result in results if result.success)

    print("\n=== Benchmark Summary ===")
    print(f"Total runs: {total}")
    print(f"Successful runs: {successes}/{total} ({success_rate:.0%})")
    if avg_all is not None:
        print(f"Average duration (all runs): {_format_seconds(avg_all)}")
    if avg_success is not None:
        print(f"Average duration (successful runs): {_format_seconds(avg_success)}")

    cpu_avg = _average(result.cpu_time for result in results if result.cpu_time is not None)
    if cpu_avg is not None:
        print(f"Average CPU time (per run): {cpu_avg:.2f}s")

    peaks = [
        peak
        for peak in (
            _peak_memory_value(result.start_memory_mb, result.end_memory_mb) for result in results
        )
        if peak is not None
    ]
    if peaks:
        peak_avg = _average(peaks)
        if peak_avg is not None:
            print(f"Average peak RSS: {peak_avg:.1f}MB (max {max(peaks):.1f}MB)")

    deltas = [
        result.end_memory_mb - result.start_memory_mb
        for result in results
        if result.start_memory_mb is not None and result.end_memory_mb is not None
    ]
    if deltas:
        delta_avg = _average(deltas)
        if delta_avg is not None:
            print(
                "Average RSS delta: "
                f"{delta_avg:+.1f}MB (max {max(deltas):+.1f}MB, min {min(deltas):+.1f}MB)"
            )

    failures = [result for result in results if not result.success and result.error]
    if failures:
        print("Failures detected:")
        for failure in failures:
            print(f"  - Run {failure.index}: {failure.error}")


def _summarise_phase_metrics(
    monitor: PerformanceMonitor,
    settings: AppSettings,
    window_start: datetime,
) -> None:
    metrics = monitor.get_metrics(
        start_time=window_start,
        categories=[MetricCategory.OPERATION],
    )

    def metric_average(name: str) -> float | None:
        return _average(
            metric.value
            for metric in metrics
            if metric.name == name and (metric.tags or {}).get("symbol") == settings.symbol
        )

    phase1_avg = metric_average("discovery_phase1_duration")
    phase2_avg = metric_average("discovery_phase2_duration")

    print("\n=== Phase Breakdown ===")
    print(f"Phase 1 average duration: {_format_seconds(phase1_avg)}")
    if settings.phase in {"phase2", "both"}:
        print(f"Phase 2 average duration: {_format_seconds(phase2_avg)}")
    else:
        print("Phase 2 average duration: phase not executed in this benchmark")


def _export_metrics(
    monitor: PerformanceMonitor,
    export_formats: Sequence[str],
    export_dir: str,
    window_start: datetime,
) -> List[str]:
    paths: List[str] = []
    for fmt in export_formats:
        try:
            path = monitor.export_metrics(
                format_type=fmt,
                export_dir=export_dir,
                start_time=window_start,
            )
        except RuntimeError as exc:  # pragma: no cover - depends on optional pandas
            print(f"⚠️  Unable to export metrics as {fmt.upper()}: {exc}")
        else:
            paths.append(path)
    return paths


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.samples <= 0:
        parser.error("--samples must be a positive integer")

    # Ensure monitoring stays enabled even if environment defaults differ.
    setattr(args, "enable_monitoring", True)

    settings = AppSettings.from_cli_args(args)
    configure(settings.log_level)
    container = ServiceContainer(settings)
    monitor = _ensure_monitor(settings, container)

    benchmark_start = datetime.now()
    try:
        results = _run_samples(settings, container, args.samples, args.fail_fast)
        _summarise_runs(results)
        _summarise_phase_metrics(monitor, settings, benchmark_start)

        exported_paths = _export_metrics(
            monitor,
            args.export_formats,
            args.export_dir,
            benchmark_start,
        )
        if exported_paths:
            print("\nMetric exports:")
            for path in exported_paths:
                print(f"  - {path}")

        has_failures = any(not result.success for result in results)
        if not results:
            return 1
        return 0 if not has_failures else 1
    finally:
        monitor.stop()


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
