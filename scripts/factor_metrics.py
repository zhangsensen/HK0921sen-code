"""CLI helper for inspecting factor metrics recorded by PerformanceMonitor."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple

from utils.monitoring import MetricCategory, get_performance_monitor


def _normalise_metric_name(name: str) -> str:
    if name.startswith("factor."):
        return name
    return f"factor.{name}"


def _metric_basename(name: str) -> str:
    return name.split(".", 1)[-1]


def _group_by_factor(metrics: Iterable) -> Dict[str, Dict[str, object]]:
    grouped: Dict[str, Dict[str, object]] = {}
    for metric in metrics:
        factor = metric.tags.get("factor_name", "unknown") if metric.tags else "unknown"
        entry = grouped.setdefault(
            factor,
            {"samples": 0, "latest": {}, "timestamps": []},
        )
        entry["samples"] = int(entry["samples"]) + 1
        entry["timestamps"].append(metric.timestamp)

        base_name = _metric_basename(metric.name)
        latest = entry["latest"]
        stored = latest.get(base_name)
        if not stored or stored[0] < metric.timestamp:
            latest[base_name] = (metric.timestamp, metric.value)

    return grouped


def _build_scoreboard(
    grouped: Dict[str, Dict[str, object]],
    metric_name: str,
) -> List[Tuple[str, float]]:
    baseline = _metric_basename(metric_name)
    scores: List[Tuple[str, float]] = []
    for factor, payload in grouped.items():
        latest = payload["latest"]  # type: ignore[assignment]
        if baseline in latest:
            scores.append((factor, float(latest[baseline][1])))
    return sorted(scores, key=lambda item: item[1], reverse=True)


def _format_factor_line(factor: str, payload: Dict[str, object]) -> str:
    samples = payload["samples"]
    latest = payload["latest"]  # type: ignore[assignment]
    metrics_repr = ", ".join(
        f"{name}={value[1]:.4f}" if isinstance(value[1], float) else f"{name}={value[1]}"
        for name, value in sorted(latest.items())
    )
    return f"- {factor} ({samples} samples): {metrics_repr}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect factor metrics recorded by the monitoring stack")
    parser.add_argument("--hours", type=int, default=24, help="Time window to inspect (default: 24 hours)")
    parser.add_argument("--metric", help="Filter to a specific metric name (e.g. sharpe_ratio)")
    parser.add_argument("--top", type=int, help="Only display the top N factors when --metric is provided")
    parser.add_argument(
        "--export",
        choices=["json", "csv"],
        help="Optional export format for the same metric window",
    )
    parser.add_argument(
        "--export-dir",
        default="exports",
        help="Directory for exported metric dumps (default: exports)",
    )
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    monitor = get_performance_monitor()
    current_time = datetime.now(timezone.utc).replace(tzinfo=None)
    start_time = current_time - timedelta(hours=args.hours)
    metrics = monitor.get_metrics(
        start_time=start_time,
        categories=[MetricCategory.FACTOR_COMPUTATION],
    )

    if args.metric:
        target = _normalise_metric_name(args.metric)
        metrics = [metric for metric in metrics if metric.name == target]
    grouped = _group_by_factor(metrics)

    print(f"=== Factor metrics in the last {args.hours}h ===")
    if not grouped:
        print("No factor metrics were recorded in the selected window.")
    else:
        for factor, payload in sorted(grouped.items()):
            print(_format_factor_line(factor, payload))

    if args.metric and grouped:
        scoreboard = _build_scoreboard(grouped, _normalise_metric_name(args.metric))
        if args.top is not None:
            scoreboard = scoreboard[: args.top]
        print("\n=== Leaderboard ===")
        if not scoreboard:
            print(f"No factors reported the metric '{args.metric}'.")
        else:
            for rank, (factor, score) in enumerate(scoreboard, start=1):
                print(f"{rank:>2}. {factor}: {score:.4f}")

    if args.export:
        export_path = monitor.export_metrics(
            format_type=args.export,
            export_dir=args.export_dir,
            start_time=start_time,
            categories=[MetricCategory.FACTOR_COMPUTATION],
        )
        print(f"\nMetrics exported to {export_path}")

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
