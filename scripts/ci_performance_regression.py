"""CI helper to detect performance regressions using the benchmark tool."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Sequence


def _load_report(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Report not found: {path}") from exc
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Unable to parse report {path}: {exc}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run benchmark_discovery and compare against a baseline report."
    )
    parser.add_argument("--baseline", required=True, help="Path to the baseline JSON report")
    parser.add_argument(
        "--report",
        default="runtime/benchmark/results/latest.json",
        help="Path to write the freshly generated report (default: runtime/benchmark/results/latest.json)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.1,
        help="Maximum allowed slowdown for avg_duration compared to baseline (default: 0.10 = 10%)",
    )
    parser.add_argument(
        "--samples",
        type=int,
        help="Override the number of samples passed to benchmark_discovery.",
    )
    parser.add_argument(
        "--benchmark-args",
        nargs=argparse.REMAINDER,
        help="Additional arguments forwarded to benchmark_discovery (prefix with --benchmark-args -- ...)",
    )
    return parser


def _run_benchmark(report_path: Path, baseline_path: Path, samples: int | None, extra_args: Sequence[str]) -> int:
    command = [
        sys.executable,
        "scripts/benchmark_discovery.py",
        "--report-path",
        str(report_path),
        "--baseline-report",
        str(baseline_path),
    ]
    if samples is not None:
        command += ["--samples", str(samples)]
    if extra_args:
        command += list(extra_args)
    result = subprocess.run(command, check=False)
    return result.returncode


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    baseline_path = Path(args.baseline)
    report_path = Path(args.report)

    # Warm validation: ensure baseline exists before running the benchmark to avoid wasted cycles.
    if not baseline_path.exists():
        parser.error(f"Baseline report not found: {baseline_path}")

    return_code = _run_benchmark(report_path, baseline_path, args.samples, args.benchmark_args or [])
    if return_code != 0:
        return return_code

    baseline_report = _load_report(baseline_path)
    current_report = _load_report(report_path)

    baseline_metrics = baseline_report.get("metrics", {})
    current_metrics = current_report.get("metrics", {})

    baseline_duration = baseline_metrics.get("avg_duration")
    current_duration = current_metrics.get("avg_duration")
    if baseline_duration in (None, 0) or current_duration is None:
        print("⚠️  Unable to evaluate regression due to missing avg_duration metrics.")
        return 0

    delta = current_duration - baseline_duration
    delta_pct = delta / baseline_duration
    direction = "faster" if delta_pct < 0 else "slower"
    print(
        "Average duration: "
        f"{current_duration:.3f}s (baseline {baseline_duration:.3f}s, "
        f"Δ {delta:.3f}s, {delta_pct:+.2%} {direction})"
    )

    if delta_pct > args.threshold:
        print(
            f"❌ Performance regression detected: {delta_pct:+.2%} exceeds threshold {args.threshold:+.2%}."
        )
        return 2

    print("✅ Performance within acceptable threshold.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI helper
    raise SystemExit(main())
