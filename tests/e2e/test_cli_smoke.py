"""End-to-end smoke test invoking the CLI workflow with sample data."""
from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from config import DEFAULT_TIMEFRAMES
from factors import all_factors


@pytest.mark.slow
@pytest.mark.parametrize("symbol", ["0700.HK"])
def test_cli_smoke_runs_full_workflow(tmp_path: Path, symbol: str) -> None:
    """Execute the CLI workflow and verify database + monitoring artefacts."""

    repo_root = Path(__file__).resolve().parents[2]
    sample_data_root = repo_root / "tests" / "e2e" / "data"
    runtime_data_root = tmp_path / "data"
    shutil.copytree(sample_data_root, runtime_data_root)

    for timeframe in ("1m", "1d"):
        csv_file = runtime_data_root / "raw_data" / timeframe / f"{symbol}.csv"
        if not csv_file.exists():
            continue
        dataframe = pd.read_csv(csv_file, parse_dates=["timestamp"])
        dataframe = dataframe.set_index("timestamp").sort_index()
        parquet_path = csv_file.with_suffix(".parquet")
        dataframe.to_parquet(parquet_path)
        csv_file.unlink()

    db_path = tmp_path / "results.sqlite"
    monitor_db_path = tmp_path / "monitoring.sqlite"
    monitor_log_dir = tmp_path / "monitor_logs"

    command = [
        sys.executable,
        "-m",
        "main",
        "--symbol",
        symbol,
        "--phase",
        "both",
        "--reset",
        "--enable-monitoring",
        "--data-root",
        str(runtime_data_root),
        "--db-path",
        str(db_path),
        "--monitor-log-dir",
        str(monitor_log_dir),
        "--monitor-db-path",
        str(monitor_db_path),
        "--combiner-top-n",
        "3",
        "--combiner-max-factors",
        "2",
    ]

    process = subprocess.run(
        command,
        cwd=repo_root,
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=True,
    )

    assert "系统运行完成" in process.stdout

    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM factor_exploration_results")
        (phase1_count,) = cursor.fetchone()
        expected_phase1 = len(DEFAULT_TIMEFRAMES) * len(all_factors())
        assert phase1_count == expected_phase1

        cursor.execute("SELECT COUNT(*) FROM combination_strategies")
        (phase2_count,) = cursor.fetchone()
        assert phase2_count > 0

    with sqlite3.connect(monitor_db_path) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT name FROM metrics")
        metric_names = {row[0] for row in cursor.fetchall()}

    assert {"discovery_phase1_duration", "discovery_phase2_duration"}.issubset(metric_names)

    phase1_metric_files = list((monitor_log_dir / "metrics").glob("metric_discovery_phase1_duration_*.json"))
    phase2_metric_files = list((monitor_log_dir / "metrics").glob("metric_discovery_phase2_duration_*.json"))
    assert phase1_metric_files, "missing phase1 duration JSON metrics"
    assert phase2_metric_files, "missing phase2 duration JSON metrics"
