# Monitoring Overview

`utils.monitoring` is now a package that separates configuration, data models and runtime behaviour. Import monitoring primitives from the dedicated modules:

- `utils.monitoring.config` contains `MonitorConfig` and factor-specific templates.
- `utils.monitoring.models` defines enums and dataclasses for metrics and alerts.
- `utils.monitoring.runtime` provides `PerformanceMonitor` and helper utilities.

The package root continues to re-export the most common types for backwards compatibility, so existing imports keep working while new code can rely on the explicit modules.

`utils.monitoring.runtime.PerformanceMonitor` centralises runtime metrics for factor exploration jobs. The monitor is designed to be embedded by the CLI orchestrator, but can also be used stand-alone in notebooks or scripts.

## Getting Started

```python
from utils.monitoring.config import MonitorConfig
from utils.monitoring.runtime import PerformanceMonitor

config = MonitorConfig(
    log_dir="runtime/logs",
    database_path="runtime/monitoring/performance.db",
    collection_interval_seconds=30,
)
monitor = PerformanceMonitor(config)
```

- Directories referenced in the config are created automatically.
- The monitor collects system snapshots (CPU, memory, disk, network) and stores them in SQLite + JSON files.

## Enabling Monitoring in the CLI Workflow

The discovery CLI can now bootstrap a `PerformanceMonitor` directly via command-line flags or environment variables. Pass `--enable-monitoring` when invoking `python -m main` to turn on background collection:

```bash
python -m main \
    --symbol 0700.HK \
    --phase both \
    --enable-monitoring \
    --monitor-log-dir runtime/logs \
    --monitor-db-path runtime/monitoring/performance.db
```

- `--monitor-log-dir` and `--monitor-db-path` override the log and SQLite locations respectively.
- The same values can be supplied via environment variables `HK_DISCOVERY_MONITORING_ENABLED`, `HK_DISCOVERY_MONITOR_LOG_DIR` and `HK_DISCOVERY_MONITOR_DB_PATH`.
- When enabled, `DiscoveryOrchestrator` wraps phase 1 and phase 2 execution with `measure_operation`, generating duration/error counters (`discovery_phase1_duration`, `discovery_phase2_duration`, etc.) and summary gauges (`discovery_phase1_result_count`, `discovery_phase2_result_count`).
- A ready-made smoke test in `tests/e2e/test_cli_smoke.py` exercises this flow end-to-end, asserting both SQLite row counts and the presence of the duration metrics in the monitoring database/JSON artefacts.

## Recording Custom Metrics

```python
from utils.monitoring.models import MetricCategory, MetricType

monitor.record_metric(
    name="factor_load_rows",
    value=12500,
    metric_type=MetricType.GAUGE,
    category=MetricCategory.DATA_LOADING,
    tags={"symbol": "0700.HK", "timeframe": "1m"},
    unit="rows",
)
```

The call queues an asynchronous write to SQLite and a JSON artefact inside `<log_dir>/metrics`.

### Factor Metrics & Alerting

Repeated factor experiments can reuse configuration-driven templates and alerts:

```python
from utils.monitoring.config import (
    FactorAlertDefinition,
    FactorMetricTemplate,
    MonitorConfig,
)
from utils.monitoring.models import AlertSeverity
from utils.monitoring.runtime import PerformanceMonitor

config = MonitorConfig(
    enabled=False,
    log_dir="runtime/logs",
    database_path="runtime/monitoring/performance.db",
    factor_metrics=[
        FactorMetricTemplate(name="sharpe_ratio"),
        FactorMetricTemplate(name="max_drawdown", default_tags={"window": "252d"}),
    ],
    factor_alerts=[
        FactorAlertDefinition(
            name="low_sharpe",
            metric="sharpe_ratio",
            condition="<",
            threshold=0.4,
            severity=AlertSeverity.WARNING,
            message_template="Sharpe below 0.4 for {factor_name}: {value:.2f}",
        )
    ],
)
monitor = PerformanceMonitor(config)

monitor.record_factor_metrics(
    "momentum",
    {"sharpe_ratio": 0.35, "max_drawdown": 0.18},
    extra_tags={"timeframe": "1d"},
)
```

- Metric names are normalised to `factor.<metric>` so alert rules and tools can find them reliably.
- Template tags (for example, the look-back window) merge with runtime tags such as `factor_name`.
- Alerts flow through the existing `_handle_alert` pipeline with tag-aware messages and persisted artefacts.

## Tracking Operations

Use `track_operation` as a context manager when you want execution time and optional metadata captured together.

```python
with monitor.track_operation("data_loading", "0700.HK") as meta:
    df = loader.load("0700.HK", "1m")
    meta.update({"rows": len(df), "timeframe": "1m"})
```

- A `*_time` metric (timer) is always recorded.
- If the yielded metadata dictionary contains entries, a companion `*_metadata` gauge is created with the metadata attached.

For failure cases wrap the work in `measure_operation` (which also records memory deltas) or handle exceptions manually and call `record_metric` with a `MetricType.COUNTER`.

## Exporting Metrics

```python
export_path = monitor.export_metrics(format_type="json", export_dir="./exports")
```

Supported formats:
- `json` (default): serialises each metric as a JSON object.
- `csv`: requires `pandas`; raises an explicit error if the dependency is missing.

You can filter by time range or category:

```python
from datetime import datetime, timedelta
from utils.monitoring.models import MetricCategory

start = datetime.utcnow() - timedelta(hours=2)
monitor.export_metrics(
    format_type="json",
    start_time=start,
    categories=[MetricCategory.DATA_LOADING, MetricCategory.OPERATION],
)
```

## Benchmarking the Discovery Workflow

`scripts/benchmark_discovery.py` automates repeated phase 1/2 runs, keeps monitoring
enabled via `ServiceContainer`, and exports the collected metrics in both JSON and
CSV (when `pandas` is available).

```bash
python scripts/benchmark_discovery.py \
    --symbol 0700.HK \
    --data-root /path/to/data_root \
    --samples 5 \
    --export-dir runtime/benchmark/exports
```

- Monitoring artefacts are written under `runtime/benchmark/logs/` and the SQLite
  store lives at `runtime/benchmark/monitoring/performance.db` by default.
- The CLI prints per-run timings, the overall success rate, and average durations
  for both phases derived from `MetricCategory.OPERATION` metrics.
- Use the exported JSON/CSV files to inspect historical trendlines. For example:

  ```python
  import pandas as pd

  from pathlib import Path

  export_dir = Path("runtime/benchmark/exports")
  latest_csv = max(export_dir.glob("metrics_export_*.csv"), default=None)
  if latest_csv is not None:
      df = pd.read_csv(latest_csv)
      phase1 = df[(df["name"] == "discovery_phase1_duration") & (df["tags"].str.contains("phase\": \"phase1\""))]
      print(phase1["value"].describe())
  ```

- CSV export uses `pandas`; if the dependency is missing the script still writes
  the JSON file and surfaces a warning on stdout.

When analysing the results focus on:

1. The `success_rate` printed by the CLI – anything below 100% requires
   investigation before the next deployment.
2. `discovery_phase1_duration` and `discovery_phase2_duration` metrics – they
   should remain close to the baseline recorded on the bundled mini dataset. A
   sustained increase of more than 20% across multiple samples is a regression
   worth flagging in CI.

## Cleaning Up

At runtime the monitor creates the following tree by default:

```
runtime/
├── logs/
│   ├── metrics/
│   └── alerts/
└── monitoring/
    └── performance.db
```

The repository no longer ships with pre-generated artefacts. Add `runtime/` (or whichever directory you choose) to your deploy-time ignore rules so production instances can rotate logs without polluting the VCS.

### Inspecting Factor Metrics from the CLI

Run `python scripts/factor_metrics.py --hours 12 --metric sharpe_ratio --top 5` to inspect the latest
factor KPIs:

- The CLI groups entries by `factor_name`, prints the freshest value per KPI, and optionally exports the
  same window via `--export json` or `--export csv`.
- Leaderboard mode honours metric names produced by `record_factor_metrics`, so `sharpe_ratio`
  and `factor.sharpe_ratio` are treated identically.

## Integration Tips

- Initialise a single `PerformanceMonitor` and reuse it through dependency injection (`ServiceContainer.logger()` is a good reference pattern).
- Use the `HK_DISCOVERY_MONITORING_ENABLED`, `HK_DISCOVERY_MONITOR_LOG_DIR` and `HK_DISCOVERY_MONITOR_DB_PATH` environment variables to override defaults for staging/production deployments.
- Combine metrics with the structured logging helpers in `utils.enhanced_logging` to produce consistent audits and performance dashboards.
