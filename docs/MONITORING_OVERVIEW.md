# Monitoring Overview

`utils.monitoring.PerformanceMonitor` centralises runtime metrics for factor exploration jobs. The monitor is designed to be embedded by the CLI orchestrator, but can also be used stand-alone in notebooks or scripts.

## Getting Started

```python
from utils.monitoring import PerformanceMonitor, MonitorConfig

config = MonitorConfig(
    log_dir="runtime/logs",
    database_path="runtime/monitoring/performance.db",
    collection_interval_seconds=30,
)
monitor = PerformanceMonitor(config)
```

- Directories referenced in the config are created automatically.
- The monitor collects system snapshots (CPU, memory, disk, network) and stores them in SQLite + JSON files.

## Recording Custom Metrics

```python
from utils.monitoring import MetricCategory, MetricType

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
from utils.monitoring import MetricCategory

start = datetime.utcnow() - timedelta(hours=2)
monitor.export_metrics(
    format_type="json",
    start_time=start,
    categories=[MetricCategory.DATA_LOADING, MetricCategory.OPERATION],
)
```

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

## Integration Tips

- Initialise a single `PerformanceMonitor` and reuse it through dependency injection (`ServiceContainer.logger()` is a good reference pattern).
- Use environment variables (`HK_DISCOVERY_MONITOR_DIR`, etc.) to override defaults for staging/production deployments.
- Combine metrics with the structured logging helpers in `utils.enhanced_logging` to produce consistent audits and performance dashboards.
