# Project Structure

This document summarises the trimmed layout of `hk_factor_discovery` after the clean-up. Each directory now has a single purpose and the runtime-only artefacts have been moved out of the repository.

```
hk_factor_discovery/
├── application/           # Thin orchestration layer (settings, DI container, services)
├── factors/               # Factor registry plus 72 concrete implementations
├── phase1/                # Single-factor exploration and backtest engine
├── phase2/                # Multi-factor combiner and scoring logic
├── utils/                 # Shared helpers (logging, monitoring, validation, cache, metrics)
├── config.py              # Static configuration for supported timeframes
├── data_loader.py         # Historical data loader with smart resampling + caching
├── database.py            # SQLite persistence helpers (results, strategies)
├── main.py                # CLI entry point (parses args, wires orchestrator)
└── tests/                 # Pytest suite covering config, container, loaders and analytics
```

## Module Responsibilities

### `application`
- `configuration.AppSettings` parses CLI arguments and environment overrides.
- `container.ServiceContainer` provides lazy singletons (data loader, backtest engine, database, logger).
- `services.DiscoveryOrchestrator` coordinates the two pipeline stages and persists results.

### `factors`
- `base_factor.FactorCalculator` defines the interface for every factor.
- Each module (trend, momentum, volume, etc.) registers concrete factors via the central registry.

### `phase1`
- `explorer.SingleFactorExplorer` iterates through factors/timeframes and runs backtests.
- `backtest_engine.SimpleBacktestEngine` produces vectorised performance metrics.

### `phase2`
- `combiner.MultiFactorCombiner` selects top factors and evaluates combinations using aggregated returns.

### `utils`
- `logging.get_logger` supplies consistent logging configuration to the application layer.
- `enhanced_logging` structures domain-specific log messages.
- `cache.InMemoryCache` offers a lightweight TTL cache for the data loader.
- `monitoring.PerformanceMonitor` records metrics, alerts and exports.
- `performance_metrics` provides reusable finance statistics helpers.
- `validation` contains input guards such as `validate_symbol`.

### `tests`
- Focus on unit-level coverage for the most critical services (config, DI container, explorers, database, utilities).
- End-to-end behaviour is exercised through the CLI orchestrator in `tests/test_application_services.py`.

## Runtime Output

Generated artefacts (logs, monitoring databases, exports) are no longer committed. The application automatically creates the necessary folders under the working directory when it runs. Refer to `docs/MONITORING_OVERVIEW.md` for guidance on where these files live at runtime and how to clean them safely.
