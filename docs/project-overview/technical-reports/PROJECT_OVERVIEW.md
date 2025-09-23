# HK Stock Factor Discovery System - Comprehensive Project Overview

> **Target Audience**: New engineers, quantitative researchers, and stakeholders needing to understand system capabilities.
>
> This document builds upon existing documentation (`README.md`, `docs/PROJECT_STRUCTURE.md`) to describe the system's purpose, objectives, core workflows, key modules, and operational aspects for quick comprehensive understanding.

## 1. Project Purpose and Objectives

### Business Background
- **Market Focus**: Short-term trading strategies for Hong Kong stock market
- **Scale Requirement**: Batch evaluation of 70+ technical/statistical factors
- **Goal**: Combine factors into alpha strategies with superior risk-return profiles

### System Mission
- **Automated Pipeline**: Provide two-phase workflow: "Single-factor exploration → Multi-factor combination"
- **Complete Coverage**: Data loading, backtesting evaluation, result persistence, and monitoring alerts
- **Production Ready**: Robust architecture suitable for research and production environments

### Core Objectives
- **Phase 1**: Vectorized backtesting of 11 timeframes × 72 factor combinations, outputting key metrics (Sharpe, stability, win rate, Information Coefficient)
- **Phase 2**: Filter and backtest multi-factor combinations under constraints (Top N, Sharpe/IC thresholds, maximum factors)
- **Infrastructure**: Full pipeline with **SQLite** persistence, **cache optimization**, **parallel acceleration**, and **performance monitoring**

## 2. Business Workflow Overview

1. **Data Loading**: `data_loader.HistoricalDataLoader` validates paths and loads pre-generated multi-timeframe datasets
2. **Factor Calculation**: 72 factors registered in `factors` module generate signals by timeframe
3. **Backtesting Evaluation (Phase 1)**: `phase1` backtest engine calculates strategy returns, risk metrics, writes to SQLite
4. **Result Caching**: Single/multi-factor results stored in database, `FactorCache` and disk cache accelerate repeated calculations
5. **Multi-factor Combination (Phase 2)**: `phase2.MultiFactorCombiner` filters and combines strategies based on Phase 1 output
6. **Monitoring & Reporting**: `utils.monitoring` provides performance metrics collection, alerts, and export capabilities

## 3. System Architecture Overview

```
┌──────────────────────────────┐
│            CLI / Scripts     │  ← `main.py`, `scripts/*.py`
└──────────────┬───────────────┘
               │
┌──────────────▼───────────────┐
│        Application Layer     │  ← Config parsing, DI container, workflow orchestration
└──────────────┬───────────────┘
               │
      ┌────────┴───────────┐
      │                    │
┌─────▼─────┐       ┌──────▼───────┐
│  phase1   │       │   phase2     │  ← Factor exploration & strategy combination
└─────┬─────┘       └──────┬───────┘
      │                    │
┌─────▼─────┐       ┌──────▼───────┐
│ data_loader│       │  database    │
└─────┬─────┘       └──────┬───────┘
      │                    │
┌─────▼────────────────────▼─────┐
│             utils               │  ← Cache, logging, monitoring, metrics tools
└────────────────────────────────┘
```

- **Application Layer**: CLI parsing (`application/configuration.py`), dependency injection container (`application/container.py`), workflow orchestration (`application/services.py`)
- **Domain Layer**: `phase1`, `phase2`, `factors` modules encapsulate core business logic
- **Infrastructure**: `data_loader.*`, `database.py`, `utils.*` provide reusable cross-cutting capabilities

## 4. 核心模块与关键代码

| 模块 | 主要职责 | 关键实现 |
| --- | --- | --- |
| `application/services.py` | `DiscoveryOrchestrator` 管理 Phase1/2 执行、监控与持久化 | 详见 `run_async` / `_run_phase2` |
| `application/container.py` | 构建 `ServiceContainer`，懒加载数据源、回测引擎、数据库、监控器 | `data_loader()`、`performance_monitor()` |
| `data_loader.py` | 统一数据读取、缓存与输入安全校验，直接载入各时间框架数据 | `_load_raw` |
| `data_loader_optimized.py` | 在并行模式下提供线程池预加载、磁盘缓存（`cache_dir/.optimized_cache`） | `preload_timeframes`、`batch_load` |
| `phase1/backtest_engine.py` | 向量化回测，计算 Sharpe、稳定性、交易成本等指标 | `SimpleBacktestEngine.backtest_factor` |
| `phase1/explorer.py` | 支持批量 Timeframe×Factor 的同步/异步探索，自动调用批量加载接口 | `_batch_load_timeframes`、`explore_all_factors_async` |
| `phase1/parallel_explorer.py` | 基于 `ProcessPoolExecutor` 的并行探索，带 FactorCache & Fallback | `_build_tasks`、`explore_all_factors` |
| `phase2/combiner.py` | 基于 Phase1 结果筛选 Top 因子并生成组合，限制组合总数 | `select_top_factors`、`generate_combinations` |
| `factors/` | 因子注册中心（`base_factor.py`）+ 72 个分类因子实现 | `register_factor`、`all_factors` |
| `database.py` | SQLite Repository + Schema 管理，持久化回测与组合结果 | `FactorRepository`, `StrategyRepository` |
| `utils/monitoring/*` | 性能监控体系：指标采集、告警、导出、上下文管理器 | `PerformanceMonitor`, `MonitorConfig` |
| `utils/cache.py` / `utils/factor_cache.py` | 内存/分布式缓存，支撑 Loader & 并行探索命中率 | `InMemoryCache`, `FactorCache.compute_signature` |

## 5. 关键程序入口

- **主 CLI**：`python -m main` 执行完整流程，可通过参数控制阶段、并行、监控、组合器阈值等。
- **基准测试**：`scripts/benchmark_discovery.py` 多次运行流程并导出监控指标，默认开启监控。
- **性能回归守护**：`scripts/ci_performance_regression.py` 自动调用基准脚本并和基线报告比较，超出阈值即返回非零状态，便于 CI/CD 接入。
- **慢测入口**：`scripts/ci_slow.py` 运行带 `slow` 标记的 pytest 集。
- **监控报表**：`scripts/factor_metrics.py` 读取 `PerformanceMonitor` 指标，对因子表现做窗口统计。

## 6. 数据与存储

- **原始数据**：
  - 支持目录结构 `raw_data/<timeframe>/<symbol>.parquet|csv` 或 `raw_data/<symbol>/<timeframe>.parquet|csv`，亦兼容旧版无 `raw_data/` 前缀的布局。
  - `HistoricalDataLoader` 自动进行路径安全校验并直接加载各时间框架数据，缺失文件会抛出异常。
- **缓存策略**：
  - 内存缓存：`InMemoryCache`（TTL 默认 300s）。
  - 磁盘缓存：并行模式下 `OptimizedDataLoader` 将数据写入 `<data_root>/.optimized_cache/`。
  - 因子结果缓存：`FactorCache` 通过数据签名复用结果，减少重复回测。
- **数据库**：
  - 默认路径 `<repo_root>/benchmark_results/hk_factor_results.sqlite`，可通过 `--db-path` 或 `HK_DISCOVERY_DB` 覆盖。
  - 表结构：`factor_exploration_results` & `combination_strategies`，包含索引与增量升级逻辑。

## 7. 配置与环境变量

- CLI 关键参数（见 `main.py`）：
  - `--symbol`, `--phase`, `--reset`, `--data-root`, `--parallel-mode`, `--max-workers`, `--memory-limit-mb`。
  - 组合器相关：`--combiner-top-n`, `--combiner-max-factors`, `--combiner-min-sharpe`, `--combiner-min-ic`。
  - 监控相关：`--enable-monitoring`, `--monitor-log-dir`, `--monitor-db-path`。
- 主要环境变量：
  - `HK_DISCOVERY_DB`, `HK_DISCOVERY_LOG_LEVEL`, `HK_DISCOVERY_CACHE_TTL`, `HK_DISCOVERY_ASYNC_BATCH`。
  - 组合器阈值：`HK_DISCOVERY_COMBINER_TOP_N`, `HK_DISCOVERY_COMBINER_MAX_FACTORS`, `HK_DISCOVERY_COMBINER_MIN_SHARPE`, `HK_DISCOVERY_COMBINER_MIN_IC`。
  - 监控开关：`HK_DISCOVERY_MONITORING_ENABLED`, `HK_DISCOVERY_MONITOR_LOG_DIR`, `HK_DISCOVERY_MONITOR_DB_PATH`。

## 8. 性能与监控要点

- **并行模式**：`parallel_mode=process` 时，容器返回 `OptimizedDataLoader` 与 `ParallelFactorExplorer`，后者会在 macOS 沙箱无进程池权限时自动降级并给出告警日志。
- **成本模型**：`utils.cost_model.HongKongTradingCosts` 在回测中刻画交易费用。
- **监控栈**：
  - `PerformanceMonitor` 支持系统指标、操作耗时、告警规则、历史导出，默认输出到 `logs/performance`。
  - `application/services.py` 中在 Phase1/2 周期自动记录指标。
  - `scripts/benchmark_discovery.py`、`scripts/factor_metrics.py` 提供结果分析工具。

## 9. 测试与质量保障

- Pytest 用例位于 `tests/`（目前覆盖 28 项），包含：
  - 配置解析 (`test_application_config.py`)
  - DI 容器 (`test_application_container.py`)
  - Orchestrator/监控 (`test_application_services.py`)
  - Loader、缓存、数据库、因子注册、并行探索、组合器等。
- 运行方式：`pytest`（如需慢测：`python scripts/ci_slow.py`）。
- 代码风格遵循 PEP 8，核心模块具备丰富的 docstring 与注释以便理解。

## 10. 目录速查

| 目录 | 说明 |
| --- | --- |
| `application/` | 配置、容器、Orchestrator 实现 |
| `phase1/`, `phase2/` | 因子探索与组合逻辑 |
| `factors/` | 因子注册中心与具体实现 |
| `utils/` | 缓存、日志、监控、绩效度量、验证等工具 |
| `tests/` | 自动化测试集 |
| `docs/` | 结构、监控、测试问题/报告等补充文档 |
| `scripts/` | 基准测试、监控分析、CI 辅助脚本 |
| `runtime/` | 默认的监控输出路径（由脚本生成） |

## 11. 实践建议与下一步思考

- **数据准备**：建议在专用 NAS 或对象存储同步原始数据，并确保路径结构与系统期望一致。
- **资源规划**：并行模式依赖多进程，macOS 开发环境可能触发沙箱限制，可在日志中观察是否自动降级。
- **监控扩展**：`utils/monitoring` 支持自定义告警规则，可在 `MonitorConfig` 中追加业务相关的 Factor 指标模板。
- **后续优化方向**：
  - 为因子/组合结果提供可视化报表或前端。
  - 进一步拆分 Phase2 组合逻辑，支持多线程或 GPU 加速。
  - 引入 Type Checking（mypy）与 Lint（ruff）提升静态质量。

## 12. 面向性能与准确性的优化建议

> 聚焦“性能、准确性、效率”三大目标，按投入成本与收益排序。

- **数据库 I/O 优化（高优先级）**：
  - 在 SQLite 连接初始化时应用 `PRAGMA journal_mode=WAL`、`synchronous=NORMAL`、`cache_size` 等参数（已内建于 `database.SQLiteClient.connect()`）。
  - 搭配 `scripts/benchmark_discovery.py --report-path ... --baseline-report ...` 即可量化 20%~40% 的写入性能改善。
- **数据加载热路径分析**：
  - 结合 `OptimizedDataLoader` 的实时监控指标（`loader.cache_hit_rate` / `loader.disk_cache_rate`），关注命中率低于 60% 的情况。
  - 当记录的命中率过低时，日志与监控 metadata 会标记 `alert=True`，方便告警或 dashboard 过滤。
- **回测准确性提升**：
  - 将 `PerformanceMetrics` 中的风险自由率、交易成本参数对齐实际交易假设，并在配置层暴露。
  - 针对 IC、Sharpe 等指标提供置信区间，增加结果解释力。
- **监控与基准测试协同**：
  - `scripts/benchmark_discovery.py` 支持生成报告、与基线比较并导出 JSON；结合 `scripts/ci_performance_regression.py` 可形成自动化性能守护。
- **任务编排效率**：
  - Phase1 异步路径依赖 CPU-bound 计算，可探索 `ProcessPoolExecutor` 替代 `run_in_executor(None, ...)` 以减轻 GIL 影响（需结合沙箱/部署环境评估）。

---
若需深入了解模块内部实现，可结合 `docs/PROJECT_STRUCTURE.md` 的目录视图与本说明文档快速定位。
