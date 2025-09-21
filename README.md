# 港股短线因子筛选系统

一个完整的香港股票市场短线交易因子发现和分析系统，采用两阶段向量化策略发现流程：

1. **阶段一 – 单因子探索**：遍历 72 个技术/统计因子与 11 个时间框架（1m→1d）的组合，利用完全向量化的回测引擎评估夏普率、稳定性、胜率等关键指标，并把结果写入 SQLite 数据库。
2. **阶段二 – 多因子组合**：先按夏普率与信息系数（IC）双指标为 792 个因子-时间框架组合打分，自动筛出全市场 Top20 领先因子，再基于它们生成 2~3 个因子的组合，计算组合收益序列并输出排序后的最优策略列表，同样支持数据库留存与复用。

## 仓库结构

```
HK0920sen-code/
├── application/                        # 依赖注入容器与工作流编排服务
│   ├── __init__.py                     # 聚合 AppSettings / ServiceContainer / Orchestrator
│   ├── configuration.py                # AppSettings，集中管理 CLI + 环境变量配置
│   ├── container.py                    # ServiceContainer，实现按需实例化与缓存
│   └── services.py                     # DiscoveryOrchestrator，串联两阶段探索
├── config.py                           # 时间框架等核心配置
├── data_loader.py                      # 支持缓存、流式批处理的历史数据加载器
├── data_loader_optimized.py            # 进程池友好的优化版数据加载器
├── database.py                         # Repository + SchemaManager 持久化层
├── factors/                            # 因子基类、注册中心与全部具体实现
├── main.py                             # CLI 入口，依赖注入启动流程
├── phase1/                             # 单因子探索器与回测引擎、并行探索器
├── phase2/                             # 多因子组合与优化逻辑
├── utils/                              # 缓存、日志、监控、校验等通用工具
└── tests/                              # Pytest 用例，覆盖配置、容器、缓存等关键模块
```

## 项目特色

- **分层架构**：`application` 层负责编排，`database` 层通过 Repository 模式解耦 SQLite 操作，易于替换底层存储或扩展为微服务。
- **依赖注入**：`ServiceContainer` 提供懒加载的单例依赖（数据加载、回测引擎、数据库等），可在测试中轻松注入替身或打桩，提高可维护性。
- **统一因子抽象**：`FactorCalculator` 抽象基类规范计算流程，使新增因子只需实现指标函数即可被注册与调用，避免跨文件修改。
- **性能优化**：历史数据层引入 TTL 缓存与批流式处理；数据库层自动维护索引；探索器支持 `asyncio` 并发执行，显著降低 CPU 与 IO 的等待时间。
- **安全防护**：输入通过 `validate_symbol` 严格校验，数据库层校验 SQL 标识符并采用参数化语句；配置可由环境变量覆盖，避免硬编码敏感信息。
- **工程化支持**：内置日志框架、可插拔缓存、性能监控器以及覆盖核心路径的自动化测试。

## 技术栈

- **核心库**: Python 3.10+, NumPy, Pandas
- **数据库**: SQLite
- **测试**: Pytest
- **数据处理**: Parquet/CSV 支持

## 快速开始

1. **安装依赖**（推荐 Python 3.10+）
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install numpy pandas
   ```

2. **准备数据目录**
   - 将 `1m`、`2m`、`3m`、`5m`、`1d` 的 OHLCV Parquet/CSV 数据放在 `symbol/timeframe.parquet` 或 `timeframe/symbol.parquet` 结构下，二者皆受支持。
   - 若使用 CSV，请包含可解析为时间索引的 `timestamp`（或 `datetime`/`date`）列，并提供 `open`、`high`、`low`、`close`、`volume` 字段。
   - 其余时间框架（10m、15m、30m、1h、2h、4h）由 `HistoricalDataLoader` 自动进行向量化重采样，无需额外文件。
   - 仓库在 `tests/e2e/data/` 下提供了一个涵盖多时间框架的迷你样本，可直接用于本地或 CI 冒烟测试。

3. **运行命令行入口**
   ```bash
   python -m main \
       --symbol 0700.HK \
       --data-root /path/to/data_root \
       --db-path .local_results/hk_factor_results.sqlite \
       --log-level INFO
   ```
   - `--phase phase1|phase2|both` 控制执行阶段，默认 `both`。
   - `--reset` 会清空 SQLite 数据库重新探索。
   - `--log-level` 指定日志级别；亦可通过环境变量 `HK_DISCOVERY_LOG_LEVEL` 配置。
   - `--enable-monitoring` 可启用性能监控，`--monitor-log-dir` 与 `--monitor-db-path` 用于覆盖监控日志/数据库目录；同名环境变量 `HK_DISCOVERY_MONITORING_ENABLED`、`HK_DISCOVERY_MONITOR_LOG_DIR`、`HK_DISCOVERY_MONITOR_DB_PATH` 亦可独立配置。
   - `HK_DISCOVERY_DB`、`HK_DISCOVERY_CACHE_TTL` 等环境变量可覆盖数据库位置与缓存策略。

## 编程接口

```python
from data_loader import HistoricalDataLoader
from factors import all_factors
from phase1 import SingleFactorExplorer
from phase2 import MultiFactorCombiner

loader = HistoricalDataLoader(data_provider=my_provider)
factors = all_factors()
explorer = SingleFactorExplorer("0700.HK", data_loader=loader, factors=factors)
phase1_results = explorer.explore_all_factors()

combiner = MultiFactorCombiner("0700.HK", phase1_results)
strategies = combiner.discover_strategies()
print(strategies[0]["strategy_name"], strategies[0]["sharpe_ratio"])
```

## 测试

仓库包含覆盖因子注册、探索器、组合器与数据库读写的 Pytest 测试：

```bash
pytest
```

若环境暂时未安装 `pandas` 或 `numpy`，探索相关的测试会自动跳过；其余针对配置、容器与安全性的用例仍会执行，确保核心工程逻辑稳定。建议在准备好全部依赖后完整运行一次以验证因子池与组合逻辑。

### 端到端冒烟测试（慢速分组）

`tests/e2e/test_cli_smoke.py` 会通过 `subprocess` 调用 `python -m main --phase both --enable-monitoring`，并对 SQLite 数据库与监控指标进行断言。该用例被标记为 `slow`，默认不会在常规 `pytest` 中执行，可通过下列命令运行：

```bash
python scripts/ci_slow.py  # 等价于 pytest -m slow
```

CI 可复用该脚本，也可以直接执行 `pytest -m slow` 将冒烟任务归入慢速分组。

## 性能监控与基准

- 监控栈现以子包形式提供：`utils.monitoring.config`（配置）、`utils.monitoring.models`（枚举与数据模型）以及 `utils.monitoring.runtime`（运行时与上下文管理器）。顶层 `utils.monitoring` 仍旧重导出常用符号，旧代码可以逐步迁移至更清晰的模块路径。
- `python scripts/benchmark_discovery.py` 会在启用 `PerformanceMonitor` 的前提下重复运行阶段一/二，默认采集 3 次样本，并将
  `MetricCategory.OPERATION` 指标导出到 `runtime/benchmark/exports/`（JSON 必定生成，CSV 依赖 `pandas`）。
- CLI 输出包含每次执行耗时、总体成功率以及阶段级别的平均耗时。导出的 JSON/CSV 可以配合 `pandas` 做进一步分析，重点关注
  `discovery_phase1_duration` 和 `discovery_phase2_duration` 指标。
- 推荐在每周的 CI 定时任务或发布前的性能回归检查中运行该脚本一次，使用仓库附带的迷你样本数据集衡量波动。
- 目标阈值：成功率保持 100%，`discovery_phase1_duration` 均值不高于 90 秒、`discovery_phase2_duration` 均值不高于 30 秒
  （如超过基线 20% 需记录告警并排查）。

## 文档

- [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md) 概述模块职责与目录划分
- [docs/MONITORING_OVERVIEW.md](docs/MONITORING_OVERVIEW.md) 介绍性能监控的使用方式与运行目录建议

## 贡献指南

欢迎提交 Issue 和 Pull Request 来改进系统。请确保：

1. 代码符合 PEP 8 标准
2. 添加适当的测试用例
3. 更新相关文档
4. 确保所有测试通过

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 联系方式

如有问题或建议，请通过 GitHub Issues 联系。

## 结果同步（S3）

云端任务完成后，可执行 `scripts/sync_factor_results.py` 将最新的 SQLite 结果文件拉取到本地：

```bash
pip install boto3  # 若尚未安装
python scripts/sync_factor_results.py --bucket quant-results --prefix factor_discovery/ --dest .local_results --latest-only
```

- `--symbol 0700.HK` 可仅下载指定标的的结果。
- `--latest-only` 会针对每个标的仅保留时间戳最新的数据库文件。
- 下载完成后，将 `DatabaseManager` 的路径指向本地 `.local_results/*.sqlite` 文件，即可在本地读取云端因子探索结果。
