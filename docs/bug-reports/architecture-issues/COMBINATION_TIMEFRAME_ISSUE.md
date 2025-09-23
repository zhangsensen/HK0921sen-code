# 组合策略时间框架元数据缺失问题

## 1. 问题描述
- **数据库设计缺陷**：`combination_strategies` 表仅保存 `factor_combination`（无时间框架字段），导致无法区分 `RSI@1m` 与 `RSI@1h`。
- **检索痛点**：
  - 筛选结果时无法判断相同因子来自哪个时间框架。
  - 回测回放、报警定位均缺乏关键维度。
  - 多资产/多频率部署时无法做策略去重或冲突检测。
- **现象确认**：`SELECT symbol,strategy_name,factor_combination FROM combination_strategies LIMIT 10;` 仅返回因子名称数组。

## 2. 架构升级
### 2.1 Schema 变更
- 新增 `timeframes` 列：`database.py:103-125`
  ```sql
  CREATE TABLE IF NOT EXISTS combination_strategies (
      ...
      factor_combination TEXT NOT NULL,
      timeframes TEXT NOT NULL,
      ...
  );
  ```
- 迁移逻辑：`SchemaManager.ensure_schema()` 自动执行 `ALTER TABLE ... ADD COLUMN timeframes TEXT NOT NULL DEFAULT '[]'`（`database.py:137-144`）。

### 2.2 数据持久化链路
- `StrategyRepository.save_many()` 将 `<factor, timeframe>` 成对写入：
  - Factors → `json.dumps(s["factors"])`
  - Timeframes → `json.dumps(s.get("timeframes", []))`（`database.py:246-262`）。
- `StrategyResult` 模型扩展：新增 `timeframes: List[str]` 字段（`database.py:61-70`），`load_by_symbol()` 解析 JSON 并返回结构化结果（`database.py:268-276`）。

### 2.3 上游补充
- `MultiFactorCombiner.backtest_combination()` 已携带 `timeframes` 列表（历史逻辑保留），现可直接落库。
- 数据质量校验新增 `timeframe_count_mismatch` 检查，确保 `<factor, timeframe>` 数量一致（`utils/data_quality.py:203-219`）。

## 3. 对分析与优化的提升
| 场景 | 旧版限制 | 新版能力 |
| --- | --- | --- |
| 策略回放 | 无法定位时间框架 | 直接获取每个因子所属 timeframe，配合 Phase1 结果比对 |
| 冲突检测 | 同名因子混用 | 基于 `(factor, timeframe)` 做唯一性校验 |
| 监控告警 | 难以聚合指标 | 可按 timeframe 聚合 Sharpe / WinRate 等指标 |

## 4. 测试与验证
- 更新 `tests/test_database_manager.py` 确认 timeframes 可读写。
- 更新 `tests/database/test_schema_and_transactions.py` 覆盖表结构、迁移与写入流程。

## 5. 后续规划
1. `StrategyResult` 对外接口支持按 timeframe/因子过滤。
2. Phase2 结果报告中显示 `(factor, timeframe)` 组合，提升可读性。
3. 监控指标新增 `strategy_timeframe` 维度，支撑自动化对比。
