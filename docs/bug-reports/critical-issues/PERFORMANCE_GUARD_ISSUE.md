# 性能指标保护器问题分析（Phase 1 回测）

## 1. 问题概述
- **定位**：`phase1/backtest_engine.py:109-115`
- **历史行为**：当 `abs(sharpe) > 10` *或* `trades.size < 5` 时，直接把 Sharpe、Stability、WinRate、ProfitFactor、MaxDrawdown 全部置零。
- **数据库证据**：`SELECT COUNT(*) FROM factor_exploration_results WHERE sharpe_ratio = 0` → `781 / 792`（98.6%）。
- **量化影响**：
  - 597 行记录 `trades_count > 0` 但五大指标全为 0。
  - 所有 10m/30m 等关键时间框架的平均 Sharpe 被压平到 0，Phase2 组合输入失真。

## 2. 根因拆解
| 条件 | 原设计意图 | 实际后果 |
| --- | --- | --- |
| `abs(sharpe) > 10` | 拦截“异常” Sharpe | 高频数据天然波动较大，真实 Sharpe 被误判并清零 |
| `trades.size < 5` | 防止小样本噪声 | 多数因子在短期窗口内交易次数 < 5，被判定为无效 |
| `return` 逻辑 | 直接覆盖所有指标 | 完全丢失交易行为信息，无法复现问题 |

## 3. 修复方案
### 3.1 分层防护
- **诊断分离**：保留真实指标，同时返回 `diagnostics`（例如 `insufficient_trades(<5)`、`sharpe_exceeds_alert_threshold`）。
- **早期退出**：仅当数据长度 < 20 或信号全零时返回空结果（`phase1/backtest_engine.py:33-63`）。
- **指标保留**：正常回测路径不再清零指标，只附带诊断。

### 3.2 配置化方向（后续）
- 将 `MAX_ABS_SHARPE_ALERT`、`MIN_TRADES_FOR_CONFIDENT_METRICS` 暴露到配置层，配合监控与告警。
- 在 Phase2/报告链路创建“低置信度”标签，而不是事先清除数据。

## 4. 验证结果
- 手工回测 `dc_phase@10m`：
  - 原逻辑：Sharpe=0，WinRate=0。
  - 新逻辑：Sharpe≈-10.4，WinRate≈0.81%，并附 `diagnostics=['sharpe_exceeds_alert_threshold']`。
- 新增单测：`tests/phase1/test_backtest_engine_phase1.py` 覆盖诊断输出与指标一致性。

## 5. 下一步
1. 将 `diagnostics` 写入监控与报告，把阻断前置为“审计”而非“篡改”。
2. 在 DataQualityValidator 与监控报表中统计诊断类型，形成逐步治理闭环。
