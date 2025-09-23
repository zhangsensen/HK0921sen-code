# 修复指南与实施路线图

## 1. 分阶段计划
| 阶段 | 目标 | 关键动作 | 成功指标 |
| --- | --- | --- | --- |
| 立即 (T+0) | 恢复指标可用性 | 部署回测与验证器补丁；补齐组合时间框架 | 单因子 Sharpe/WinRate 不再为 0，组合表含 `timeframes` |
| 短期 (T+3) | 重新生成基线 | 重跑 Phase1/Phase2，刷新 `benchmark_results/*.sqlite` 与监控报告 | 新基线报告中指标分布合理，诊断率可解释 |
| 中期 (T+14) | 加固校验与监控 | 上线诊断统计面板；CI 增加“指标全零”断言；引入配置化阈值 | CI 和监控能提前捕获指标被清零的异常 |
| 长期 (T+30) | 建立治理闭环 | 制定异常指标审计流程，版本化回测保护参数，定期抽样复核 | 季度审计报告覆盖诊断统计，指标可信度回归健康范围 |

## 2. 代码修复摘要
| 文件 | 行号 | 调整要点 |
| --- | --- | --- |
| `phase1/backtest_engine.py` | `33-133` | 去除强行清零；保留真实指标并输出 `diagnostics`；为空数据/无信号时返回早退结果 |
| `utils/data_quality.py` | `12-219` | 扩大边界、保留指标、记录违规信息；新增时间框架数量校验 |
| `database.py` | `61-335` | `StrategyResult` 增加 `timeframes`；`combination_strategies` 表新增列；读写 JSON 化 |

## 3. 代码片段示例
```python
# phase1/backtest_engine.py:109-115
if trades.size < self.MIN_TRADES_FOR_CONFIDENT_METRICS:
    diagnostics.append(f"insufficient_trades(<{self.MIN_TRADES_FOR_CONFIDENT_METRICS})")
if abs(sharpe) > self.MAX_ABS_SHARPE_ALERT:
    diagnostics.append("sharpe_exceeds_alert_threshold")
```

```python
# utils/data_quality.py:168-175
messages = []
if violation:
    messages.append(violation)
if advisory_violation:
    messages.append(advisory_violation)
violation_message = "; ".join(messages) if messages else None
```

```python
# database.py:243-272
(
    s["symbol"],
    s["strategy_name"],
    json.dumps(s["factors"]),
    json.dumps(s.get("timeframes", [])),
    ...
)
```

## 4. 验证步骤
1. **单元测试**：`pytest tests/phase1/test_backtest_engine_phase1.py tests/test_database_manager.py tests/database/test_schema_and_transactions.py tests/utils/test_data_quality_validator.py`
2. **数据库自检**：
   - `SELECT COUNT(*) FROM factor_exploration_results WHERE sharpe_ratio = 0;` 应显著下降。
   - `SELECT factor_combination,timeframes FROM combination_strategies LIMIT 5;` 时间框架字段应填充。
3. **基线重建**：运行 `python scripts/benchmark_discovery.py --samples 5 --report-path runtime/benchmark/results/baseline_postfix.json`，确认生成的 JSON 中 `comparison.metrics` 有非零差异。
4. **监控检查**：验证 `diagnostics` 与 `_validation_violations` 已随结果写入，监控可聚合统计。

## 5. 注意事项
- 旧数据表的 `timeframes` 默认 `'[]'`，需要脚本回填（可按 `factor_combination` 和 Phase1 结果 join）。
- 若后续调整阈值，需同步更新 `AppSettings`/配置文件以及监控告警阈值。
- 回测重放需使用修复后的代码重新导出所有报告，避免历史污染。
