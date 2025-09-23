# 数据质量验证器激进处理问题

## 1. 现象回顾
- 组件位置：`utils/data_quality.py:12-220`
- 历史逻辑：
  1. `METRIC_BOUNDS` 将 Sharpe 限制在 [-10,10]，ProfitFactor 限制在 [0,50]。
  2. `EXTREME_VALUE_THRESHOLDS` 对超过 5/10/0.95 的值做“中和”处理（直接改写为 0 或 0.5）。
  3. `_validate_cross_metrics` 将 `win_rate=0` 且 `profit_factor>1` 的结果强制置 0。
- 与 Phase1 保护器叠加 → 即便回测输出了真实指标，也被验证器再次清零。

## 2. 叠加效应分析
| 调用链 | 旧逻辑输出 | 验证器处理 | 数据库最终值 |
| --- | --- | --- | --- |
| 高频 Sharpe≈-10.4 | -10.4 | 超过阈值 → 设为 0 | 0 |
| WinRate≈0.008 | 0.008 | `win_rate=0` 视作 0 | 0 |
| ProfitFactor≈0.5 | 0.5 | 因 WinRate=0 → 设为 0 | 0 |

## 3. 改进方案
### 3.1 指标保留原则
- **扩大硬性边界**：Sharpe 允许 [-100,100]，ProfitFactor 允许 [0,500] 等（`METRIC_BOUNDS` 调整）。
- **告警而非归零**：超过建议阈值仅记录 `_validation_violations`，不再修改指标值。
- **交叉校验无侧写**：`_validate_cross_metrics` 仅产生日志，不再篡改 `win_rate` 或 `profit_factor`。

### 3.2 额外校验
- 新增 `timeframe_count_mismatch`，保证组合的因子与时间框架数量一致。
- `validate_factor_result`、`validate_combination_strategy` 统一输出 `_validation_status`，供监控统计。

## 4. 验证测试
- 添加 `tests/utils/test_data_quality_validator.py`：
  - 确认 Sharpe=18、ProfitFactor=25 不被清零，仅记录警告。
  - 检查组合时间框架不匹配时出现 violation。

## 5. 后续优化建议
1. **动态阈值**：根据时间框架/样本量动态调整 advisory threshold。
2. **多层响应**：严重违规（例如 NaN/Inf）仍清理；普通超出仅记录；高风险（极端值）触发人工审计。
3. **可观测性**：将 `_validation_violations` 聚合到监控仪表板，形成数据质量周报。
