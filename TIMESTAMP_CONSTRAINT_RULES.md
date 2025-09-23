# 🔒 时间戳格式核心铁律

## 核心规则

**🚫 禁止格式：毫秒时间戳 (如 `1741182300000`)**
**✅ 强制格式：人类可读时间戳 (如 `2025-03-05 09:30:00`)**

## 规则详情

### 1. 强制约束
- 所有时间戳必须保持人类可读格式 `YYYY-MM-DD HH:MM:SS`
- 永远不允许使用毫秒时间戳格式
- 这是不可绕过的系统约束

### 2. 禁止的时间戳格式
- ❌ 毫秒时间戳：`1741182300000`
- ❌ Unix时间戳数字：`1712254200`
- ❌ Epoch毫秒数：`1741182300000`
- ❌ 任何13位及以上的纯数字时间戳

### 3. 允许的时间戳格式
- ✅ `datetime64[ns]` (pandas datetime64)
- ✅ `2025-03-05 09:30:00` (标准字符串格式)
- ✅ `2025-03-05T09:30:00` (ISO格式)
- ✅ Python datetime对象

## 实施方案

### 1. 约束验证器
**文件：** `utils/timestamp_constraint_validator.py`

```python
from timestamp_constraint_validator import validate_resampling_output, TimestampConstraintError

# 在重采样代码中使用
result = validate_resampling_output(df, "重采样操作")
```

### 2. 重采样代码更新
已更新的文件：
- `resampling/simple_resampler.py`
- `resampling/production_resampler_simple.py`
- `realtime_resampling_engine.py`

### 3. 验证机制
- **输入验证：** 处理前验证时间戳格式
- **处理中验证：** 重采样过程中持续检查
- **输出验证：** 保存前后双重验证
- **文件验证：** 读取已保存文件进行最终确认

### 4. 错误处理
- 检测到非法格式时立即抛出 `TimestampConstraintError`
- 自动删除包含错误格式的文件
- 提供清晰的错误信息和修复建议

## 技术实现

### 约束验证器核心功能
```python
class TimestampConstraintValidator:
    def validate_timestamp_format(self, timestamp, context)
    def validate_dataframe_timestamps(self, df, timestamp_col)
    def enforce_readable_format(self, df, timestamp_col, context)
    def check_resampling_output(self, df, operation)
```

### 全局验证函数
```python
# 通用验证
validate_timestamps(df, context)

# 重采样专用验证
validate_resampling_output(df, operation)
```

## 测试验证

### 测试文件
**文件：** `test_timestamp_constraint.py`

```bash
python3 test_timestamp_constraint.py
```

### 测试覆盖
- ✅ 有效时间戳格式验证
- ✅ 无效时间戳格式拒绝
- ✅ DataFrame时间戳验证
- ✅ 重采样集成验证
- ✅ 约束违反恢复测试

## 使用示例

### 1. 基本验证
```python
from utils.timestamp_constraint_validator import validate_timestamps

# 验证DataFrame
validated_df = validate_timestamps(df, "数据处理")
```

### 2. 重采样验证
```python
from utils.timestamp_constraint_validator import validate_resampling_output

# 重采样后验证
result = validate_resampling_output(resampled_df, "1小时重采样")
```

### 3. 自定义验证
```python
from utils.timestamp_constraint_validator import TimestampConstraintValidator

validator = TimestampConstraintValidator()
result = validator.enforce_readable_format(df, 'timestamp', "自定义操作")
```

## 文件结构

```
utils/
├── timestamp_constraint_validator.py  # 核心验证器
├── timestamp_converter.py            # 时间戳转换工具
└── timestamp_formatter.py            # 时间戳格式化工具

resampling/
├── simple_resampler.py               # 简单重采样器（已更新）
├── production_resampler_simple.py    # 生产重采样器（已更新）
└── examples/

test_timestamp_constraint.py         # 约束测试套件
```

## 核心铁律原则

1. **不可绕过：** 所有时间戳操作必须经过验证
2. **强制执行：** 检测到非法格式立即失败
3. **清晰反馈：** 提供详细的错误信息和修复建议
4. **全面覆盖：** 从输入到输出的完整验证链
5. **自动恢复：** 错误时自动清理和恢复

## 违反约束的后果

- ⚠️ 操作立即失败并抛出异常
- 🗑️ 错误文件自动删除
- 📝 详细错误日志记录
- 🔒 阻止后续处理继续

## 监控和维护

### 日志监控
- 所有验证操作都记录日志
- 错误情况详细记录
- 成功操作跟踪统计

### 性能考虑
- 验证操作优化为高效处理
- 大数据集采样验证
- 缓存机制避免重复验证

---

**🔒 此铁律适用于整个系统，所有代码必须遵守**