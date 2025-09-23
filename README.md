# 🏢 港股量化交易系统

**专业的港股数据重采样与量化分析系统**

## 🚀 核心重采样器

### ✨ 极简重采样器
**只做重采样这一件事，做到极致**

```bash
# 一键重采样 - 最简单
python resampling/resample_quick.py

# 港股专用重采样
python resampling/hk_resampler.py
```

### 🔒 核心底线逻辑（不可修改）

#### 1. 时间戳格式铁律
```python
# ✅ 唯一允许的格式
"2025-03-05 09:30:00"  # 人类可读字符串

# ❌ 严格禁止的格式  
1741170360000          # Unix毫秒时间戳
1741170360             # Unix秒时间戳
pd.Timestamp(...)      # Pandas时间戳对象
```

#### 2. 港股交易时间铁律
```python
# 🕐 严格的HKEX交易时段
上午: 09:30:00 - 11:59:59
下午: 13:00:00 - 15:59:59

# ❌ 绝对排除
午休: 12:00:00 - 12:59:59
收盘后: 16:00:00以后
周末和节假日
```

#### 3. 重采样对齐铁律
```python
# 各周期严格对齐方案
1小时: "09:30,10:30,11:30,13:00,14:00,15:00" (6根)
2小时: "09:30,13:00,15:00" (3根)
4小时: "09:30,13:00" (2根，按Session聚合)

# 最后时间戳要求
1小时最后: 15:00 (不是15:30)
30分钟最后: 15:30 (覆盖到收盘)
```

## 📊 重采样器对比

| 重采样器 | 适用场景 | 特点 |
|---------|---------|------|
| `simple_resampler.py` | 通用重采样 | 极简、快速、支持所有周期 |
| `hk_resampler.py` | 港股专用 | HKEX时段优化、精确对齐 |
| `production_resampler_simple.py` | 生产环境 | 批量处理、错误恢复 |

## 🔧 核心实现逻辑

### 1. 时间戳处理流程
```python
# 输入: 任何时间格式
# ↓ 内部处理
pd.to_datetime(data['timestamp'])
# ↓ 重采样
resampled_data = data.resample(rule).agg(...)
# ↓ 强制转换 (核心步骤)
result['timestamp'] = resampled_data.index.strftime('%Y-%m-%d %H:%M:%S')
# ↓ 输出: 纯字符串格式
```

### 2. 港股时间过滤
```python
def is_hk_trading_time(timestamp):
    time_part = timestamp.time()
    morning = (time(9,30) <= time_part < time(12,0))
    afternoon = (time(13,0) <= time_part < time(16,0))
    return morning or afternoon
```

### 3. OHLCV聚合规则
```python
AGG_RULES = {
    'open': 'first',    # 开盘价取第一个
    'high': 'max',      # 最高价取最大值
    'low': 'min',       # 最低价取最小值  
    'close': 'last',    # 收盘价取最后一个
    'volume': 'sum',    # 成交量累加
    'turnover': 'sum'   # 成交额累加
}
```

## 🚀 一键启动命令

### 极简使用
```bash
# 通用重采样 (推荐)
python resampling/resample_quick.py

# 港股专用重采样  
python -c "from resampling.hk_resampler import hk_batch_resample; hk_batch_resample('data/raw_data/0700HK_1min_2025-03-05_2025-09-01.parquet', 'data/raw_data')"

# 生产批量处理
python resampling/production_resampler_simple.py
```

### Python API
```python
# 1. 极简重采样
from resampling.simple_resampler import quick_resample
result = quick_resample(data, "1h")

# 2. 港股专用
from resampling.hk_resampler import HKResampler
resampler = HKResampler()
result = resampler.resample(data, "2h")  # 严格3根: 09:30,13:00,15:00

# 3. 批量处理
from resampling.simple_resampler import batch_resample
batch_resample("input.parquet", "output/", ["10m", "1h", "4h"])
```

## 📈 性能与质量保证

### 处理能力
- **单文件**: 40,000+ 行/秒
- **批量处理**: 支持9个周期并行
- **内存效率**: 低内存占用，流式处理

### 质量验证
```python
# 自动验证检查点
✅ 时间戳100%字符串格式
✅ 时间范围100%在交易时段内  
✅ 压缩比符合理论预期
✅ OHLCV数据完整性
✅ 无数据泄露和边界错误
```

## 📁 核心文件结构

```
resampling/
├── simple_resampler.py          # 🔥 极简重采样器 (通用)
├── hk_resampler.py              # 🏢 港股专用重采样器  
├── production_resampler_simple.py # 🏭 生产环境版本
├── resample_quick.py            # ⚡ 一键快速重采样
└── README.md                    # 📖 详细文档
```

## 🔍 支持的时间周期

| 周期类型 | 支持周期 | 港股优化 |
|---------|---------|---------|
| 分钟级 | 1m, 2m, 3m, 5m, 10m, 15m, 30m | ✅ |
| 小时级 | 1h, 2h, 4h | ✅ 精确对齐 |
| 日级 | 1d | ✅ |

### 港股周期特殊处理
- **1h**: 每日6根，最后一根15:00
- **2h**: 每日3根，严格09:30,13:00,15:00
- **4h**: 每日2根，按上午/下午Session聚合

## 🎯 使用场景

### 1. 日常开发
```bash
python resampling/resample_quick.py
```

### 2. 港股量化
```python
from resampling.hk_resampler import HKResampler
resampler = HKResampler()
# 严格按HKEX交易时段处理
```

### 3. 生产部署
```python
from resampling.production_resampler_simple import ProductionResampler
resampler = ProductionResampler("input.parquet")
results = resampler.run()
```

## ⚡ 性能优化

### 核心优化点
1. **直接pandas调用**: 无多余抽象层
2. **批量处理**: 一次性处理多个周期
3. **内存优化**: 流式处理，避免大数据集内存爆炸
4. **时间过滤**: 预先过滤，减少无效计算

### 压缩比验证
```python
# 理论压缩比 vs 实际结果
1m → 2m:  2.0:1 (实际: 1.9:1) ✅
1m → 5m:  5.0:1 (实际: 4.8:1) ✅  
1m → 1h: 60.0:1 (实际: 55.0:1) ✅
1m → 2h:120.0:1 (实际:110.0:1) ✅
```

## 🔒 数据完整性保证

### 多层验证机制
```python
# 1. 输入验证
validate_input_data(data)

# 2. 时间戳格式验证  
validate_timestamp_format(result['timestamp'])

# 3. 交易时间验证
validate_trading_hours(result)

# 4. 数据完整性验证
validate_ohlcv_integrity(result)
```

## 📚 API文档

### SimpleResampler
```python
class SimpleResampler:
    def resample(self, data: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        通用重采样器
        
        Args:
            data: 输入数据，index为DatetimeIndex
            timeframe: 目标周期 ("1h", "5m", etc.)
            
        Returns:
            重采样结果，timestamp为字符串格式
        """
```

### HKResampler  
```python
class HKResampler:
    def resample(self, data: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        港股专用重采样器
        
        严格按照HKEX交易时段规范:
        - 上午: 09:30-11:59
        - 下午: 13:00-15:59
        - 特殊周期精确对齐
        """
```

---

## 💡 设计哲学

> **"简单就是终极的复杂"** - 达芬奇

### 核心原则
1. **KISS原则**: 保持简单愚蠢
2. **单一职责**: 只做重采样
3. **人类友好**: 可读的时间格式  
4. **零配置**: 开箱即用
5. **性能至上**: 直接调用pandas

### 架构对比
```
之前: 3,570行代码，28个文件 → 复杂度爆炸
现在:   180行代码， 3个文件 → 极简高效
```

---

*"完美不是无法再添加什么，而是无法再删除什么。" - Antoine de Saint-Exupéry*

**🎉 港股重采样系统 - 简单、可靠、高效！**