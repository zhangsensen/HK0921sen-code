# 港股因子发现系统 - 系统架构

## 🏗️ 总体架构

港股因子发现系统采用**两阶段因子发现架构**，通过系统化的单因子探索和多因子组合优化，构建稳健的量化交易策略。

### 📊 核心设计理念

1. **模块化设计**: 各功能模块独立，便于维护和扩展
2. **数据驱动**: 基于历史数据进行系统的因子验证
3. **质量控制**: 全流程的数据验证和质量保证
4. **性能导向**: 以风险调整后收益为核心的评估体系

## 🔄 系统流程图

```
历史数据获取 → 单因子探索 → 性能评估 → 因子筛选 → 多因子组合 → 策略优化 → 结果输出
```

## 📁 核心模块架构

### 1. 数据层 (Data Layer)

#### 数据加载器 (`data_loader.py`)
```python
class HistoricalDataLoader:
    """历史数据加载和预处理"""

    def load(self, symbol: str, timeframe: str) -> pd.DataFrame
    def preload_timeframes(self, symbol: str, timeframes: List[str])
    def batch_load(self, requests: List[Tuple[str, str]])
```

**主要功能**:
- 支持多时间框架数据获取 (1m, 5m, 15m, 30m, 1h, 4h)
- 批量数据加载和缓存
- 数据预处理和格式化

#### 数据库管理 (`database.py`)
```python
class DatabaseManager:
    """数据库管理层"""

    # 核心数据表
    - factor_exploration_results: 单因子回测结果
    - combination_strategies: 多因子组合策略
    - system_config: 系统配置管理
```

**主要功能**:
- SQLite数据持久化
- 数据完整性验证
- 高效的数据检索和查询

### 2. 计算层 (Computation Layer)

#### 因子计算器 (`factors.py`)
```python
class FactorCalculator:
    """72种技术指标因子"""

    # 因子类别
    - 趋势因子: MACD, RSI, Stochastic等
    - 动量因子: CCI, Williams %R等
    - 波动率因子: ATR, Standard Deviation等
    - 成交量因子: Volume Oscillator, OBV等
```

**主要功能**:
- 72种技术指标计算
- 标准化因子信号生成
- 多时间框架支持

#### 性能计算器 (`utils/performance_metrics.py`)
```python
class PerformanceMetrics:
    """性能指标计算"""

    @staticmethod
    def calculate_sharpe_ratio(returns: np.ndarray) -> float
    @staticmethod
    def calculate_stability(returns: np.ndarray) -> float
    @staticmethod
    def calculate_profit_factor(gains: np.ndarray, losses: np.ndarray) -> float
```

### 3. 回测层 (Backtesting Layer)

#### 单因子回测引擎 (`phase1/backtest_engine.py`)
```python
class SimpleBacktestEngine:
    """单因子回测引擎"""

    def __init__(self, symbol: str, initial_capital: float = 100_000)
    def backtest_factor(self, data: pd.DataFrame, signals: pd.Series) -> dict
```

**主要功能**:
- 信号生成和回测执行
- 性能指标计算
- 香港交易成本建模
- 诊断信息输出

#### 多因子组合器 (`phase2/combiner.py`)
```python
class MultiFactorCombiner:
    """多因子组合优化器"""

    def select_top_factors(self, top_n: Optional[int] = None)
    def generate_combinations(self, factors: Sequence[Mapping[str, object]])
    def backtest_combination(self, combo: Sequence[Mapping[str, object]])
```

**主要功能**:
- 因子筛选和排序
- 多因子组合生成
- 组合策略回测
- 性能优化

### 4. 质量控制层 (Quality Control Layer)

#### 数据质量验证器 (`utils/data_quality.py`)
```python
class DataQualityValidator:
    """数据质量验证器"""

    # 指标边界定义
    METRIC_BOUNDS = {
        'sharpe_ratio': (-100.0, 100.0),
        'stability': (-1.0, 1.0),
        'win_rate': (0.0, 1.0),
        'profit_factor': (0.0, 500.0),
        'max_drawdown': (0.0, 1.0),
    }
```

**主要功能**:
- 指标边界检查
- 极端值检测
- 跨指标一致性验证
- 数据质量报告生成

## 🗄️ 数据架构

### 核心数据表

#### 1. factor_exploration_results
```sql
CREATE TABLE factor_exploration_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    factor_name TEXT NOT NULL,
    sharpe_ratio REAL NOT NULL,
    stability REAL NOT NULL,
    trades_count INTEGER NOT NULL,
    win_rate REAL NOT NULL,
    profit_factor REAL NOT NULL,
    max_drawdown REAL NOT NULL,
    information_coefficient REAL NOT NULL DEFAULT 0,
    exploration_date TEXT NOT NULL,
    UNIQUE(symbol, timeframe, factor_name)
);
```

#### 2. combination_strategies
```sql
CREATE TABLE combination_strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    factor_combination TEXT NOT NULL,
    timeframes TEXT NOT NULL,
    sharpe_ratio REAL NOT NULL,
    stability REAL NOT NULL,
    trades_count INTEGER NOT NULL,
    win_rate REAL NOT NULL,
    profit_factor REAL NOT NULL,
    max_drawdown REAL NOT NULL,
    average_information_coefficient REAL NOT NULL DEFAULT 0,
    creation_date TEXT NOT NULL,
    UNIQUE(symbol, strategy_name)
);
```

### 数据流程图

```
原始数据 → 数据验证 → 因子计算 → 回测执行 → 质量检查 → 数据存储 → 结果分析
```

## 🔧 配置管理

### 系统配置 (`config.py`)
```python
@dataclass
class AppConfig:
    """应用配置"""
    data_directory: str = "data"
    results_directory: str = "benchmark_results"

@dataclass
class CombinerConfig:
    """组合器配置"""
    top_n: int = 10
    max_factors: int = 4
    max_combinations: Optional[int] = 100
    min_sharpe: float = 0.5
    min_information_coefficient: float = 0.01

@dataclass
class BacktestConfig:
    """回测配置"""
    initial_capital: float = 100_000
    allocation: float = 0.1
    min_data_points: int = 20
```

## 📊 性能监控

### 关键性能指标 (KPIs)

1. **计算性能**
   - 因子计算速度: < 1秒/因子
   - 回测执行速度: < 5秒/策略
   - 数据查询响应: < 100ms

2. **数据质量**
   - 数据完整性: > 99%
   - 指标准确性: > 95%
   - 异常值比例: < 5%

3. **系统稳定性**
   - 运行时间稳定性: 99.9%
   - 内存使用效率: < 1GB
   - 错误率: < 0.1%

## 🚀 扩展性设计

### 水平扩展
- 支持多股票并行处理
- 分布式因子计算
- 负载均衡和任务分配

### 垂直扩展
- 模块化插件架构
- 自定义因子支持
- 多数据库支持

### 功能扩展
- 实时数据集成
- 机器学习集成
- 风险管理模块

## 🔒 安全考虑

### 数据安全
- 数据加密存储
- 访问权限控制
- 数据完整性验证

### 系统安全
- 输入参数验证
- SQL注入防护
- 异常处理机制

## 📈 监控和日志

### 系统监控
- 性能指标监控
- 错误率监控
- 资源使用监控

### 日志管理
- 结构化日志记录
- 错误追踪和调试
- 性能分析日志

---

*系统架构文档持续更新中...*