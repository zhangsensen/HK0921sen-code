#!/usr/bin/env python3
"""
港股重采样器 - 最终版
严格按照HKEX交易时段规范的重采样器

🕐 港股交易时间:
- 上午: 09:30-11:59 
- 下午: 13:00-15:59
- 排除: 午休时间、节假日、非交易日

✅ 核心特性:
1. 严格的港股时间过滤
2. 精确的时间戳对齐
3. 人类可读时间格式
4. 完美的压缩比
"""

import pandas as pd
from datetime import time, datetime
from pathlib import Path
import numpy as np

# 简单的时间戳验证
def validate_timestamp_format(ts_str: str, context: str = ""):
    """验证时间戳格式"""
    if not isinstance(ts_str, str):
        raise ValueError(f"❌ {context}: 时间戳不是字符串格式: {ts_str}")
    
    if len(ts_str) != 19 or ts_str[10] != ' ' or ts_str[4] != '-' or ts_str[7] != '-':
        raise ValueError(f"❌ {context}: 时间戳格式错误: {ts_str}")
    
    return True

class HKResamplerFixed:
    """港股重采样器 - 修复版"""
    
    # 🔒 严格的港股交易时段定义
    MORNING_START = time(9, 30)    # 上午开盘
    MORNING_END = time(12, 0)      # 上午收盘（不包含12:00）
    AFTERNOON_START = time(13, 0)  # 下午开盘  
    AFTERNOON_END = time(16, 0)    # 下午收盘（不包含16:00）
    
    # 支持的时间周期
    TIMEFRAMES = {
        '1m': '1min', '2m': '2min', '3m': '3min', '5m': '5min',
        '10m': '10min', '15m': '15min', '30m': '30min',
        '1h': '1h', '2h': '2h', '4h': '4h'
    }
    
    # OHLCV聚合规则
    AGG_RULES = {
        'open': 'first', 'high': 'max', 'low': 'min',
        'close': 'last', 'volume': 'sum', 'turnover': 'sum'
    }
    
    def __init__(self):
        """初始化重采样器"""
        pass
    
    def is_hk_trading_time(self, timestamp) -> bool:
        """
        🔒 严格的港股交易时间判断
        
        修复问题：
        1. 不包含午休时间 (12:00-13:00)
        2. 不包含16:00（已收盘）
        3. 严格边界检查
        """
        if isinstance(timestamp, str):
            dt = pd.to_datetime(timestamp)
        else:
            dt = timestamp
            
        # 只保留工作日
        if dt.weekday() >= 5:  # 周六日
            return False
            
        time_part = dt.time()
        
        # 严格的交易时间判断
        morning_trading = (self.MORNING_START <= time_part < self.MORNING_END)
        afternoon_trading = (self.AFTERNOON_START <= time_part < self.AFTERNOON_END)
        
        return morning_trading or afternoon_trading
    
    def filter_hk_trading_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        🔒 严格过滤港股交易数据
        
        修复问题：
        1. 使用正确的时间判断逻辑
        2. 确保只保留真正的交易时间数据
        """
        if data.empty:
            return data
            
        # 确保index是DatetimeIndex
        if not isinstance(data.index, pd.DatetimeIndex):
            raise ValueError("数据index必须是DatetimeIndex")
        
        # 使用严格的时间过滤
        mask = data.index.to_series().apply(self.is_hk_trading_time)
        filtered_data = data[mask]
        
        print(f"时间过滤: {len(data)} -> {len(filtered_data)} 行")
        return filtered_data
    
    def resample(self, data: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        港股重采样核心函数 - 修复版
        
        修复问题：
        1. 先严格过滤交易时间
        2. 然后进行重采样
        3. 最后再次验证结果
        """
        if timeframe not in self.TIMEFRAMES:
            raise ValueError(f"不支持的时间周期: {timeframe}")
        
        if not isinstance(data.index, pd.DatetimeIndex):
            raise ValueError("数据index必须是DatetimeIndex")
        
        print(f"🔄 开始{timeframe}重采样")
        print(f"原始数据: {len(data)} 行")
        
        # 🔒 步骤1: 严格过滤港股交易时间
        trading_data = self.filter_hk_trading_data(data)
        if trading_data.empty:
            print("⚠️  过滤后无交易数据")
            return pd.DataFrame()
        
        # 🔒 步骤2: 执行重采样
        rule = self.TIMEFRAMES[timeframe]
        agg_dict = {col: func for col, func in self.AGG_RULES.items() 
                   if col in trading_data.columns}
        
        if timeframe == '4h':
            # 4小时特殊处理：按交易时段分组
            result_list = []
            
            # 按日期分组
            for date, day_data in trading_data.groupby(trading_data.index.date):
                # 上午数据 (09:30-11:59:59)
                morning = day_data.between_time('09:30', '11:59:59')
                if not morning.empty:
                    morning_agg = {}
                    for col, func in agg_dict.items():
                        if func == 'first':
                            morning_agg[col] = morning[col].iloc[0]
                        elif func == 'last':
                            morning_agg[col] = morning[col].iloc[-1]
                        elif func == 'max':
                            morning_agg[col] = morning[col].max()
                        elif func == 'min':
                            morning_agg[col] = morning[col].min()
                        elif func == 'sum':
                            morning_agg[col] = morning[col].sum()
                    
                    morning_series = pd.Series(morning_agg, name=pd.Timestamp(f"{date} 09:30:00"))
                    result_list.append(morning_series)
                
                # 下午数据 (13:00-15:59:59)  
                afternoon = day_data.between_time('13:00', '15:59:59')
                if not afternoon.empty:
                    afternoon_agg = {}
                    for col, func in agg_dict.items():
                        if func == 'first':
                            afternoon_agg[col] = afternoon[col].iloc[0]
                        elif func == 'last':
                            afternoon_agg[col] = afternoon[col].iloc[-1]
                        elif func == 'max':
                            afternoon_agg[col] = afternoon[col].max()
                        elif func == 'min':
                            afternoon_agg[col] = afternoon[col].min()
                        elif func == 'sum':
                            afternoon_agg[col] = afternoon[col].sum()
                    
                    afternoon_series = pd.Series(afternoon_agg, name=pd.Timestamp(f"{date} 13:00:00"))
                    result_list.append(afternoon_series)
            
            if result_list:
                resampled = pd.DataFrame(result_list)
                resampled.index.name = 'timestamp'
            else:
                resampled = pd.DataFrame()
                
        elif timeframe == '1h':
            # 1小时重采样：按方案要求严格实现 09:30,10:30,11:30,13:00,14:00,15:00 (6根)
            result_list = []
            
            # 按日期分组，手动创建6根1小时bar
            for date, day_data in trading_data.groupby(trading_data.index.date):
                # 上午3根: 09:30, 10:30, 11:30
                for hour in [9, 10, 11]:
                    if hour == 9:
                        bar_data = day_data.between_time('09:30', '10:29:59')
                        label_time = f"{date} 09:30:00"
                    elif hour == 10:
                        bar_data = day_data.between_time('10:30', '11:29:59')
                        label_time = f"{date} 10:30:00"
                    elif hour == 11:
                        bar_data = day_data.between_time('11:30', '11:59:59')  # 只有30分钟
                        label_time = f"{date} 11:30:00"
                    
                    if not bar_data.empty:
                        bar_agg = {}
                        for col, func in agg_dict.items():
                            if func == 'first':
                                bar_agg[col] = bar_data[col].iloc[0]
                            elif func == 'last':
                                bar_agg[col] = bar_data[col].iloc[-1]
                            elif func == 'max':
                                bar_agg[col] = bar_data[col].max()
                            elif func == 'min':
                                bar_agg[col] = bar_data[col].min()
                            elif func == 'sum':
                                bar_agg[col] = bar_data[col].sum()
                        
                        bar_series = pd.Series(bar_agg, name=pd.Timestamp(label_time))
                        result_list.append(bar_series)
                
                # 下午3根: 13:00, 14:00, 15:00
                for hour in [13, 14, 15]:
                    if hour == 13:
                        bar_data = day_data.between_time('13:00', '13:59:59')
                        label_time = f"{date} 13:00:00"
                    elif hour == 14:
                        bar_data = day_data.between_time('14:00', '14:59:59')
                        label_time = f"{date} 14:00:00"
                    elif hour == 15:
                        bar_data = day_data.between_time('15:00', '15:59:59')
                        label_time = f"{date} 15:00:00"
                    
                    if not bar_data.empty:
                        bar_agg = {}
                        for col, func in agg_dict.items():
                            if func == 'first':
                                bar_agg[col] = bar_data[col].iloc[0]
                            elif func == 'last':
                                bar_agg[col] = bar_data[col].iloc[-1]
                            elif func == 'max':
                                bar_agg[col] = bar_data[col].max()
                            elif func == 'min':
                                bar_agg[col] = bar_data[col].min()
                            elif func == 'sum':
                                bar_agg[col] = bar_data[col].sum()
                        
                        bar_series = pd.Series(bar_agg, name=pd.Timestamp(label_time))
                        result_list.append(bar_series)
            
            if result_list:
                resampled = pd.DataFrame(result_list)
                resampled.index.name = 'timestamp'
            else:
                resampled = pd.DataFrame()
                
        elif timeframe == '2h':
            # 2小时重采样：按方案要求严格实现 09:30,13:00,15:00 (3根)
            result_list = []
            
            # 按日期分组，手动创建3根2小时bar
            for date, day_data in trading_data.groupby(trading_data.index.date):
                # Bar 1: 09:30-11:30 (2小时)
                bar1_data = day_data.between_time('09:30', '11:29:59')
                if not bar1_data.empty:
                    bar1_agg = {}
                    for col, func in agg_dict.items():
                        if func == 'first':
                            bar1_agg[col] = bar1_data[col].iloc[0]
                        elif func == 'last':
                            bar1_agg[col] = bar1_data[col].iloc[-1]
                        elif func == 'max':
                            bar1_agg[col] = bar1_data[col].max()
                        elif func == 'min':
                            bar1_agg[col] = bar1_data[col].min()
                        elif func == 'sum':
                            bar1_agg[col] = bar1_data[col].sum()
                    
                    bar1_series = pd.Series(bar1_agg, name=pd.Timestamp(f"{date} 09:30:00"))
                    result_list.append(bar1_series)
                
                # Bar 2: 13:00-15:00 (2小时)
                bar2_data = day_data.between_time('13:00', '14:59:59')
                if not bar2_data.empty:
                    bar2_agg = {}
                    for col, func in agg_dict.items():
                        if func == 'first':
                            bar2_agg[col] = bar2_data[col].iloc[0]
                        elif func == 'last':
                            bar2_agg[col] = bar2_data[col].iloc[-1]
                        elif func == 'max':
                            bar2_agg[col] = bar2_data[col].max()
                        elif func == 'min':
                            bar2_agg[col] = bar2_data[col].min()
                        elif func == 'sum':
                            bar2_agg[col] = bar2_data[col].sum()
                    
                    bar2_series = pd.Series(bar2_agg, name=pd.Timestamp(f"{date} 13:00:00"))
                    result_list.append(bar2_series)
                
                # Bar 3: 15:00-16:00 (1小时，不完整)
                bar3_data = day_data.between_time('15:00', '15:59:59')
                if not bar3_data.empty:
                    bar3_agg = {}
                    for col, func in agg_dict.items():
                        if func == 'first':
                            bar3_agg[col] = bar3_data[col].iloc[0]
                        elif func == 'last':
                            bar3_agg[col] = bar3_data[col].iloc[-1]
                        elif func == 'max':
                            bar3_agg[col] = bar3_data[col].max()
                        elif func == 'min':
                            bar3_agg[col] = bar3_data[col].min()
                        elif func == 'sum':
                            bar3_agg[col] = bar3_data[col].sum()
                    
                    bar3_series = pd.Series(bar3_agg, name=pd.Timestamp(f"{date} 15:00:00"))
                    result_list.append(bar3_series)
            
            if result_list:
                resampled = pd.DataFrame(result_list)
                resampled.index.name = 'timestamp'
            else:
                resampled = pd.DataFrame()
        else:
            # 分钟级重采样
            resampled = trading_data.resample(
                rule, closed='left', label='left'
            ).agg(agg_dict)
            
            # 🔒 关键修复：移除重采样产生的边界时间戳
            # pandas resample可能在12:00, 16:00等边界产生时间戳
            if not resampled.empty:
                # 过滤掉非交易时间的时间戳
                valid_mask = resampled.index.to_series().apply(self.is_hk_trading_time)
                resampled = resampled[valid_mask]
        
        # 🔒 步骤3: 清理空数据
        if not resampled.empty:
            resampled = resampled.dropna(how='all')
        
        print(f"重采样后: {len(resampled)} 行")
        
        # 🔒 步骤4: 最终验证（应该已经没有非交易时间了）
        if not resampled.empty:
            # 简单验证，确保没有遗漏
            invalid_count = 0
            for ts in resampled.index:
                if not self.is_hk_trading_time(ts):
                    invalid_count += 1
            
            if invalid_count > 0:
                print(f"⚠️  仍有 {invalid_count} 个非交易时间，需要进一步修复")
            else:
                print(f"✅ 时间验证通过: 所有时间戳都在交易时段内")
        
        # 🔒 步骤5: 转换为DataFrame格式，时间戳为字符串
        if resampled.empty:
            return pd.DataFrame()
            
        result = resampled.reset_index()
        if result.columns[0] != 'timestamp':
            result = result.rename(columns={result.columns[0]: 'timestamp'})
        
        # 强制转换为字符串格式
        if pd.api.types.is_datetime64_any_dtype(result['timestamp']):
            result['timestamp'] = result['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 🔒 步骤6: 最终验证
        self._validate_result(result, timeframe)
        
        # 计算压缩比
        if len(trading_data) > 0:
            compression_ratio = len(trading_data) / len(result)
            print(f"✅ {timeframe}: {len(trading_data)} -> {len(result)} 行 (压缩比: {compression_ratio:.1f}:1)")
        
        return result
    
    def _validate_result(self, result: pd.DataFrame, timeframe: str):
        """验证结果格式"""
        if result.empty:
            return
        
        # 检查时间戳格式
        for idx, ts in enumerate(result['timestamp'].head(3)):
            validate_timestamp_format(ts, f"港股重采样({timeframe})[{idx}]")
            
            # 验证时间戳是否在交易时间内
            if not self.is_hk_trading_time(ts):
                raise ValueError(f"❌ 非交易时间: {ts}")
    
    def resample_file(self, input_file: str, output_file: str, timeframe: str):
        """重采样文件"""
        print(f"\n📁 处理文件: {timeframe}")
        
        # 读取数据
        df = pd.read_parquet(input_file)
        original_count = len(df)
        
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
        
        # 重采样
        result = self.resample(df, timeframe)
        
        if not result.empty:
            # 保存
            result.to_parquet(output_file, index=False)
            
            # 验证保存的文件
            saved = pd.read_parquet(output_file)
            if isinstance(saved['timestamp'].iloc[0], str):
                print(f"✅ 保存成功: {saved['timestamp'].iloc[0]} (字符串格式)")
            else:
                raise ValueError(f"❌ 保存验证失败: 时间戳不是字符串格式")
        else:
            print(f"⚠️  {timeframe}: 无有效交易数据")
        
        return result


def hk_batch_resample_fixed(input_file: str, output_dir: str, 
                           timeframes: list = None):
    """港股批量重采样 - 修复版"""
    if timeframes is None:
        timeframes = ['2m', '3m', '5m', '10m', '15m', '30m', '1h', '2h', '4h']
    
    resampler = HKResamplerFixed()
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    print(f"🏢 港股重采样器 - 修复版")
    print(f"📁 源文件: {input_file}")
    print(f"📂 输出目录: {output_dir}")
    print("🔒 严格执行HKEX交易时段规范")
    print("=" * 60)
    
    results = {}
    for tf in timeframes:
        try:
            output_file = output_dir / f"{Path(input_file).stem}_{tf}_fixed.parquet"
            result = resampler.resample_file(input_file, str(output_file), tf)
            results[tf] = len(result) if not result.empty else 0
        except Exception as e:
            print(f"❌ {tf}: 失败 - {e}")
            results[tf] = -1
    
    print("=" * 60)
    print("📊 修复版重采样结果:")
    success_count = 0
    for tf, count in results.items():
        if count > 0:
            print(f"  {tf:>4}: ✅ {count} 行")
            success_count += 1
        elif count == 0:
            print(f"  {tf:>4}: ⚠️  无数据")
        else:
            print(f"  {tf:>4}: ❌ 失败")
    
    print(f"\n🎯 成功率: {success_count}/{len(timeframes)}")
    
    return results


if __name__ == "__main__":
    # 测试修复版重采样器
    import numpy as np
    
    print("🏢 港股重采样器修复版测试")
    print("=" * 50)
    
    # 创建严格的港股交易时间测试数据
    dates = []
    
    # 生成一天的港股交易时间数据
    base_date = "2025-03-05"
    
    # 上午: 09:30-11:59
    morning_range = pd.date_range(f'{base_date} 09:30:00', f'{base_date} 11:59:00', freq='1min')
    dates.extend(morning_range)
    
    # 下午: 13:00-15:59
    afternoon_range = pd.date_range(f'{base_date} 13:00:00', f'{base_date} 15:59:00', freq='1min')
    dates.extend(afternoon_range)
    
    print(f"测试数据: {len(dates)} 行 (严格港股交易时间)")
    
    test_data = pd.DataFrame({
        'open': np.random.normal(400, 5, len(dates)),
        'high': np.random.normal(402, 5, len(dates)),
        'low': np.random.normal(398, 5, len(dates)),
        'close': np.random.normal(400, 5, len(dates)),
        'volume': np.random.randint(1000, 5000, len(dates))
    }, index=dates)
    
    resampler = HKResamplerFixed()
    
    # 测试各种周期
    for tf in ['2m', '5m', '15m', '1h']:
        try:
            result = resampler.resample(test_data, tf)
            if not result.empty:
                sample_ts = result['timestamp'].iloc[0]
                print(f"\n{tf:>4}: ✅ 成功")
                print(f"      时间戳: {sample_ts}")
        except Exception as e:
            print(f"\n{tf:>4}: ❌ 失败 - {e}")
    
    print("\n✅ 港股重采样器修复版测试完成！")
