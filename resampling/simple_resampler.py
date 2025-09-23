#!/usr/bin/env python3
"""
极简重采样器 - 专注核心功能
替代复杂的重采样框架，只做重采样这一件事

特点:
- 100行代码完成所有功能
- 支持港股交易时间
- 🔒 核心规则：确保时间戳为人类可读格式 (YYYY-MM-DD HH:MM:SS)
- 🔒 铁律：禁止使用毫秒时间戳格式 (如 1741182300000)
- 简单直接，无过度设计
"""

import pandas as pd
from typing import Dict, Optional
from pathlib import Path
import sys
import os

# 添加utils路径以便导入约束验证器
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from timestamp_constraint_validator import validate_resampling_output, TimestampConstraintError


class SimpleResampler:
    """极简重采样器 - 只做重采样，不做其他"""
    
    # 支持的时间周期映射
    TIMEFRAMES = {
        "1m": "1min", "2m": "2min", "3m": "3min", "5m": "5min",
        "10m": "10min", "15m": "15min", "30m": "30min", 
        "1h": "1h", "2h": "2h", "4h": "4h", "1d": "1D"
    }
    
    # OHLCV聚合规则
    AGG_RULES = {
        'open': 'first', 'high': 'max', 'low': 'min', 
        'close': 'last', 'volume': 'sum', 'turnover': 'sum'
    }
    
    def resample(self, data: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """重采样数据
        
        Args:
            data: 输入数据，index必须是DatetimeIndex
            timeframe: 目标时间周期 (如 "1h", "5m")
            
        Returns:
            重采样后的数据，时间戳为人类可读格式
        """
        if timeframe not in self.TIMEFRAMES:
            raise ValueError(f"不支持的时间周期: {timeframe}")
        
        if not isinstance(data.index, pd.DatetimeIndex):
            raise ValueError("数据index必须是DatetimeIndex")
        
        rule = self.TIMEFRAMES[timeframe]
        
        # 构建聚合字典（只包含存在的列）
        agg_dict = {col: func for col, func in self.AGG_RULES.items() 
                   if col in data.columns}
        
        # 港股特殊处理：小时级重采样使用offset对齐9:30开盘
        if timeframe in ["1h", "2h", "4h"]:
            resampled = data.resample(
                rule, label='left', closed='left', offset='30min'
            ).agg(agg_dict)
        else:
            resampled = data.resample(rule, label='left').agg(agg_dict)
        
        # 删除空行
        resampled = resampled.dropna(how='all')
        
        # 过滤交易时间（简单版本）
        if not resampled.empty:
            resampled = self._filter_trading_hours(resampled)
        
        # 🔒 核心约束：永久确保时间戳为人类可读格式 "YYYY-MM-DD HH:MM:SS"
        result = resampled.reset_index()

        # 重命名索引列为timestamp
        if result.columns[0] != 'timestamp':
            result = result.rename(columns={result.columns[0]: 'timestamp'})

        # 🔒 核心铁律：强制转换为人类可读字符串格式
        # 将datetime格式转换为字符串格式，彻底消除数值时间戳
        if pd.api.types.is_datetime64_any_dtype(result['timestamp']):
            result['timestamp'] = result['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 🔒 严格验证：确保时间戳为字符串格式
        for idx, ts in enumerate(result['timestamp'].head(3)):
            if not isinstance(ts, str):
                raise ValueError(f"❌ 严重错误：时间戳不是字符串格式，第{idx}行: {ts} (类型: {type(ts)})")
            
            # 验证字符串格式是否正确
            if not (len(ts) == 19 and ts[4] == '-' and ts[7] == '-' and ts[10] == ' ' and ts[13] == ':' and ts[16] == ':'):
                raise ValueError(f"❌ 时间戳格式不正确，第{idx}行: {ts}")
        
        # 🔒 铁律：使用全局约束验证器进行最终验证
        try:
            result = validate_resampling_output(result, f"重采样({timeframe})")
        except TimestampConstraintError as e:
            raise ValueError(f"❌ 重采样约束验证失败: {e}")
        
        return result
    
    def _filter_trading_hours(self, df: pd.DataFrame) -> pd.DataFrame:
        """简单的交易时间过滤"""
        if df.empty:
            return df
        
        # 只保留工作日
        df = df[df.index.dayofweek < 5]
        
        # 港股交易时间：9:00-17:00（宽松过滤）
        time = df.index.time
        trading_hours = (time >= pd.Timestamp("09:00").time()) & \
                       (time <= pd.Timestamp("17:00").time())
        
        return df[trading_hours]
    
    def resample_file(self, input_file: str, output_file: str, timeframe: str):
        """重采样文件"""
        print(f"重采样 {input_file} -> {output_file} ({timeframe})")
        
        # 读取数据
        df = pd.read_parquet(input_file)
        
        # 设置时间索引
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
        
        # 重采样
        result = self.resample(df, timeframe)
        
        # 🔒 核心铁律：保存前进行最终约束验证
        try:
            result = validate_resampling_output(result, f"保存前({timeframe})")
        except TimestampConstraintError as e:
            raise ValueError(f"❌ 保存前约束验证失败: {e}")

        # 保存
        result.to_parquet(output_file, index=False)

        # 🔒 保存后验证：确保文件格式正确
        try:
            saved_df = pd.read_parquet(output_file)
            validate_resampling_output(saved_df, f"保存后验证({timeframe})")
        except TimestampConstraintError as e:
            # 如果保存后验证失败，删除错误文件
            import os
            if os.path.exists(output_file):
                os.remove(output_file)
            raise ValueError(f"❌ 保存后验证失败，已删除错误文件: {e}")
        print(f"✅ 完成: {len(df)} -> {len(result)} 行")
        
        # 验证保存的格式
        saved_check = pd.read_parquet(output_file)
        if 'timestamp' in saved_check.columns:
            sample_ts = str(saved_check['timestamp'].iloc[0])
            if sample_ts.isdigit() and len(sample_ts) >= 13:
                print(f"⚠️  警告: 检测到毫秒时间戳格式: {sample_ts}")
            else:
                print(f"✅ 时间戳格式正确: {sample_ts}")
        
        return result


def quick_resample(data: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """快速重采样函数 - 一行调用"""
    return SimpleResampler().resample(data, timeframe)


def batch_resample(input_file: str, output_dir: str, timeframes: list):
    """批量重采样"""
    resampler = SimpleResampler()
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    
    for tf in timeframes:
        output_file = output_dir / f"{Path(input_file).stem}_{tf}.parquet"
        resampler.resample_file(input_file, str(output_file), tf)


if __name__ == "__main__":
    # 使用示例
    import numpy as np
    
    print("🔄 极简重采样器")
    print("=" * 50)
    print("✅ 时间戳输出格式: 2025-03-25 00:00:00")
    print("❌ 禁止格式: 1741170360000 (毫秒时间戳)")
    print("=" * 50)
    
    # 创建测试数据
    dates = pd.date_range('2025-09-22 09:30:00', '2025-09-22 15:59:00', freq='1min')
    test_data = pd.DataFrame({
        'open': np.random.normal(400, 5, len(dates)),
        'high': np.random.normal(402, 5, len(dates)),
        'low': np.random.normal(398, 5, len(dates)),
        'close': np.random.normal(400, 5, len(dates)),
        'volume': np.random.randint(1000, 5000, len(dates))
    }, index=dates)
    
    # 重采样测试
    resampler = SimpleResampler()
    
    for timeframe in ["5m", "15m", "1h"]:
        result = resampler.resample(test_data, timeframe)
        sample_ts = result['timestamp'].iloc[0]
        print(f"{timeframe:>3}: {len(test_data):>4} -> {len(result):>3} 行, 时间戳: {sample_ts}")
    
    print("\n✅ 测试完成！时间戳格式正确。")
