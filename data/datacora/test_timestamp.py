#!/usr/bin/env python3
"""
测试时间戳处理问题
"""
import pandas as pd
import sys
from pathlib import Path

# 添加重采样模块路径
sys.path.append('/Users/zhangshenshen/HK0920sen-code/resampling')
sys.path.append('/Users/zhangshenshen/HK0920sen-code/resampling/core')

from core.resampling_engine.resampler import OHLCVResamplingStrategy, TimeframeResampler

def test_timestamp_handling():
    print("=== 测试时间戳处理 ===")

    # 读取原始数据
    source_file = '/Users/zhangshenshen/HK0920sen-code/data/0700HK_1min_2025-03-05_2025-09-01.parquet'
    df = pd.read_parquet(source_file)

    print(f"原始数据时间戳类型: {df['timestamp'].dtype}")
    print(f"原始数据前3个时间戳: {df['timestamp'].head(3).tolist()}")

    # 转换为datetime索引（模拟重采样过程）
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)

    print(f"转换后索引类型: {type(df.index)}")
    print(f"索引前3个值: {df.index[:3].tolist()}")

    # 测试重采样
    strategy = OHLCVResamplingStrategy()
    resampler = TimeframeResampler(strategy)

    # 测试10分钟重采样
    resampled = resampler.resample(df, '10m')

    print(f"\n重采样后索引类型: {type(resampled.index)}")
    print(f"重采样后前3个索引: {resampled.index[:3].tolist()}")

    # 重置索引，添加timestamp列（模拟保存过程）
    resampled_df = resampled.reset_index()
    resampled_df.rename(columns={'index': 'timestamp'}, inplace=True)

    print(f"\n重置索引后时间戳类型: {resampled_df['timestamp'].dtype}")
    print(f"重置索引后前3个时间戳: {resampled_df['timestamp'].head(3).tolist()}")

    # 检查是否有任何Unix时间戳
    if resampled_df['timestamp'].dtype == 'int64' or resampled_df['timestamp'].dtype == 'float64':
        print("\n⚠️  警告: 发现整数类型时间戳，可能是Unix时间戳!")
        sample_unix = resampled_df['timestamp'].iloc[0]
        try:
            converted = pd.to_datetime(sample_unix, unit='ms')
            print(f"示例转换: {sample_unix} -> {converted}")
        except:
            print(f"无法转换时间戳: {sample_unix}")
    else:
        print("\n✅ 时间戳格式正确")

    # 保存测试
    test_output = '/tmp/test_resampling.parquet'
    resampled_df.to_parquet(test_output, index=False)

    # 读取验证
    verify_df = pd.read_parquet(test_output)
    print(f"\n保存后验证:")
    print(f"时间戳类型: {verify_df['timestamp'].dtype}")
    print(f"前3个时间戳: {verify_df['timestamp'].head(3).tolist()}")

if __name__ == "__main__":
    test_timestamp_handling()