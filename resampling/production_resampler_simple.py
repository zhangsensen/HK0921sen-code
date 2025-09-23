#!/usr/bin/env python3
"""
简化的生产重采样器
基于极简重采样器，添加必要的生产功能

🔒 核心规则：所有时间戳必须保持人类可读格式 (YYYY-MM-DD HH:MM:SS)
🔒 铁律：禁止使用毫秒时间戳格式 (如 1741182300000)
"""

import pandas as pd
import logging
from pathlib import Path
from hk_resampler import HKResampler
import sys
import os

# 添加utils路径以便导入约束验证器
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from timestamp_constraint_validator import validate_resampling_output, TimestampConstraintError

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProductionResampler:
    """简化的生产重采样器 - 基于港股重采样器核心"""

    def __init__(self, source_file: str, output_dir: str = None):
        self.source_file = source_file
        # 默认输出到 data/raw_data 目录
        if output_dir is None:
            self.output_dir = Path("/Users/zhangshenshen/HK0920sen-code/data/raw_data")
        else:
            self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 使用港股重采样器作为核心
        self.core_resampler = HKResampler()
        
        logger.info(f"生产重采样器初始化")
        logger.info(f"源文件: {source_file}")
        logger.info(f"输出目录: {output_dir}")
    
    def run(self, timeframes: list = None):
        """运行生产重采样"""
        if timeframes is None:
            timeframes = ["10m", "15m", "30m", "1h", "2h", "4h"]
        
        logger.info(f"开始生产重采样，目标时间周期: {timeframes}")
        
        # 读取源数据
        logger.info("读取源数据...")
        df = pd.read_parquet(self.source_file)
        
        # 🔒 核心约束：设置时间索引为datetime格式
        if 'timestamp' in df.columns:
            # 首先验证输入数据格式
            for i in range(min(3, len(df))):
                sample_ts = str(df['timestamp'].iloc[i])
                # 检查是否为数值时间戳格式（这是被严格禁止的）
                if sample_ts.replace('.', '', 1).isdigit() and len(sample_ts.replace('.', '', 1)) >= 10:
                    readable_time = "YYYY-MM-DD HH:MM:SS"
                    try:
                        from datetime import datetime
                        if len(sample_ts.replace('.', '', 1)) >= 13:
                            readable_time = datetime.fromtimestamp(float(sample_ts) / 1000).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            readable_time = datetime.fromtimestamp(float(sample_ts)).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass
                    raise ValueError(f"❌ 严重违反核心铁律：源数据包含数值时间戳格式 {sample_ts} (应该是 {readable_time})")

            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
        
        logger.info(f"源数据: {len(df)} 行")
        
        results = {}
        
        # 批量重采样
        for timeframe in timeframes:
            try:
                logger.info(f"重采样到 {timeframe}...")
                
                # 使用核心重采样器
                result = self.core_resampler.resample(df, timeframe)
                
                # 🔒 核心铁律：确保时间戳为字符串格式后再保存
                if pd.api.types.is_datetime64_any_dtype(result['timestamp']):
                    result['timestamp'] = result['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # 验证时间戳为字符串格式
                sample_ts = result['timestamp'].iloc[0]
                if not isinstance(sample_ts, str):
                    raise ValueError(f"❌ 保存前验证失败：时间戳不是字符串格式: {sample_ts} (类型: {type(sample_ts)})")
                
                # 保存结果
                output_file = self.output_dir / f"{Path(self.source_file).stem}_{timeframe}.parquet"
                result.to_parquet(output_file, index=False)
                
                # 🔒 核心约束：验证时间戳格式
                saved = pd.read_parquet(output_file)
                try:
                    validate_resampling_output(saved, f"生产验证({timeframe})")
                    constraint_status = '✅ 通过核心约束验证'
                except TimestampConstraintError as e:
                    # 删除错误文件
                    if os.path.exists(output_file):
                        os.remove(output_file)
                    raise ValueError(f"❌ 生产约束验证失败，已删除错误文件: {e}")

                sample_ts = str(saved['timestamp'].iloc[0])
                results[timeframe] = {
                    'output_file': str(output_file),
                    'rows': len(result),
                    'compression_ratio': len(df) / len(result),
                    'timestamp_format': 'datetime',  # 强制为datetime
                    'sample_timestamp': sample_ts,
                    'constraint_status': constraint_status
                }
                
                logger.info(f"✅ {timeframe}: {len(df)} -> {len(result)} 行")
                
            except Exception as e:
                logger.error(f"❌ {timeframe} 重采样失败: {e}")
                results[timeframe] = {'error': str(e)}
        
        logger.info("生产重采样完成")
        return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='生产重采样器')
    parser.add_argument('source', help='源数据文件')
    parser.add_argument('output', help='输出目录')
    parser.add_argument('--timeframes', nargs='+', 
                       default=["10m", "15m", "30m", "1h", "2h", "4h"],
                       help='目标时间周期')
    
    args = parser.parse_args()
    
    resampler = ProductionResampler(args.source, args.output)
    results = resampler.run(args.timeframes)
    
    print("\n📊 重采样结果:")
    print("=" * 60)
    for tf, result in results.items():
        if 'error' in result:
            print(f"{tf:>4}: ❌ {result['error']}")
        else:
            print(f"{tf:>4}: ✅ {result['rows']} 行, 格式: {result['timestamp_format']}")
            print(f"      时间戳示例: {result['sample_timestamp']}")
