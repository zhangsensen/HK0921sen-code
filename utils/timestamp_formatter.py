#!/usr/bin/env python3
"""
时间戳格式化器模块
确保所有时间戳都以标准格式存储和显示

核心约束：
1. 时间戳必须以 datetime 格式存储
2. 永远不允许使用毫秒时间戳 (如 1741221000000)
3. 时间戳格式化是不可更改的约束
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Union, Dict, Any
import logging

class TimestampFormatter:
    """时间戳格式化器
    
    这个类确保所有时间数据都以人类可读的格式存储和处理
    """
    
    # 标准时间格式 - 这是不可更改的约束
    STANDARD_FORMAT = "%Y-%m-%d %H:%M:%S"
    
    def __init__(self):
        self.logger = logging.getLogger("TimestampFormatter")
    
    @classmethod
    def format_timestamp(cls, timestamp: Union[pd.Timestamp, datetime, str, int, float]) -> str:
        """格式化单个时间戳为标准可读格式
        
        Args:
            timestamp: 各种格式的时间戳
            
        Returns:
            标准格式的时间字符串: "YYYY-MM-DD HH:MM:SS"
            
        Raises:
            ValueError: 如果时间戳无法解析
        """
        try:
            # 处理不同类型的时间戳输入
            if isinstance(timestamp, (int, float)):
                # 处理毫秒时间戳 (如 1741221000000)
                if timestamp > 1e12:  # 毫秒时间戳
                    timestamp = timestamp / 1000
                # 转换Unix时间戳
                dt = pd.to_datetime(timestamp, unit='s')
            elif isinstance(timestamp, str):
                # 字符串时间戳
                dt = pd.to_datetime(timestamp)
            elif isinstance(timestamp, (pd.Timestamp, datetime)):
                # 已经是时间对象
                dt = pd.to_datetime(timestamp)
            else:
                # 尝试直接转换
                dt = pd.to_datetime(timestamp)
            
            # 格式化为标准格式
            return dt.strftime(cls.STANDARD_FORMAT)
            
        except Exception as e:
            raise ValueError(f"无法格式化时间戳 {timestamp}: {e}")
    
    @classmethod
    def _format_dataframe(cls, df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """格式化DataFrame中的时间戳列

        Args:
            df: 包含时间戳的DataFrame
            timestamp_col: 时间戳列名

        Returns:
            格式化后的DataFrame（只有timestamp列）
        """
        if df.empty:
            return df

        df_copy = df.copy()

        # 确保有时间戳列
        if timestamp_col not in df_copy.columns:
            raise ValueError(f"DataFrame中没有找到时间戳列: {timestamp_col}")

        try:
            # 确保timestamp列是datetime类型
            if not pd.api.types.is_datetime64_any_dtype(df_copy[timestamp_col]):
                df_copy[timestamp_col] = pd.to_datetime(df_copy[timestamp_col])

            # 移除冗余的timestamp_readable字段（如果存在）
            if 'timestamp_readable' in df_copy.columns:
                df_copy = df_copy.drop('timestamp_readable', axis=1)

            return df_copy

        except Exception as e:
            raise ValueError(f"格式化DataFrame时间戳失败: {e}")
    
    @classmethod
    def validate_timestamp_format(cls, df: pd.DataFrame, timestamp_col: str = 'timestamp') -> Dict[str, Any]:
        """验证DataFrame的时间戳格式是否符合标准

        Args:
            df: 要验证的DataFrame
            timestamp_col: 时间戳列名

        Returns:
            验证结果字典
        """
        result = {
            'valid': False,
            'has_timestamp_col': False,
            'timestamp_type': None,
            'sample_timestamps': [],
            'issues': []
        }

        if df.empty:
            result['issues'].append("DataFrame为空")
            return result

        # 检查时间戳列是否存在
        if timestamp_col not in df.columns:
            result['issues'].append(f"缺少时间戳列: {timestamp_col}")
            return result

        result['has_timestamp_col'] = True
        result['timestamp_type'] = str(df[timestamp_col].dtype)

        # 获取样本时间戳
        sample_size = min(3, len(df))
        for i in range(sample_size):
            ts = df[timestamp_col].iloc[i]
            result['sample_timestamps'].append({
                'original': str(ts),
                'type': str(type(ts))
            })

        # 检查时间戳格式
        try:
            if pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
                result['valid'] = True
            else:
                result['issues'].append("timestamp列不是datetime类型")

        except Exception as e:
            result['issues'].append(f"时间戳格式验证失败: {e}")

        return result
    
    @classmethod
    def enforce_readable_format(cls, df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """强制执行可读时间戳格式约束
        
        这是一个不可绕过的约束检查和修复函数
        """
        if df.empty:
            return df
        
        # 验证当前格式
        validation = cls.validate_timestamp_format(df, timestamp_col)
        
        if validation['valid']:
            # 格式已经正确，直接返回
            return df
        
        # 格式不正确，强制修复
        logging.warning(f"检测到时间戳格式问题，正在强制修复: {validation['issues']}")
        
        try:
            return cls._format_dataframe(df, timestamp_col)
        except Exception as e:
            raise RuntimeError(f"无法修复时间戳格式，这违反了系统约束: {e}")


class TimestampConstraintValidator:
    """时间戳约束验证器
    
    确保整个系统遵循时间戳格式约束
    """
    
    @staticmethod
    def validate_resampling_output(df: pd.DataFrame, operation: str = "重采样") -> pd.DataFrame:
        """验证重采样输出的时间戳格式约束
        
        Args:
            df: 重采样输出的DataFrame
            operation: 操作名称（用于日志）
            
        Returns:
            格式化后的DataFrame
            
        Raises:
            RuntimeError: 如果无法满足时间戳约束
        """
        if df.empty:
            return df
        
        logger = logging.getLogger("TimestampConstraintValidator")
        
        # 强制执行约束
        try:
            formatted_df = TimestampFormatter.enforce_readable_format(df)
            
            # 再次验证
            validation = TimestampFormatter.validate_timestamp_format(formatted_df)
            
            if not validation['valid']:
                raise RuntimeError(
                    f"{operation}输出不满足时间戳格式约束: {validation['issues']}"
                )
            
            logger.info(f"✅ {operation}输出通过时间戳格式约束验证")
            return formatted_df
            
        except Exception as e:
            raise RuntimeError(
                f"❌ {operation}违反了时间戳格式约束，这是不允许的: {e}"
            )
    
    @staticmethod
    def check_millisecond_timestamps(df: pd.DataFrame) -> bool:
        """检查是否存在毫秒时间戳格式（这是被禁止的）
        
        Returns:
            True: 如果发现毫秒时间戳
            False: 如果格式正确
        """
        if df.empty:
            return False
        
        # 检查timestamp列
        if 'timestamp' in df.columns:
            # 检查是否有看起来像Unix/毫秒时间戳的值
            timestamp_col = df['timestamp']
            
            # 如果是数值类型且值很大，可能是Unix或毫秒时间戳
            if pd.api.types.is_numeric_dtype(timestamp_col):
                max_val = timestamp_col.max()
                if max_val >= 1e9:  # 10位通常是Unix秒，13位及以上通常是毫秒
                    return True
        
        # 检查字符串格式
        for col in df.columns:
            if 'timestamp' in col.lower():
                sample_val = str(df[col].iloc[0])
                if sample_val.isdigit() and len(sample_val) >= 10:
                    return True
        
        return False


# 全局约束检查函数
def ensure_readable_timestamps(df: pd.DataFrame, operation: str = "数据处理") -> pd.DataFrame:
    """全局时间戳格式约束检查函数
    
    这个函数应该在所有重采样操作后调用
    """
    return TimestampConstraintValidator.validate_resampling_output(df, operation)


# 约束常量 - 不可修改
TIMESTAMP_FORMAT_CONSTRAINT = {
    'format': 'datetime',
    'description': '时间戳必须使用 datetime 格式',
    'required_columns': ['timestamp'],
    'forbidden_formats': ['毫秒时间戳', 'Unix时间戳数字', '其他不可读格式'],
    'enforcement': '强制执行，不可绕过'
}
