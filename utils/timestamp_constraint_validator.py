#!/usr/bin/env python3
"""
时间戳约束验证器 - 核心铁律执行模块

🔒 核心铁律：所有时间戳必须保持人类可读格式 (YYYY-MM-DD HH:MM:SS)
🔒 禁止格式：毫秒时间戳格式 (如 1741182300000)
🔒 强制执行：不可绕过的系统约束

此模块确保整个系统中所有时间戳都遵循人类可读格式
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Union, Dict, Any, List
import sys

# 简化的logging处理
class SimpleLogger:
    def info(self, msg):
        print(f"INFO: {msg}")
    def warning(self, msg):
        print(f"WARNING: {msg}")
    def error(self, msg):
        print(f"ERROR: {msg}")

class LoggingModule:
    def getLogger(self, name):
        return SimpleLogger()

# 创建简单的logging模块
try:
    # 先检查是否已经有logging模块被导入
    if 'logging' in globals():
        # 如果已经有logging但不是标准库，替换为标准库
        import logging as real_logging
        if not hasattr(logging, 'getLogger'):
            logging = real_logging
    else:
        import logging
except ImportError:
    logging = LoggingModule()

class TimestampConstraintError(Exception):
    """时间戳约束违反异常"""
    pass

class TimestampConstraintValidator:
    """时间戳约束验证器 - 强制执行人类可读时间戳格式"""

    # 🚫 禁止的时间戳格式
    FORBIDDEN_FORMATS = [
        'millisecond_timestamp',  # 毫秒时间戳 (如 1741182300000)
        'unix_timestamp_numeric', # Unix时间戳数字
        'epoch_milliseconds',      # Epoch毫秒数
    ]

    # ✅ 允许的时间戳格式
    ALLOWED_FORMATS = [
        'datetime64',             # pandas datetime64
        'datetime_object',        # Python datetime object
        'iso_string',             # ISO格式字符串 (YYYY-MM-DD HH:MM:SS)
    ]

    def __init__(self):
        self.logger = logging.getLogger("TimestampConstraintValidator")

    def validate_timestamp_format(self, timestamp: Any, context: str = "Unknown") -> bool:
        """
        验证单个时间戳格式是否符合约束

        Args:
            timestamp: 要验证的时间戳
            context: 验证上下文（用于错误信息）

        Returns:
            bool: True表示格式正确

        Raises:
            TimestampConstraintError: 如果格式违反约束
        """
        try:
            def _human_readable(example_value: float, divisor: int) -> str:
                """生成示例的人类可读时间戳"""
                try:
                    dt = datetime.fromtimestamp(example_value / divisor)
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    return 'YYYY-MM-DD HH:MM:SS'

            # 检查并拒绝纯数字的Unix/毫秒时间戳（这是被禁止的）
            if isinstance(timestamp, (int, np.integer)):
                int_value = int(timestamp)
                abs_value = abs(int_value)
                digit_count = len(str(abs_value))
                if digit_count >= 10:
                    # 10位通常为Unix秒时间戳，13位及以上通常为毫秒
                    divisor = 1000 if digit_count >= 13 else 1
                    readable_time = _human_readable(float(int_value), divisor)
                    format_name = "毫秒时间戳" if digit_count >= 13 else "Unix时间戳"
                    raise TimestampConstraintError(
                        f"❌ {context}: 检测到被禁止的{format_name}格式 {timestamp} "
                        f"(应该是 {readable_time})"
                    )

            if isinstance(timestamp, (float, np.floating)):
                if not np.isfinite(timestamp):
                    raise TimestampConstraintError(f"❌ {context}: 时间戳不是有效数字 {timestamp}")
                float_value = float(timestamp)
                abs_value = abs(int(float_value))
                digit_count = len(str(abs_value))
                if digit_count >= 10:
                    divisor = 1000 if digit_count >= 13 else 1
                    readable_time = _human_readable(float_value, divisor)
                    format_name = "毫秒时间戳" if digit_count >= 13 else "Unix时间戳"
                    raise TimestampConstraintError(
                        f"❌ {context}: 检测到被禁止的{format_name}格式 {timestamp} "
                        f"(应该是 {readable_time})"
                    )

            # 检查字符串格式
            if isinstance(timestamp, str):
                stripped = timestamp.strip()
                if stripped.isdigit() and len(stripped) >= 10:
                    digits = len(stripped)
                    divisor = 1000 if digits >= 13 else 1
                    readable_time = _human_readable(float(int(stripped)), divisor)
                    format_name = "毫秒时间戳" if digits >= 13 else "Unix时间戳"
                    raise TimestampConstraintError(
                        f"❌ {context}: 检测到被禁止的{format_name}字符串 '{timestamp}' "
                        f"(应该是 '{readable_time}')"
                    )
                # 处理可能带小数的字符串数字，例如 "1741182300.0"
                numeric_candidate = stripped.replace(".", "", 1)
                if numeric_candidate.isdigit() and len(numeric_candidate) >= 10:
                    digits = len(numeric_candidate)
                    divisor = 1000 if digits >= 13 else 1
                    readable_time = _human_readable(float(stripped), divisor)
                    format_name = "毫秒时间戳" if digits >= 13 else "Unix时间戳"
                    raise TimestampConstraintError(
                        f"❌ {context}: 检测到被禁止的{format_name}字符串 '{timestamp}' "
                        f"(应该是 '{readable_time}')"
                    )

            # 检查是否为有效的datetime格式
            try:
                dt = pd.to_datetime(timestamp)
                if pd.isna(dt):
                    raise TimestampConstraintError(f"❌ {context}: 时间戳为空值")
                return True
            except Exception as e:
                raise TimestampConstraintError(f"❌ {context}: 无法解析时间戳 '{timestamp}': {e}")

        except TimestampConstraintError:
            raise
        except Exception as e:
            raise TimestampConstraintError(f"❌ {context}: 时间戳验证失败 '{timestamp}': {e}")

    def validate_dataframe_timestamps(self, df: pd.DataFrame,
                                    timestamp_col: str = 'timestamp',
                                    context: str = "DataFrame") -> pd.DataFrame:
        """
        验证DataFrame中的时间戳列是否符合约束

        Args:
            df: 要验证的DataFrame
            timestamp_col: 时间戳列名
            context: 验证上下文

        Returns:
            验证通过的DataFrame

        Raises:
            TimestampConstraintError: 如果格式违反约束
        """
        if df.empty:
            return df

        if timestamp_col not in df.columns:
            raise TimestampConstraintError(f"❌ {context}: DataFrame中没有找到时间戳列 '{timestamp_col}'")

        try:
            # 检查前几个时间戳样本
            sample_size = min(5, len(df))
            for i in range(sample_size):
                timestamp = df[timestamp_col].iloc[i]
                self.validate_timestamp_format(timestamp, f"{context}[行{i}]")

            # 检查整个列的数据类型
            if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
                # 尝试转换为datetime格式
                df[timestamp_col] = pd.to_datetime(df[timestamp_col])

                # 再次验证转换后的格式
                for i in range(sample_size):
                    timestamp = df[timestamp_col].iloc[i]
                    sample_ts = str(timestamp)
                    numeric_candidate = sample_ts.replace('.', '', 1)
                    if numeric_candidate.isdigit() and len(numeric_candidate) >= 10:
                        raise TimestampConstraintError(
                            f"❌ {context}: 转换后仍检测到数值时间戳格式 {sample_ts}"
                        )

            self.logger.info(f"✅ {context}: 时间戳格式验证通过 ({len(df)} 行)")
            return df

        except TimestampConstraintError:
            raise
        except Exception as e:
            raise TimestampConstraintError(f"❌ {context}: DataFrame时间戳验证失败: {e}")

    def enforce_readable_format(self, df: pd.DataFrame,
                               timestamp_col: str = 'timestamp',
                               context: str = "数据处理") -> pd.DataFrame:
        """
        强制执行可读时间戳格式约束

        Args:
            df: 要处理的DataFrame
            timestamp_col: 时间戳列名
            context: 处理上下文

        Returns:
            符合约束的DataFrame

        Raises:
            TimestampConstraintError: 如果无法满足约束
        """
        if df.empty:
            return df

        try:
            # 验证当前格式
            validated_df = self.validate_dataframe_timestamps(df, timestamp_col, context)

            # 确保timestamp列是datetime格式
            if not pd.api.types.is_datetime64_any_dtype(validated_df[timestamp_col]):
                validated_df[timestamp_col] = pd.to_datetime(validated_df[timestamp_col])

            # 最终验证
            sample_ts = str(validated_df[timestamp_col].iloc[0])
            numeric_candidate = sample_ts.replace('.', '', 1)
            if numeric_candidate.isdigit() and len(numeric_candidate) >= 10:
                raise TimestampConstraintError(
                    f"❌ {context}: 强制约束失败，仍存在数值时间戳格式 {sample_ts}"
                )

            return validated_df

        except TimestampConstraintError:
            raise
        except Exception as e:
            raise TimestampConstraintError(f"❌ {context}: 强制执行时间戳约束失败: {e}")

    def check_resampling_output(self, df: pd.DataFrame,
                               operation: str = "重采样") -> pd.DataFrame:
        """
        专门用于重采样输出的约束检查

        Args:
            df: 重采样输出的DataFrame
            operation: 操作名称

        Returns:
            验证通过的DataFrame

        Raises:
            TimestampConstraintError: 如果违反约束
        """
        context = f"{operation}输出"
        return self.enforce_readable_format(df, 'timestamp', context)

    def log_constraint_violation(self, message: str, level: str = "ERROR"):
        """记录约束违反信息"""
        log_func = getattr(self.logger, level.lower(), self.logger.error)
        log_func(f"🔒 时间戳约束违反: {message}")

        # 严重错误时输出到stderr
        if level.upper() in ["ERROR", "CRITICAL"]:
            print(f"❌ 时间戳约束违反: {message}", file=sys.stderr)

# 全局验证器实例
_global_validator = TimestampConstraintValidator()

def validate_timestamps(df: pd.DataFrame, context: str = "数据处理") -> pd.DataFrame:
    """
    全局时间戳验证函数

    Args:
        df: 要验证的DataFrame
        context: 验证上下文

    Returns:
        验证通过的DataFrame

    Raises:
        TimestampConstraintError: 如果违反约束
    """
    return _global_validator.enforce_readable_format(df, 'timestamp', context)

def validate_resampling_output(df: pd.DataFrame, operation: str = "重采样") -> pd.DataFrame:
    """
    专门用于重采样输出的全局验证函数

    Args:
        df: 重采样输出的DataFrame
        operation: 操作名称

    Returns:
        验证通过的DataFrame

    Raises:
        TimestampConstraintError: 如果违反约束
    """
    return _global_validator.check_resampling_output(df, operation)

# 核心约束常量 - 不可修改
TIMESTAMP_CONSTRAINTS = {
    'required_format': 'datetime64[ns]',
    'human_readable': True,
    'forbidden_millisecond': True,
    'enforcement_level': 'STRICT',  # STRICT, WARN, DISABLED
    'description': '所有时间戳必须保持人类可读格式 (YYYY-MM-DD HH:MM:SS)',
    'prohibited_formats': ['millisecond_timestamp', 'epoch_milliseconds']
}

if __name__ == "__main__":
    # 测试约束验证器
    print("🔒 测试时间戳约束验证器")
    print("=" * 50)

    validator = TimestampConstraintValidator()

    # 测试数据
    test_cases = [
        ("有效的datetime", pd.Timestamp('2025-03-05 09:30:00')),
        ("有效的字符串", "2025-03-05 09:30:00"),
        ("被禁止的毫秒时间戳", 1741182300000),
        ("被禁止的毫秒字符串", "1741182300000"),
    ]

    for name, test_value in test_cases:
        try:
            result = validator.validate_timestamp_format(test_value, name)
            print(f"✅ {name}: 通过验证")
        except TimestampConstraintError as e:
            print(f"❌ {name}: {e}")

    print("\n🔒 核心铁律已建立")
    print("✅ 所有时间戳必须保持人类可读格式")
    print("❌ 禁止使用毫秒时间戳格式 (如 1741182300000)")
