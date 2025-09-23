#!/usr/bin/env python3
"""
实时交易友好的重采样引擎
解决传统重采样导致的交易机会流失问题

核心规则：所有时间戳必须保持人类可读格式 (YYYY-MM-DD HH:MM:SS)
禁止使用毫秒时间戳格式 (如 1741182300000)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

class RealTimeResamplingStrategy:
    """实时交易友好的重采样策略"""

    def __init__(self):
        # 实时友好的时间框架
        self.realtime_timeframes = {
            "1min": "1min",
            "5min": "5min",
            "15min": "15min",
            "30min": "30min",
            "1h": "1h"
        }

        # 港股交易时段友好的重采样规则
        self.hk_trading_rules = {
            # 30分钟重采样：完美对齐港股交易时段
            "30min": {
                "rule": "30min",
                "label": "left",     # 用开始时间标记
                "closed": "left",    # 左闭右开区间
                "offset": "0min"     # 无偏移
            },
            # 15分钟重采样：更高频
            "15min": {
                "rule": "15min",
                "label": "left",
                "closed": "left",
                "offset": "0min"
            },
            # 5分钟重采样：高频交易
            "5min": {
                "rule": "5min",
                "label": "left",
                "closed": "left",
                "offset": "0min"
            },
            # 1分钟重采样：实时监控
            "1min": {
                "rule": "1min",
                "label": "left",
                "closed": "left",
                "offset": "0min"
            }
        }

        import logging
        self.logger = logging.getLogger("RealTimeResampling." + __name__)

    def resample_realtime(self, data: pd.DataFrame, target_timeframe: str,
                         current_time: Optional[datetime] = None) -> pd.DataFrame:
        """
        实时友好的重采样方法

        Args:
            data: 原始数据
            target_timeframe: 目标时间框架
            current_time: 当前时间（用于滚动窗口）

        Returns:
            重采样后的数据，时间戳代表窗口开始时间
        """
        try:
            if data.empty:
                return data

            # 🔒 核心约束：确保时间索引为datetime格式
            if not isinstance(data.index, pd.DatetimeIndex):
                data.index = pd.to_datetime(data.index)

            # 获取重采样规则
            resample_config = self.hk_trading_rules.get(target_timeframe)
            if not resample_config:
                raise ValueError(f"不支持的时间框架: {target_timeframe}")

            # 构建聚合字典
            agg_dict = {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum"
            }

            # 添加可选列
            for col in ['turnover', 'vwap', 'count']:
                if col in data.columns:
                    agg_dict[col] = "sum"

            # 执行实时友好的重采样
            resampled = data.resample(
                rule=resample_config["rule"],
                label=resample_config["label"],
                closed=resample_config["closed"],
                offset=resample_config["offset"]
            ).agg(agg_dict)

            # 删除空行
            resampled.dropna(how="all", inplace=True)

            # 过滤交易时间
            resampled = self._filter_trading_hours(resampled)

            # 数据质量验证
            resampled = self._validate_resampled_data(resampled, target_timeframe)

            # 🔒 核心约束验证：确保输出时间戳为人类可读格式
            if not resampled.empty:
                # 重置索引以检查timestamp列
                temp_df = resampled.reset_index()
                if 'timestamp' in temp_df.columns:
                    sample_ts = str(temp_df['timestamp'].iloc[0])
                    if sample_ts.isdigit() and len(sample_ts) >= 13:
                        raise ValueError(f"❌ 违反核心约束：检测到毫秒时间戳格式 {sample_ts}")

                    # 确保timestamp列是datetime格式
                    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
                    resampled = temp_df.set_index('timestamp')

            self.logger.info(f"实时重采样完成: {len(resampled)} 行, 时间框架: {target_timeframe}")
            return resampled

        except Exception as e:
            self.logger.error(f"实时重采样失败: {e}")
            raise

    def create_rolling_windows(self, data: pd.DataFrame, window_size: str = "30min") -> pd.DataFrame:
        """
        创建滚动窗口重采样，每分钟更新

        Args:
            data: 原始数据
            window_size: 窗口大小 ("30min", "15min", "5min")

        Returns:
            滚动窗口重采样结果
        """
        try:
            if data.empty:
                return data

            # 获取最新时间
            latest_time = data.index.max()

            # 生成滚动窗口
            windows = []
            current_time = latest_time

            # 回溯生成滚动窗口（最多生成最近100个窗口）
            for i in range(100):
                window_end = current_time - pd.Timedelta(minutes=i)
                window_start = window_end - pd.Timedelta(window_size)

                # 确保在交易时间内
                if self._is_trading_time(window_start) and self._is_trading_time(window_end):
                    window_data = data.loc[window_start:window_end]

                    if not window_data.empty:
                        # 计算窗口统计
                        window_stats = {
                            'timestamp': window_start,
                            'open': window_data['open'].iloc[0],
                            'high': window_data['high'].max(),
                            'low': window_data['low'].min(),
                            'close': window_data['close'].iloc[-1],
                            'volume': window_data['volume'].sum()
                        }

                        # 添加可选列
                        for col in ['turnover', 'vwap', 'count']:
                            if col in window_data.columns:
                                if col == 'count':
                                    window_stats[col] = window_data[col].sum()
                                else:
                                    window_stats[col] = window_data[col].sum()

                        windows.append(window_stats)

                # 如果窗口开始时间超出数据范围，停止
                if window_start < data.index.min():
                    break

            # 转换为DataFrame并按时间排序
            if windows:
                result_df = pd.DataFrame(windows)
                result_df.set_index('timestamp', inplace=True)
                result_df.sort_index(inplace=True)
                return result_df
            else:
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"滚动窗口重采样失败: {e}")
            return pd.DataFrame()

    def _filter_trading_hours(self, df: pd.DataFrame) -> pd.DataFrame:
        """过滤港股交易时间"""
        if df.empty:
            return df

        # 过滤周末
        df = df[df.index.dayofweek < 5]

        if df.empty:
            return df

        # 港股交易时间
        time = df.index.time
        morning_session = (time >= datetime.time(9, 30)) & (time < datetime.time(12, 0))
        afternoon_session = (time >= datetime.time(13, 0)) & (time < datetime.time(16, 1))

        df = df[morning_session | afternoon_session]
        return df

    def _is_trading_time(self, timestamp: pd.Timestamp) -> bool:
        """检查是否为交易时间"""
        # 周末不交易
        if timestamp.weekday() >= 5:
            return False

        # 检查交易时段
        time = timestamp.time()
        morning = datetime.time(9, 30) <= time < datetime.time(12, 0)
        afternoon = datetime.time(13, 0) <= time < datetime.time(16, 0)

        return morning or afternoon

    def _validate_resampled_data(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """验证重采样数据质量"""
        if df.empty:
            return df

        # OHLC逻辑验证
        ohlc_columns = ['open', 'high', 'low', 'close']
        if all(col in df.columns for col in ohlc_columns):
            ohlc_data = df[ohlc_columns].dropna()
            if not ohlc_data.empty:
                # 修正high < max(open, close)的情况
                invalid_high = ohlc_data['high'] < ohlc_data[['open', 'close']].max(axis=1)
                if invalid_high.any():
                    self.logger.warning(f"修正 {invalid_high.sum()} 行无效high值")
                    df.loc[invalid_high.index, 'high'] = ohlc_data.loc[invalid_high.index, ['open', 'close']].max(axis=1)

                # 修正low > min(open, close)的情况
                invalid_low = ohlc_data['low'] > ohlc_data[['open', 'close']].min(axis=1)
                if invalid_low.any():
                    self.logger.warning(f"修正 {invalid_low.sum()} 行无效low值")
                    df.loc[invalid_low.index, 'low'] = ohlc_data.loc[invalid_low.index, ['open', 'close']].min(axis=1)

        # 负值检查
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            if col in df.columns:
                negative_values = df[col] < 0
                if negative_values.any():
                    self.logger.warning(f"修正 {negative_values.sum()} 个负值在 {col}")
                    df.loc[negative_values, col] = 0

        # 成交量检查
        if 'volume' in df.columns:
            negative_volume = df['volume'] < 0
            if negative_volume.any():
                self.logger.warning(f"修正 {negative_volume.sum()} 个负成交量")
                df.loc[negative_volume, 'volume'] = 0

        return df

def test_realtime_resampling():
    """测试实时重采样功能"""
    print("测试实时交易友好的重采样引擎...")

    # 创建测试数据 - 模拟9:30开盘的实时数据
    test_data = pd.DataFrame({
        'timestamp': [
            '2025-03-05 09:30:00',  # 开盘
            '2025-03-05 09:31:00',  # 开盘后1分钟
            '2025-03-05 09:32:00',
            '2025-03-05 09:33:00',
            '2025-03-05 09:34:00',
            '2025-03-05 09:35:00',
            '2025-03-05 09:36:00',
            '2025-03-05 09:37:00',
            '2025-03-05 09:38:00',
            '2025-03-05 09:39:00',
            '2025-03-05 09:40:00',  # 开盘后10分钟
        ],
        'open': [100, 100.5, 101, 101.5, 102, 102.5, 103, 103.5, 104, 104.5, 105],
        'high': [100.5, 101, 101.5, 102, 102.5, 103, 103.5, 104, 104.5, 105, 105.5],
        'low': [99.5, 100, 100.5, 101, 101.5, 102, 102.5, 103, 103.5, 104, 104.5],
        'close': [100.5, 101, 101.5, 102, 102.5, 103, 103.5, 104, 104.5, 105, 105.5],
        'volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000]
    })

    # 转换为datetime索引
    test_data['timestamp'] = pd.to_datetime(test_data['timestamp'])
    test_data = test_data.set_index('timestamp')

    print("原始数据:")
    print(test_data)

    # 创建实时重采样引擎
    realtime_strategy = RealTimeResamplingStrategy()

    # 测试30分钟重采样
    print("\n" + "="*50)
    print("测试30分钟实时重采样:")

    try:
        resampled_30min = realtime_strategy.resample_realtime(test_data, "30min")
        print("30分钟重采样结果:")
        print(resampled_30min)

        # 验证时间戳
        if not resampled_30min.empty:
            first_timestamp = resampled_30min.index[0]
            print(f"\n第一个时间戳: {first_timestamp}")
            print(f"期望: 2025-03-05 09:30:00 (开盘时间)")

            if first_timestamp == pd.to_datetime('2025-03-05 09:30:00'):
                print("✅ 实时重采样时间戳正确！")
            else:
                print("❌ 时间戳仍有问题")

    except Exception as e:
        print(f"❌ 30分钟重采样失败: {e}")

    # 测试滚动窗口
    print("\n" + "="*50)
    print("测试滚动窗口重采样:")

    try:
        rolling_data = realtime_strategy.create_rolling_windows(test_data, "5min")
        print("5分钟滚动窗口结果:")
        print(rolling_data)

        if not rolling_data.empty:
            print(f"\n滚动窗口数据点数: {len(rolling_data)}")
            print("✅ 滚动窗口重采样成功！")

    except Exception as e:
        print(f"❌ 滚动窗口重采样失败: {e}")

if __name__ == "__main__":
    test_realtime_resampling()