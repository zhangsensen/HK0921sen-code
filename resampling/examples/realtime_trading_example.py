#!/usr/bin/env python3
"""
实时交易系统重采样使用示例
展示如何在实际交易环境中使用修复后的重采样逻辑
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta
import time

# 添加重采样模块路径
sys.path.append(str(Path(__file__).parent.parent / 'core'))
from resampling_engine.resampler import OHLCVResamplingStrategy, TimeframeResampler
from resampling_engine.realtime_resampler import RealtimeTimeframeResampler

class RealtimeTradingSystem:
    """实时交易系统示例"""
    
    def __init__(self):
        # 传统重采样器（用于历史数据分析）
        self.traditional_resampler = TimeframeResampler(OHLCVResamplingStrategy())
        
        # 实时重采样器（用于实时信号生成）
        self.realtime_resampler = RealtimeTimeframeResampler()
        
        # 存储实时数据
        self.realtime_data = pd.DataFrame()
        
    def load_historical_data(self, file_path: str) -> pd.DataFrame:
        """加载历史数据"""
        print(f"📊 加载历史数据: {file_path}")
        
        # 这里模拟加载，实际应该从文件读取
        # return pd.read_parquet(file_path)
        
        # 创建模拟历史数据 - 使用工作日
        # 早盘：9:30-12:00
        morning_dates = pd.date_range(
            start="2025-09-22 09:30:00",  # 2025-09-22是星期一
            end="2025-09-22 11:59:00", 
            freq="1min"
        )
        
        # 午盘：13:00-16:00
        afternoon_dates = pd.date_range(
            start="2025-09-22 13:00:00",
            end="2025-09-22 15:59:00", 
            freq="1min"
        )
        
        # 合并交易时间
        trading_dates = morning_dates.union(afternoon_dates).tolist()
        
        # 生成模拟OHLCV数据
        import numpy as np
        np.random.seed(42)
        n_points = len(trading_dates)
        
        base_price = 400.0
        prices = base_price + np.cumsum(np.random.normal(0, 0.3, n_points))
        
        data = []
        for i, timestamp in enumerate(trading_dates):
            price = prices[i]
            data.append({
                'open': prices[i-1] if i > 0 else price,
                'high': price + abs(np.random.normal(0, 0.1)),
                'low': price - abs(np.random.normal(0, 0.1)),
                'close': price,
                'volume': np.random.randint(1000, 5000)
            })
        
        df = pd.DataFrame(data, index=pd.DatetimeIndex(trading_dates))
        
        # 修正OHLC逻辑
        for i in range(len(df)):
            row = df.iloc[i]
            df.iloc[i, df.columns.get_loc('high')] = max(row['open'], row['high'], row['close'])
            df.iloc[i, df.columns.get_loc('low')] = min(row['open'], row['low'], row['close'])
        
        print(f"✅ 历史数据加载完成: {len(df)} 个数据点")
        return df
        
    def analyze_historical_patterns(self, data: pd.DataFrame):
        """分析历史模式"""
        print("\n🔍 历史数据分析...")
        
        # 使用修复后的传统重采样器
        hourly_data = self.traditional_resampler.resample(data, "1h")
        
        print("历史1小时数据分析:")
        print(f"  数据点数: {len(hourly_data)}")
        if len(hourly_data) > 0:
            print(f"  时间范围: {hourly_data.index[0]} 到 {hourly_data.index[-1]}")
        
        # 计算一些基本统计
        if len(hourly_data) > 0:
            avg_volume = hourly_data['volume'].mean()
            price_range = hourly_data['close'].max() - hourly_data['close'].min()
            print(f"  平均小时成交量: {avg_volume:,.0f}")
            print(f"  价格波动范围: {price_range:.2f}")
            
            # 显示前几个小时的数据
            print("\n前3个小时的数据:")
            for i, (timestamp, row) in enumerate(hourly_data.head(3).iterrows()):
                print(f"  {timestamp}: {row['open']:.2f} -> {row['close']:.2f} (量:{row['volume']:,.0f})")
        else:
            print("  ⚠️  重采样后没有数据，可能是交易时间过滤过于严格")
            print("  原始数据时间范围:")
            print(f"    开始: {data.index[0]}")
            print(f"    结束: {data.index[-1]}")
            print(f"    数据点数: {len(data)}")
    
    def simulate_realtime_trading(self, historical_data: pd.DataFrame):
        """模拟实时交易场景"""
        print("\n🚀 模拟实时交易场景...")
        print("=" * 60)
        
        # 模拟交易日开始
        trading_start = pd.Timestamp("2025-09-22 09:30:00")
        current_data = pd.DataFrame()
        
        # 关键交易时间点
        key_moments = [
            (5, "开盘5分钟"),
            (15, "开盘15分钟"), 
            (30, "开盘30分钟"),
            (60, "开盘1小时"),
            (90, "开盘1.5小时")
        ]
        
        for minutes_elapsed, description in key_moments:
            current_time = trading_start + timedelta(minutes=minutes_elapsed)
            
            # 获取到当前时间的所有数据
            current_data = historical_data[historical_data.index <= current_time]
            
            if len(current_data) == 0:
                continue
                
            print(f"\n⏰ {description} ({current_time.strftime('%H:%M')})")
            print(f"   累积数据点: {len(current_data)}")
            
            try:
                # 获取实时信号
                signal = self.realtime_resampler.get_latest_signal(
                    current_data, "1h", current_time
                )
                
                if signal['status'] == 'active':
                    print(f"   ✅ 信号状态: 活跃")
                    print(f"   📊 信号时间戳: {signal['timestamp']}")
                    print(f"   💰 当前价格: {signal['signal']['close']:.2f}")
                    print(f"   📈 价格变化: {signal['signal']['open']:.2f} -> {signal['signal']['close']:.2f}")
                    print(f"   📊 成交量: {signal['signal']['volume']:,.0f}")
                    print(f"   ⏱️  信号新鲜度: {'新鲜' if signal['is_fresh'] else '过期'}")
                    
                    # 交易决策逻辑示例
                    price_change = signal['signal']['close'] - signal['signal']['open']
                    if price_change > 0.5:
                        print(f"   🟢 交易信号: 买入 (涨幅 {price_change:.2f})")
                    elif price_change < -0.5:
                        print(f"   🔴 交易信号: 卖出 (跌幅 {price_change:.2f})")
                    else:
                        print(f"   ⚪ 交易信号: 观望 (变化 {price_change:.2f})")
                        
                else:
                    print(f"   ⚠️  信号状态: {signal['status']}")
                    
            except Exception as e:
                print(f"   ❌ 信号生成失败: {e}")
    
    def compare_old_vs_new_logic(self, data: pd.DataFrame):
        """对比新旧逻辑的差异"""
        print("\n📊 新旧逻辑对比分析")
        print("=" * 60)
        
        # 模拟旧逻辑（label='right'）
        print("🔴 旧逻辑 (label='right', 延迟信号):")
        old_result = data.resample("1h", label='right', closed='left').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        for i, (timestamp, row) in enumerate(old_result.head(2).iterrows()):
            print(f"  {timestamp}: 需要等到{timestamp.strftime('%H:%M')}才能获得信号")
        
        # 新逻辑（修复后）
        print("\n🟢 新逻辑 (offset='30min', 实时信号):")
        new_result = self.traditional_resampler.resample(data, "1h")
        
        for i, (timestamp, row) in enumerate(new_result.head(2).iterrows()):
            print(f"  {timestamp}: 在{timestamp.strftime('%H:%M')}就可以开始生成信号")
        
        # 计算时间优势
        if len(old_result) > 0 and len(new_result) > 0:
            time_advantage = (old_result.index[0] - new_result.index[0]).total_seconds() / 60
            print(f"\n💡 时间优势: 新逻辑比旧逻辑早 {time_advantage:.0f} 分钟获得交易信号")
            print("   这意味着在开盘阶段不会错过任何交易机会！")

def main():
    """主函数"""
    print("🏆 实时交易系统重采样演示")
    print("展示修复后的重采样逻辑如何解决交易机会流失问题")
    print("=" * 80)
    
    # 创建交易系统
    trading_system = RealtimeTradingSystem()
    
    # 加载历史数据（实际应该从parquet文件读取）
    historical_data = trading_system.load_historical_data("mock_data.parquet")
    
    # 分析历史模式
    trading_system.analyze_historical_patterns(historical_data)
    
    # 模拟实时交易
    trading_system.simulate_realtime_trading(historical_data)
    
    # 对比新旧逻辑
    trading_system.compare_old_vs_new_logic(historical_data)
    
    print("\n" + "=" * 80)
    print("✅ 演示完成！")
    print("\n🎯 关键要点:")
    print("1. 修复后的重采样使用offset='30min'对齐港股交易时间")
    print("2. 时间戳现在正确标记为窗口开始时间（9:30而不是10:30）")
    print("3. 实时重采样支持部分数据的早期信号生成")
    print("4. 不再错过开盘阶段的黄金交易机会")
    print("5. 传统和实时两种模式可以根据需要选择使用")

if __name__ == "__main__":
    main()
