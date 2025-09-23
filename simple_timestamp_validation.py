#!/usr/bin/env python3
"""
简化版时间戳约束验证
避免logging冲突，专注核心验证功能
"""

import pandas as pd
import numpy as np
import sys
import os
from pathlib import Path

# 直接导入约束验证功能
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))

def validate_single_timestamp(timestamp, context="测试"):
    """简化的时间戳验证函数"""
    try:
        # 检查数值时间戳（被禁止的格式）
        if isinstance(timestamp, (int, float)):
            abs_value = abs(int(timestamp))
            digit_count = len(str(abs_value))
            if digit_count >= 10:
                from datetime import datetime
                divisor = 1000 if digit_count >= 13 else 1
                readable_time = datetime.fromtimestamp(float(timestamp) / divisor).strftime('%Y-%m-%d %H:%M:%S')
                format_name = "毫秒时间戳" if digit_count >= 13 else "Unix时间戳"
                raise ValueError(f"❌ {context}: 检测到被禁止的{format_name}格式 {timestamp} (应该是 {readable_time})")
        
        # 检查字符串数值时间戳
        if isinstance(timestamp, str):
            stripped = timestamp.strip()
            if stripped.replace('.', '', 1).isdigit() and len(stripped.replace('.', '', 1)) >= 10:
                from datetime import datetime
                digits = len(stripped.replace('.', '', 1))
                divisor = 1000 if digits >= 13 else 1
                readable_time = datetime.fromtimestamp(float(stripped) / divisor).strftime('%Y-%m-%d %H:%M:%S')
                format_name = "毫秒时间戳" if digits >= 13 else "Unix时间戳"
                raise ValueError(f"❌ {context}: 检测到被禁止的{format_name}字符串 '{timestamp}' (应该是 '{readable_time}')")
        
        # 尝试转换为datetime
        dt = pd.to_datetime(timestamp)
        if pd.isna(dt):
            raise ValueError(f"❌ {context}: 时间戳为空值")
        
        return True
        
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"❌ {context}: 时间戳验证失败 '{timestamp}': {e}")

def test_original_data():
    """测试原始数据"""
    print("🔍 检查原始数据时间戳格式")
    print("=" * 50)
    
    data_file = "/Users/zhangshenshen/HK0920sen-code/data/raw_data/0700HK_1min_2025-03-05_2025-09-01.parquet"
    
    if not os.path.exists(data_file):
        print(f"❌ 文件不存在: {data_file}")
        return False
    
    df = pd.read_parquet(data_file)
    print(f"数据: {df.shape[0]} 行")
    
    # 检查前5个时间戳
    for i in range(min(5, len(df))):
        ts = df['timestamp'].iloc[i]
        try:
            validate_single_timestamp(ts, f"原始数据[{i}]")
        except ValueError as e:
            print(f"{e}")
            return False
    
    print(f"✅ 原始数据时间戳格式正确")
    print(f"示例: {df['timestamp'].iloc[0]}")
    return True

def test_resampled_files():
    """测试重采样文件"""
    print("\n🔍 检查重采样文件时间戳格式")
    print("=" * 50)
    
    base_path = Path("/Users/zhangshenshen/HK0920sen-code/data/raw_data")
    timeframes = ["10m", "15m", "30m", "1h", "2h", "4h"]
    
    all_valid = True
    
    for tf in timeframes:
        file_path = base_path / f"0700HK_1min_2025-03-05_2025-09-01_{tf}.parquet"
        
        if not file_path.exists():
            print(f"{tf:>4}: 文件不存在")
            continue
        
        try:
            df = pd.read_parquet(file_path)
            
            # 检查前3个时间戳
            for i in range(min(3, len(df))):
                ts = df['timestamp'].iloc[i]
                validate_single_timestamp(ts, f"重采样{tf}[{i}]")
            
            # 检查数据类型
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                print(f"❌ {tf}: 时间戳不是datetime类型: {df['timestamp'].dtype}")
                all_valid = False
                continue
            
            sample_ts = str(df['timestamp'].iloc[0])
            print(f"✅ {tf:>4}: {len(df)} 行, 示例: {sample_ts}")
            
        except ValueError as e:
            print(f"❌ {tf:>4}: {e}")
            all_valid = False
        except Exception as e:
            print(f"❌ {tf:>4}: 验证错误: {e}")
            all_valid = False
    
    return all_valid

def test_constraint_cases():
    """测试约束验证用例"""
    print("\n🔍 测试约束验证用例")
    print("=" * 50)
    
    # 应该被拒绝的格式
    forbidden_cases = [
        ("Unix时间戳", 1712254200),
        ("毫秒时间戳", 1741182300000),
        ("毫秒字符串", "1741182300000"),
        ("Unix字符串", "1712254200"),
    ]
    
    # 应该通过的格式
    allowed_cases = [
        ("标准字符串", "2025-03-05 09:30:00"),
        ("ISO格式", "2025-03-05T09:30:00"),
        ("Pandas Timestamp", pd.Timestamp('2025-03-05 09:30:00')),
    ]
    
    all_correct = True
    
    # 测试禁止格式
    for name, test_value in forbidden_cases:
        try:
            validate_single_timestamp(test_value, name)
            print(f"❌ {name}: 应该被拒绝但通过了")
            all_correct = False
        except ValueError as e:
            if "应该是" in str(e):
                print(f"✅ {name}: 正确拒绝并提供可读格式")
            else:
                print(f"⚠️  {name}: 正确拒绝但格式提示不完整")
    
    # 测试允许格式
    for name, test_value in allowed_cases:
        try:
            validate_single_timestamp(test_value, name)
            print(f"✅ {name}: 正确通过验证")
        except Exception as e:
            print(f"❌ {name}: 应该通过但被拒绝: {e}")
            all_correct = False
    
    return all_correct

def test_live_resampling():
    """测试实时重采样"""
    print("\n🔍 测试实时重采样")
    print("=" * 50)
    
    # 创建测试数据
    dates = pd.date_range('2025-03-05 09:30:00', '2025-03-05 11:59:00', freq='1min')
    test_data = pd.DataFrame({
        'open': np.random.normal(400, 5, len(dates)),
        'high': np.random.normal(402, 5, len(dates)),
        'low': np.random.normal(398, 5, len(dates)),
        'close': np.random.normal(400, 5, len(dates)),
        'volume': np.random.randint(1000, 5000, len(dates))
    }, index=dates)
    
    print(f"测试数据: {len(test_data)} 行")
    
    # 导入重采样器
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'resampling'))
    from simple_resampler import SimpleResampler
    
    resampler = SimpleResampler()
    test_timeframes = ["5m", "15m"]
    
    all_valid = True
    
    for tf in test_timeframes:
        try:
            result = resampler.resample(test_data, tf)
            
            # 验证结果
            for i in range(min(3, len(result))):
                ts = result['timestamp'].iloc[i]
                validate_single_timestamp(ts, f"实时重采样{tf}[{i}]")
            
            sample_ts = str(result['timestamp'].iloc[0])
            print(f"✅ {tf}: {len(result)} 行, 示例: {sample_ts}")
            
        except Exception as e:
            print(f"❌ {tf}: {e}")
            all_valid = False
    
    return all_valid

def main():
    """主函数"""
    print("🔒 简化版时间戳约束验证")
    print("=" * 60)
    print("核心铁律：所有时间戳必须保持人类可读格式")
    print("禁止格式：毫秒时间戳、Unix时间戳等数值格式")
    print("=" * 60)
    
    tests = [
        ("原始数据验证", test_original_data),
        ("重采样文件验证", test_resampled_files),
        ("约束验证用例", test_constraint_cases),
        ("实时重采样验证", test_live_resampling),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}...")
        try:
            if test_func():
                print(f"✅ {test_name} 通过")
                passed += 1
            else:
                print(f"❌ {test_name} 失败")
                failed += 1
        except Exception as e:
            print(f"❌ {test_name} 执行错误: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"📊 测试结果: {passed} 通过, {failed} 失败")
    
    if failed == 0:
        print("🎉 所有测试通过！时间戳约束系统完全正常")
        print("✅ 核心铁律已严格执行")
        print("❌ 数值时间戳格式已被完全禁止")
    else:
        print("⚠️ 部分测试失败")
    
    return failed == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
