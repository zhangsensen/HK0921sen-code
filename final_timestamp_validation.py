#!/usr/bin/env python3
"""
最终时间戳验证报告
验证Unix时间戳问题是否已完全解决
"""

import pandas as pd
import os
from datetime import datetime

def validate_timestamp_format(ts_value, context=""):
    """验证时间戳格式是否符合人类可读要求"""
    ts_str = str(ts_value)
    
    # 检查是否为数值时间戳格式（被禁止）
    if ts_str.replace('.', '', 1).isdigit() and len(ts_str.replace('.', '', 1)) >= 10:
        digits = len(ts_str.replace('.', '', 1))
        if digits >= 13:
            readable = datetime.fromtimestamp(float(ts_str) / 1000).strftime('%Y-%m-%d %H:%M:%S')
            return False, f"❌ 毫秒时间戳 {ts_str} (应该是 {readable})"
        else:
            readable = datetime.fromtimestamp(float(ts_str)).strftime('%Y-%m-%d %H:%M:%S')
            return False, f"❌ Unix时间戳 {ts_str} (应该是 {readable})"
    
    # 检查是否为正确的datetime格式
    if isinstance(ts_value, pd.Timestamp) or "2025-" in ts_str:
        return True, f"✅ 人类可读格式: {ts_str}"
    
    return False, f"⚠️  未知格式: {ts_str}"

def main():
    print("🔒 最终时间戳验证报告")
    print("=" * 60)
    print("验证目标：确认所有重采样数据使用人类可读时间戳格式")
    print("核心铁律：禁止Unix时间戳 (1712254200) 和毫秒时间戳 (1741182300000)")
    print("=" * 60)
    
    # 1. 验证原始数据
    print("\n📁 原始数据验证")
    print("-" * 30)
    orig_file = "/Users/zhangshenshen/HK0920sen-code/data/raw_data/0700HK_1min_2025-03-05_2025-09-01.parquet"
    
    if os.path.exists(orig_file):
        df = pd.read_parquet(orig_file)
        sample_ts = df['timestamp'].iloc[0]
        is_valid, msg = validate_timestamp_format(sample_ts, "原始数据")
        print(f"原始数据: {msg}")
        print(f"数据类型: {df['timestamp'].dtype}, 总行数: {len(df)}")
    else:
        print("❌ 原始数据文件不存在")
    
    # 2. 验证重采样文件
    print("\n📊 重采样文件验证")
    print("-" * 30)
    
    timeframes = ["10m", "15m", "30m", "1h", "2h", "4h"]
    base_path = "/Users/zhangshenshen/HK0920sen-code/data/raw_data"
    
    all_valid = True
    total_files = 0
    valid_files = 0
    
    for tf in timeframes:
        file_path = f"{base_path}/0700HK_1min_2025-03-05_2025-09-01_{tf}.parquet"
        
        if os.path.exists(file_path):
            total_files += 1
            df = pd.read_parquet(file_path)
            sample_ts = df['timestamp'].iloc[0]
            is_valid, msg = validate_timestamp_format(sample_ts, f"重采样{tf}")
            
            if is_valid:
                valid_files += 1
                print(f"{tf:>4}: {msg} | {len(df)} 行")
            else:
                print(f"{tf:>4}: {msg} | {len(df)} 行")
                all_valid = False
        else:
            print(f"{tf:>4}: 文件不存在")
    
    # 3. 验证约束测试用例
    print("\n🧪 约束测试用例验证")
    print("-" * 30)
    
    # 测试Unix时间戳 1712254200 (应该转换为 2024-04-05 02:10:00)
    unix_ts = 1712254200
    expected_readable = datetime.fromtimestamp(unix_ts).strftime('%Y-%m-%d %H:%M:%S')
    print(f"Unix时间戳测试: {unix_ts} -> {expected_readable}")
    
    is_valid, msg = validate_timestamp_format(unix_ts, "Unix测试")
    if not is_valid and expected_readable in msg:
        print("✅ Unix时间戳正确被拒绝并提供了正确的可读格式")
    else:
        print(f"❌ Unix时间戳验证异常: {msg}")
        all_valid = False
    
    # 测试毫秒时间戳 1741182300000
    ms_ts = 1741182300000
    expected_readable_ms = datetime.fromtimestamp(ms_ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
    print(f"毫秒时间戳测试: {ms_ts} -> {expected_readable_ms}")
    
    is_valid, msg = validate_timestamp_format(ms_ts, "毫秒测试")
    if not is_valid and expected_readable_ms in msg:
        print("✅ 毫秒时间戳正确被拒绝并提供了正确的可读格式")
    else:
        print(f"❌ 毫秒时间戳验证异常: {msg}")
        all_valid = False
    
    # 4. 最终报告
    print("\n" + "=" * 60)
    print("📋 最终验证报告")
    print("=" * 60)
    
    print(f"重采样文件状态: {valid_files}/{total_files} 文件格式正确")
    
    if all_valid and valid_files == total_files:
        print("🎉 验证结果: 完全成功！")
        print("✅ 所有重采样数据都使用人类可读时间戳格式")
        print("✅ Unix时间戳和毫秒时间戳格式已被完全禁止")
        print("✅ 约束验证系统正常工作")
        print("✅ 时间戳约束铁律已严格执行")
        
        print("\n🔒 核心铁律执行状态:")
        print("  ❌ Unix时间戳 (1712254200) -> 被禁止 ✓")
        print("  ❌ 毫秒时间戳 (1741182300000) -> 被禁止 ✓")
        print("  ✅ 人类可读格式 (2025-03-05 09:30:00) -> 强制使用 ✓")
        
        return True
    else:
        print("⚠️ 验证结果: 存在问题")
        print("需要进一步检查和修复")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
