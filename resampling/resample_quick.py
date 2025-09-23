#!/usr/bin/env python3
"""
快速重采样脚本 - 一键执行
"""

import sys
import os

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from production_resampler_simple import ProductionResampler

def main():
    # 使用默认参数：源文件和输出目录
    source_file = "/Users/zhangshenshen/HK0920sen-code/data/raw_data/0700HK_1min_2025-03-05_2025-09-01.parquet"

    print("🚀 快速重采样 1m -> 10m,15m,30m,1h,2h,4h")
    print("=" * 50)

    try:
        resampler = ProductionResampler(source_file)
        results = resampler.run()

        print("\n✅ 重采样完成！")
        print("=" * 50)
        for tf, result in results.items():
            if 'error' in result:
                print(f"{tf:>4}: ❌ {result['error']}")
            else:
                print(f"{tf:>4}: ✅ {result['rows']} 行")

    except Exception as e:
        print(f"❌ 执行失败: {e}")

if __name__ == "__main__":
    main()