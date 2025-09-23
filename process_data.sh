#!/bin/bash
echo "🚀 开始数据处理流程..."

# 1. 执行重采样
cd resampling
echo "📊 执行重采样..."
python resample_quick.py

# 2. 执行重命名和组织
cd ../data/datacora
echo "📁 执行重命名和组织..."
python rename_data_files.py ../raw_data

echo "✅ 数据处理完成!"