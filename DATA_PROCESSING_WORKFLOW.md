# 数据处理完整流程

## 📋 处理流程

### 1. 原始数据准备
```
位置: data/raw_data/
文件格式: 0700HK_1min_2025-03-05_2025-09-01.parquet
```

### 2. 执行重采样
```bash
cd resampling
python resample_quick.py
```
- 输出: 10m, 15m, 30m, 1h, 2h, 4h 时间框架文件
- 位置: data/raw_data/

### 3. 执行数据重命名和组织
```bash
cd data/datacora
python rename_data_files.py ../raw_data --dry-run  # 预览
python rename_data_files.py ../raw_data         # 执行
```
- 按时间框架创建子目录
- 重命名文件为标准格式

### 4. 最终目录结构
```
data/raw_data/
├── 1m/         # 1分钟数据
├── 2m/         # 2分钟数据
├── 3m/         # 3分钟数据
├── 5m/         # 5分钟数据
├── 10m/        # 10分钟数据
├── 15m/        # 15分钟数据
├── 30m/        # 30分钟数据
├── 1h/         # 1小时数据
├── 2h/         # 2小时数据
├── 4h/         # 4小时数据
└── 1d/         # 1天数据
```

## 🚀 一键执行脚本

创建 `process_data.sh`:
```bash
#!/bin/bash
echo "开始数据处理流程..."

# 1. 重采样
cd resampling
echo "执行重采样..."
python resample_quick.py

# 2. 重命名和组织
cd ../data/datacora
echo "执行重命名和组织..."
python rename_data_files.py ../raw_data

echo "✅ 数据处理完成!"
```