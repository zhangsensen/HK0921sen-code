#!/usr/bin/env python3
"""
批量重命名数据文件脚本并按时间框架组织目录结构

将格式从 {SYMBOL}_{TIMEFRAME}_YYYY-MM-DD_YYYY-MM-DD.parquet
重命名为 {SYMBOL}_{TIMEFRAME}.parquet 并移动到对应的时间框架子目录

支持的目录结构:
    raw_data/
    ├── 1m/
    │   ├── 0700HK.parquet
    │   └── 0999HK.parquet
    ├── 5m/
    │   ├── 0700HK.parquet
    │   └── 0999HK.parquet
    ├── 1h/
    │   ├── 0700HK.parquet
    │   └── 0999HK.parquet
    └── 1d/
        ├── 0700HK.parquet
        └── 0999HK.parquet

特殊处理：
- 统一分钟格式，将 'min' 转换为 'm' (例如: 1min -> 1m)
- 保持其他时间格式不变 (h, day)
- 自动创建时间框架子目录
- 支持多种时间框架：1m, 2m, 3m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 1d

用法:
    python rename_data_files.py [directory] [--dry-run] [--recursive] [--verbose] [--organize-by-timeframe]

参数:
    directory: 目标目录路径 (默认: 当前目录)
    --dry-run: 预览模式，不实际重命名
    --recursive: 递归处理子目录
    --verbose: 详细输出模式
    --organize-by-timeframe: 按时间框架组织目录结构 (默认启用)
    --no-organize: 不按时间框架组织，仅重命名文件
    --help: 显示帮助信息
"""

import argparse
import os
import re
import sys
import shutil
from pathlib import Path
from typing import List, Tuple, Dict, Set
import logging


class DataFileRenamer:
    """数据文件重命名和组织工具"""

    def __init__(self, verbose: bool = False, organize_by_timeframe: bool = True):
        self.verbose = verbose
        self.organize_by_timeframe = organize_by_timeframe
        self.logger = self._setup_logging()
        
        # 匹配需要重命名的文件：
        # 1. 基础重采样文件：{SYMBOL}_{TIMEFRAME}_YYYY-MM-DD_YYYY-MM-DD.parquet
        # 2. 进一步重采样文件：{SYMBOL}_{BASE_TIMEFRAME}_YYYY-MM-DD_YYYY-MM-DD_{TARGET_TIMEFRAME}.parquet
        # 3. 包含'min'的文件（支持前缀）
        self.date_pattern = re.compile(r'^(.+)_([0-9]+min|[0-9]+m|[0-9]+h|[0-9]+day)_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.parquet$')
        self.resampled_pattern = re.compile(r'^(.+_[0-9]+min|[0-9]+m|[0-9]+h|[0-9]+day)_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}_([0-9]+m|[0-9]+h|[0-9]+day)\.parquet$')
        self.min_pattern = re.compile(r'^(.+)_([0-9]+min)\.parquet$')
        self.already_formatted_pattern = re.compile(r'^(.+)_([0-9]+m|[0-9]+h|[0-9]+day)\.parquet$')
        
        # 支持的时间框架映射
        self.timeframe_mapping = {
            '1min': '1m', '2min': '2m', '3min': '3m', '5min': '5m',
            '10min': '10m', '15min': '15m', '30min': '30m',
            '1m': '1m', '2m': '2m', '3m': '3m', '5m': '5m',
            '10m': '10m', '15m': '15m', '30m': '30m',
            '1h': '1h', '2h': '2h', '4h': '4h',
            '1day': '1d', '1d': '1d'
        }

    def _setup_logging(self) -> logging.Logger:
        """设置日志"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # 获取脚本所在目录
        script_dir = Path(__file__).parent

        # 创建日志文件
        log_file = script_dir / f"rename_log_{Path(__file__).stem}.log"

        # 文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)

        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def parse_filename(self, filename: str) -> Tuple[str, str, str]:
        """
        解析文件名，提取股票代码和时间框架

        Args:
            filename: 原始文件名

        Returns:
            Tuple[股票代码, 时间框架, 新文件名]

        Raises:
            ValueError: 文件名格式不匹配
        """
        # 尝试匹配重采样文件名（包含目标时间框架）
        resampled_match = self.resampled_pattern.match(filename)
        if resampled_match:
            base_part = resampled_match.group(1)
            timeframe = resampled_match.group(2)

            # 从基础部分提取股票代码
            base_match = re.match(r'^(.+)_([0-9]+min|[0-9]+m|[0-9]+h|[0-9]+day)$', base_part)
            if base_match:
                prefix_and_symbol = base_match.group(1)
            else:
                prefix_and_symbol = base_part
        else:
            # 尝试匹配带日期的文件名
            date_match = self.date_pattern.match(filename)
            if date_match:
                prefix_and_symbol = date_match.group(1)
                timeframe = date_match.group(2)
            else:
                # 尝试匹配需要统一格式的文件名
                min_match = self.min_pattern.match(filename)
                if min_match:
                    prefix_and_symbol = min_match.group(1)
                    timeframe = min_match.group(2)
                else:
                    # 检查是否已经是标准格式
                    formatted_match = self.already_formatted_pattern.match(filename)
                    if formatted_match:
                        prefix_and_symbol = formatted_match.group(1)
                        timeframe = formatted_match.group(2)
                    else:
                        raise ValueError(f"文件名格式不匹配: {filename}")

        # 智能分离前缀和股票代码
        if '_' in prefix_and_symbol:
            # 假设最后一部分是股票代码
            parts = prefix_and_symbol.split('_')
            symbol = parts[-1]
            prefix = '_'.join(parts[:-1])
        else:
            symbol = prefix_and_symbol
            prefix = None

        # 标准化时间框架格式
        timeframe = self.timeframe_mapping.get(timeframe, timeframe)

        # 构建新的文件名（不包含前缀）
        new_filename = f"{symbol}_{timeframe}.parquet"

        return symbol, timeframe, new_filename

    def get_timeframe_directory(self, base_dir: Path, timeframe: str) -> Path:
        """
        获取时间框架对应的目录路径

        Args:
            base_dir: 基础目录
            timeframe: 时间框架

        Returns:
            时间框架目录路径
        """
        if self.organize_by_timeframe:
            timeframe_dir = base_dir / timeframe
            return timeframe_dir
        else:
            return base_dir

    def ensure_directory_exists(self, directory: Path) -> bool:
        """
        确保目录存在，如果不存在则创建

        Args:
            directory: 目录路径

        Returns:
            是否成功创建或目录已存在
        """
        try:
            directory.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            self.logger.error(f"创建目录失败 {directory}: {e}")
            return False

    def scan_directory(self, directory: Path, recursive: bool = False) -> List[Path]:
        """
        扫描目录查找需要重命名的文件

        Args:
            directory: 目标目录
            recursive: 是否递归扫描子目录

        Returns:
            需要重命名的文件列表
        """
        files_to_rename = []

        if recursive:
            for root, _, files in os.walk(directory):
                for file in files:
                    file_path = Path(root) / file
                    if self._should_rename(file):
                        files_to_rename.append(file_path)
        else:
            for file in os.listdir(directory):
                file_path = directory / file
                if file_path.is_file() and self._should_rename(file):
                    files_to_rename.append(file_path)

        return files_to_rename

    def _should_rename(self, filename: str) -> bool:
        """判断文件是否需要重命名"""
        return (self.date_pattern.match(filename) is not None or
                self.resampled_pattern.match(filename) is not None or
                self.min_pattern.match(filename) is not None)

    def check_conflicts(self, files_to_rename: List[Path]) -> Dict[str, List[Path]]:
        """
        检查重命名冲突

        Args:
            files_to_rename: 需要重命名的文件列表

        Returns:
            冲突字典 {新文件名: [原始文件路径列表]}
        """
        conflicts = {}
        new_names = {}

        for file_path in files_to_rename:
            try:
                symbol, timeframe, new_filename = self.parse_filename(file_path.name)
                
                # 如果按时间框架组织，需要考虑目标目录
                if self.organize_by_timeframe:
                    target_dir = file_path.parent / timeframe
                    target_path = target_dir / new_filename
                else:
                    target_path = file_path.parent / new_filename
                
                # 使用相对路径作为冲突检查的键
                relative_target_path = target_path.relative_to(file_path.parent.parent if self.organize_by_timeframe else file_path.parent)
                
                if str(relative_target_path) in new_names:
                    if str(relative_target_path) not in conflicts:
                        conflicts[str(relative_target_path)] = [new_names[str(relative_target_path)]]
                    conflicts[str(relative_target_path)].append(file_path)
                else:
                    new_names[str(relative_target_path)] = file_path
                    
            except ValueError:
                if self.verbose:
                    self.logger.warning(f"跳过不匹配的文件: {file_path}")

        return conflicts

    def organize_and_rename_files(self, files_to_rename: List[Path], dry_run: bool = False) -> Dict[str, int]:
        """
        执行文件重命名和组织

        Args:
            files_to_rename: 需要重命名的文件列表
            dry_run: 预览模式

        Returns:
            统计信息字典
        """
        stats = {
            'total': len(files_to_rename),
            'renamed': 0,
            'moved': 0,
            'skipped': 0,
            'errors': 0,
            'directories_created': 0
        }

        for file_path in files_to_rename:
            try:
                symbol, timeframe, new_filename = self.parse_filename(file_path.name)
                
                # 确定目标目录
                if self.organize_by_timeframe:
                    target_dir = self.get_timeframe_directory(file_path.parent, timeframe)
                    target_path = target_dir / new_filename
                else:
                    target_dir = file_path.parent
                    target_path = target_dir / new_filename

                # 检查目标文件是否已存在
                if target_path.exists() and target_path != file_path:
                    self.logger.warning(f"目标文件已存在，跳过: {file_path} -> {target_path}")
                    stats['skipped'] += 1
                    continue

                if dry_run:
                    if self.organize_by_timeframe:
                        print(f"[预览] 将移动并重命名: {file_path.name} -> {target_dir.name}/{new_filename}")
                    else:
                        print(f"[预览] 将重命名: {file_path.name} -> {new_filename}")
                else:
                    # 确保目标目录存在
                    if self.organize_by_timeframe and not target_dir.exists():
                        if self.ensure_directory_exists(target_dir):
                            stats['directories_created'] += 1
                        else:
                            stats['errors'] += 1
                            continue

                    # 执行移动和重命名
                    shutil.move(str(file_path), str(target_path))
                    
                    if self.verbose:
                        if self.organize_by_timeframe:
                            print(f"已移动并重命名: {file_path.name} -> {target_dir.name}/{new_filename}")
                        else:
                            print(f"已重命名: {file_path.name} -> {new_filename}")

                    if self.organize_by_timeframe:
                        stats['moved'] += 1
                    stats['renamed'] += 1

            except Exception as e:
                self.logger.error(f"处理文件失败 {file_path}: {e}")
                stats['errors'] += 1

        return stats

    def process_directory(self, directory: str, dry_run: bool = False,
                        recursive: bool = False) -> bool:
        """
        处理目录

        Args:
            directory: 目标目录
            dry_run: 预览模式
            recursive: 递归处理

        Returns:
            处理是否成功
        """
        dir_path = Path(directory)

        if not dir_path.exists():
            self.logger.error(f"目录不存在: {directory}")
            return False

        if not dir_path.is_dir():
            self.logger.error(f"路径不是目录: {directory}")
            return False

        self.logger.info(f"扫描目录: {directory}")
        
        if self.organize_by_timeframe:
            self.logger.info("按时间框架组织目录结构: 启用")
        else:
            self.logger.info("按时间框架组织目录结构: 禁用")

        # 扫描文件
        files_to_rename = self.scan_directory(dir_path, recursive)

        if not files_to_rename:
            self.logger.info("没有找到需要重命名的文件")
            return True

        # 检查冲突
        conflicts = self.check_conflicts(files_to_rename)
        if conflicts:
            self.logger.warning("发现重命名冲突:")
            for new_name, file_list in conflicts.items():
                self.logger.warning(f"  {new_name}:")
                for file_path in file_list:
                    self.logger.warning(f"    - {file_path}")
            self.logger.error("存在冲突，请先解决冲突再执行重命名")
            return False

        # 显示将要重命名的文件
        if dry_run or self.verbose:
            print(f"\n找到 {len(files_to_rename)} 个需要处理的文件:")
            for file_path in files_to_rename:
                try:
                    symbol, timeframe, new_filename = self.parse_filename(file_path.name)
                    relative_path = file_path.relative_to(dir_path)
                    
                    if self.organize_by_timeframe:
                        new_relative_path = Path(timeframe) / new_filename
                    else:
                        new_relative_path = new_filename
                    
                    print(f"  {relative_path} -> {new_relative_path}")
                except ValueError:
                    continue
            print()

        # 执行重命名和组织
        stats = self.organize_and_rename_files(files_to_rename, dry_run)

        # 显示统计信息
        self._display_stats(stats, dry_run)

        return stats['errors'] == 0

    def _display_stats(self, stats: Dict[str, int], dry_run: bool):
        """显示统计信息"""
        action = "预览" if dry_run else "实际"

        print(f"\n=== 文件处理统计 ({action}模式) ===")
        print(f"总计文件数: {stats['total']}")
        print(f"成功处理: {stats['renamed']}")
        if self.organize_by_timeframe:
            print(f"移动到子目录: {stats['moved']}")
            print(f"创建目录数: {stats['directories_created']}")
        print(f"跳过文件数: {stats['skipped']}")
        print(f"错误文件数: {stats['errors']}")

        if dry_run:
            print("\n注意: 这是预览模式，没有实际执行重命名")
            print("如需执行实际重命名，请移除 --dry-run 参数")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="批量重命名数据文件并按时间框架组织目录结构",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
=== 极简命令示例 ===
    # 预览重命名结果（推荐先运行这个）
    python rename_data_files.py ../raw_data --dry-run

    # 执行重命名和组织
    python rename_data_files.py ../raw_data

    # 处理当前项目根目录的 raw_data
    python rename_data_files.py ../../raw_data --dry-run

    # 处理当前目录
    python rename_data_files.py . --dry-run

支持的目录结构:
    raw_data/
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
        """
    )

    parser.add_argument(
        'directory',
        nargs='?',
        default='.',
        help='目标目录路径 (默认: 当前目录)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='预览模式，不实际重命名'
    )

    parser.add_argument(
        '--recursive', '-r',
        action='store_true',
        help='递归处理子目录'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='详细输出模式'
    )

    parser.add_argument(
        '--organize-by-timeframe',
        action='store_true',
        help='按时间框架组织目录结构 (默认启用)'
    )

    parser.add_argument(
        '--no-organize',
        action='store_true',
        help='不按时间框架组织，仅重命名文件'
    )

    args = parser.parse_args()

    # 确定是否按时间框架组织
    organize_by_timeframe = not args.no_organize

    # 创建重命名器
    renamer = DataFileRenamer(verbose=args.verbose, organize_by_timeframe=organize_by_timeframe)

    # 处理目录
    success = renamer.process_directory(
        directory=args.directory,
        dry_run=args.dry_run,
        recursive=args.recursive
    )

    # 返回适当的退出码
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
