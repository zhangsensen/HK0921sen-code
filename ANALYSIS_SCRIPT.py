#!/usr/bin/env python3
"""
仓库分析脚本 - 用于识别重复代码和清理机会
在执行清理前运行此脚本来了解当前状态
"""

import ast
import json
import sys
from pathlib import Path
from typing import Dict, List

def find_python_files(root_dir: str) -> List[Path]:
    """找到所有的Python文件"""
    root = Path(root_dir)
    return list(root.rglob("*.py"))

def analyze_file_structure(file_path: Path) -> Dict:
    """分析文件结构和导入"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        tree = ast.parse(content)

        imports = []
        classes = []
        functions = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
            elif isinstance(node, ast.ClassDef):
                classes.append(node.name)
            elif isinstance(node, ast.FunctionDef):
                functions.append(node.name)

        return {
            'path': str(file_path),
            'imports': imports,
            'classes': classes,
            'functions': functions,
            'lines': len(content.splitlines()),
        }
    except Exception as e:
        return {'path': str(file_path), 'error': str(e)}

def find_duplicate_files(root_dir: str) -> Dict[str, List[Path]]:
    """找到可能的重复文件"""
    files = find_python_files(root_dir)
    name_groups = {}

    for file_path in files:
        name = file_path.name
        if name not in name_groups:
            name_groups[name] = []
        name_groups[name].append(file_path)

    return {name: paths for name, paths in name_groups.items() if len(paths) > 1}

def generate_report(root_dir: str) -> Dict:
    """生成完整的分析报告"""
    print(f"🔍 分析仓库: {root_dir}")

    # 找到重复文件
    duplicates = find_duplicate_files(root_dir)
    print(f"📊 发现 {len(duplicates)} 组重复文件名")

    # 分析所有Python文件
    files = find_python_files(root_dir)
    analyses = []
    for file_path in files:
        analysis = analyze_file_structure(file_path)
        analyses.append(analysis)

    report = {
        'summary': {
            'total_files': len(files),
            'duplicate_groups': len(duplicates),
            'avg_lines': round(sum(item.get('lines', 0) for item in analyses) / max(len(analyses), 1), 2),
        },
        'duplicates': duplicates,
        'file_analyses': analyses
    }

    return report

def print_summary(report: Dict):
    """打印分析摘要"""
    print("\n" + "="*60)
    print("📋 仓库分析摘要")
    print("="*60)

    summary = report['summary']
    print(f"总Python文件数: {summary['total_files']}")
    print(f"重复文件组数: {summary['duplicate_groups']}")
    print(f"平均代码行数: {summary['avg_lines']}")

    print("\n🔍 重复文件详情:")
    for name, paths in report['duplicates'].items():
        print(f"  {name}:")
        for path in paths:
            print(f"    - {path}")

if __name__ == "__main__":
    root_dir = "."
    summary_only = False
    for arg in sys.argv[1:]:
        if arg == "--summary":
            summary_only = True
        else:
            root_dir = arg

    report = generate_report(root_dir)
    print_summary(report)

    if not summary_only:
        with open('cleanup_analysis.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n📄 详细报告已保存到: cleanup_analysis.json")
