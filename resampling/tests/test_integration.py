# tests/framework/test_resampling_integration.py
import unittest
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import yaml
import os
import sys

# Add project root to path to allow framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from resampling.core.controller import ResamplingController

class TestResamplingIntegration(unittest.TestCase):
    """重采样集成测试"""
    
    def setUp(self):
        """测试设置"""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.setup_test_data()
    
    def setup_test_data(self):
        """设置测试数据"""
        # 创建测试数据目录结构 (raw_data/1m/SYMBOL.parquet)
        raw_data_1m_dir = self.temp_dir / "raw_data" / "1m"
        raw_data_1m_dir.mkdir(parents=True)
        
        # 创建1分钟数据文件
        dates_1m = pd.date_range('2025-01-01 09:30:00', periods=240, freq='1min')  # 4 hours of data
        test_data_1m = pd.DataFrame({
            'open': [100 + i * 0.01 for i in range(240)],
            'high': [101 + i * 0.01 for i in range(240)],
            'low': [99 + i * 0.01 for i in range(240)],
            'close': [100.5 + i * 0.01 for i in range(240)],
            'volume': [1000 + i % 100 for i in range(240)]
        }, index=dates_1m)
        test_data_1m.to_parquet(raw_data_1m_dir / "0700.HK.parquet")

        # 创建1小时数据文件 (for 2h/4h resampling)
        raw_data_1h_dir = self.temp_dir / "raw_data" / "1h"
        raw_data_1h_dir.mkdir(parents=True)
        dates_1h = pd.date_range('2025-01-01 09:00:00', periods=8, freq='1H')
        test_data_1h = pd.DataFrame({
            'open': [200, 210, 205, 215, 220, 225, 230, 235],
            'high': [201, 211, 206, 216, 221, 226, 231, 236],
            'low': [199, 209, 204, 214, 219, 224, 229, 234],
            'close': [200.5, 210.5, 205.5, 215.5, 220.5, 225.5, 230.5, 235.5],
            'volume': [10000, 11000, 10500, 11500, 12000, 12500, 13000, 13500]
        }, index=dates_1h)
        test_data_1h.to_parquet(raw_data_1h_dir / "0700.HK.parquet")

    def test_full_resampling_process(self):
        """测试完整重采样过程"""
        # 创建配置文件
        config_path = self.temp_dir / "config.yaml"
        config_data = {
            'data_root': str(self.temp_dir),
            'target_timeframes': ['5m', '15m', '30m', '2h'],
            'symbols': ['0700.HK'],
            'parallel_workers': 1, # Use 1 for predictable testing
            'overwrite_existing': True,
            'metadata_enabled': True,
            'output_format': 'parquet',
            'output_layout': 'raw_data/{timeframe}/{symbol}.parquet',
            'chunk_size': 10000
        }
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)

        # 创建控制器
        controller = ResamplingController(config_path)
        
        # 验证设置
        validation_results = controller.validate_setup()
        self.assertTrue(validation_results['data_root_exists'])
        self.assertTrue(validation_results['found_available_symbols'])
        self.assertTrue(validation_results['target_timeframes_valid'])
        
        # 执行重采样
        results = controller.run_resampling()
        
        # 验证结果
        self.assertEqual(results['total_tasks'], 4)
        self.assertEqual(results['successful'], 4)
        self.assertEqual(results['failed'], 0)
        
        # 验证输出文件
        for timeframe in ['5m', '15m', '30m', '2h']:
            output_file = self.temp_dir / "raw_data" / timeframe / "0700.HK.parquet"
            self.assertTrue(output_file.exists(), f"Output file missing: {output_file}")
            # Verify content
            df = pd.read_parquet(output_file)
            self.assertFalse(df.empty)
            self.assertEqual(df.index.name, 'datetime')

    def tearDown(self):
        """清理测试数据"""
        shutil.rmtree(self.temp_dir)

if __name__ == '__main__':
    unittest.main()
