# tests/framework/test_resampling_framework.py
import unittest
import tempfile
import pandas as pd
from pathlib import Path
import os
import sys

# Add project root to path to allow framework imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from resampling.core.resampling_engine.resampler import OHLCVResamplingStrategy, TimeframeResampler
# from framework.data_input.raw_data_reader import ParquetDataReader
# from framework.data_output.data_writer import ParquetDataWriter

class TestResamplingFramework(unittest.TestCase):
    """重采样框架单元测试"""
    
    def setUp(self):
        """测试设置"""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.strategy = OHLCVResamplingStrategy()
        self.resampler = TimeframeResampler(self.strategy)
        
        # 创建测试数据
        self.test_data = self._create_test_data()
    
    def _create_test_data(self) -> pd.DataFrame:
        """创建测试数据"""
        dates = pd.date_range('2025-01-01 09:30:00', periods=100, freq='1min')
        data = pd.DataFrame({
            'open': [100 + i * 0.1 for i in range(100)],
            'high': [100.5 + i * 0.1 for i in range(100)],
            'low': [99.5 + i * 0.1 for i in range(100)],
            'close': [100.2 + i * 0.1 for i in range(100)],
            'volume': [1000 + i for i in range(100)]
        }, index=dates)
        return data
    
    def test_resampling_strategy_15m(self):
        """测试15分钟重采样策略"""
        result = self.strategy.resample(self.test_data, '15m')
        
        # 验证结果
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 7) # 100 minutes / 15 min/period = 6.66 -> 7 periods
        self.assertLess(len(result), len(self.test_data))
        
        # 验证OHLCV计算
        self.assertIn('open', result.columns)
        self.assertIn('high', result.columns)
        self.assertIn('low', result.columns)
        self.assertIn('close', result.columns)
        self.assertIn('volume', result.columns)

        # Check first row values
        first_period_data = self.test_data.iloc[0:15]
        self.assertEqual(result.iloc[0]['open'], first_period_data.iloc[0]['open'])
        self.assertEqual(result.iloc[0]['high'], first_period_data['high'].max())
        self.assertEqual(result.iloc[0]['low'], first_period_data['low'].min())
        self.assertEqual(result.iloc[0]['close'], first_period_data.iloc[-1]['close'])
        self.assertEqual(result.iloc[0]['volume'], first_period_data['volume'].sum())

    def test_timeframe_validation(self):
        """测试时间周期验证"""
        # 测试有效时间周期
        self.assertTrue(self.strategy.validate_timeframe('15m'))
        self.assertTrue(self.strategy.validate_timeframe('1h'))
        
        # 测试无效时间周期
        self.assertFalse(self.strategy.validate_timeframe('invalid'))
        with self.assertRaises(ValueError):
            self.resampler.resample(self.test_data, 'invalid')

    def test_base_timeframe_selection(self):
        """测试基础时间周期选择"""
        self.assertEqual(self.strategy.get_base_timeframe('15m'), '1m')
        self.assertEqual(self.strategy.get_base_timeframe('2h'), '1h')
        self.assertEqual(self.strategy.get_base_timeframe('1d'), '1d')
        with self.assertRaises(ValueError):
            self.strategy.get_base_timeframe('invalid')

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir)

if __name__ == '__main__':
    unittest.main()
