"""
Stock Dashboard 통합 테스트
Bronze Layer와 Silver Layer의 핵심 기능을 테스트합니다.
"""

import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.app.bronze.bronze_layer_delta import BronzeLayerDelta
from src.app.silver.silver_layer_delta import SilverLayerDelta
from src.utils.data_storage import DeltaStorageManager

class TestBronzeLayer:
    """Bronze Layer 테스트"""
    
    def test_bronze_layer_init(self):
        """Bronze Layer 초기화 테스트"""
        with patch('src.app.bronze.bronze_layer_delta.storage.Client'):
            bronze_layer = BronzeLayerDelta(
                gcs_bucket="test-bucket",
                gcs_path="test/path"
            )
            
            assert bronze_layer.gcs_bucket == "test-bucket"
            assert bronze_layer.gcs_path == "test/path"
            assert bronze_layer.price_table_path == "gs://test-bucket/test/path/bronze_price_daily"
            assert bronze_layer.dividend_table_path == "gs://test-bucket/test/path/bronze_dividend_events"
    
    def test_symbol_normalization(self):
        """심볼 정규화 테스트"""
        with patch('src.app.bronze.bronze_layer_delta.storage.Client'):
            bronze_layer = BronzeLayerDelta("test-bucket")
            
            # 다양한 심볼 형식 테스트
            assert bronze_layer.to_yahoo_symbol("BRK.B") == "BRK-B"
            assert bronze_layer.to_yahoo_symbol("BRK.A") == "BRK-A"
            assert bronze_layer.to_yahoo_symbol("AAPL") == "AAPL"
            assert bronze_layer.to_yahoo_symbol("  msft  ") == "MSFT"
            assert bronze_layer.to_yahoo_symbol("googl") == "GOOGL"
    
    @patch('src.app.bronze.bronze_layer_delta.requests.get')
    @patch('src.app.bronze.bronze_layer_delta.pd.read_html')
    def test_sp500_data_collection(self, mock_read_html, mock_get):
        """S&P 500 데이터 수집 테스트"""
        with patch('src.app.bronze.bronze_layer_delta.storage.Client'):
            # 모킹 설정
            mock_response = Mock()
            mock_response.text = "test html"
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            # 테스트 데이터
            test_data = pd.DataFrame({
                'Symbol': ['AAPL', 'MSFT', 'GOOGL'],
                'Security': ['Apple Inc.', 'Microsoft Corporation', 'Alphabet Inc.'],
                'GICS Sector': ['Technology', 'Technology', 'Technology']
            })
            mock_read_html.return_value = [test_data]
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            result = bronze_layer.get_sp500_from_wikipedia()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 3
            assert 'Symbol' in result.columns
            assert 'AAPL' in result['Symbol'].values

class TestSilverLayer:
    """Silver Layer 테스트"""
    
    def test_silver_layer_init(self):
        """Silver Layer 초기화 테스트"""
        silver_layer = SilverLayerDelta(
            gcs_bucket="test-bucket",
            bronze_path="test/bronze",
            silver_path="test/silver"
        )
        
        assert silver_layer.gcs_bucket == "test-bucket"
        assert silver_layer.bronze_path == "test/bronze"
        assert silver_layer.silver_path == "test/silver"
        assert silver_layer.bronze_price_path == "gs://test-bucket/test/bronze/bronze_price_daily"
        assert silver_layer.bronze_dividend_events_path == "gs://test-bucket/test/bronze/bronze_dividend_events"
        assert silver_layer.silver_dividend_metrics_path == "gs://test-bucket/test/silver/silver_dividend_metrics_daily"
    
    def test_dividend_metrics_calculation(self):
        """배당 지표 계산 테스트"""
        silver_layer = SilverLayerDelta("test-bucket")
        
        # 테스트 데이터
        price_df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'GOOGL'],
            'close': [150.0, 300.0, 100.0]
        })
        
        dividend_events_df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'AAPL'],
            'amount': [0.25, 0.75, 0.25],
            'ex_date': [date(2024, 1, 1), date(2024, 1, 1), date(2024, 4, 1)]
        })
        
        target_date = date(2024, 6, 1)
        result = silver_layer.build_dividend_metrics_daily(price_df, dividend_events_df, target_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert 'date' in result.columns
        assert 'ticker' in result.columns
        assert 'dividend_ttm' in result.columns
        assert 'dividend_yield_ttm' in result.columns
        
        # AAPL의 TTM 배당금 확인 (0.25 + 0.25 = 0.50)
        aapl_data = result[result['ticker'] == 'AAPL'].iloc[0]
        assert aapl_data['dividend_ttm'] == 0.50
        assert aapl_data['dividend_yield_ttm'] == (0.50 / 150.0) * 100
    
    def test_empty_price_data_handling(self):
        """빈 가격 데이터 처리 테스트"""
        silver_layer = SilverLayerDelta("test-bucket")
        
        empty_price_df = pd.DataFrame()
        dividend_events_df = pd.DataFrame()
        target_date = date(2024, 6, 1)
        
        result = silver_layer.build_dividend_metrics_daily(empty_price_df, dividend_events_df, target_date)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ['date', 'ticker', 'last_price', 'market_cap', 
                                      'dividend_ttm', 'dividend_yield_ttm', 'div_count_1y', 
                                      'last_div_date', 'updated_at']

class TestDataStorage:
    """데이터 저장 테스트"""
    
    def test_delta_storage_manager_init(self):
        """Delta Storage Manager 초기화 테스트"""
        with patch('src.utils.data_storage.storage.Client'):
            storage_manager = DeltaStorageManager(
                gcs_bucket="test-bucket",
                gcs_path="test/path"
            )
            
            assert storage_manager.gcs_bucket == "test-bucket"
            assert storage_manager.gcs_path == "test/path"
            assert storage_manager.price_table_path == "gs://test-bucket/test/path/bronze_price_daily"
            assert storage_manager.dividend_events_table_path == "gs://test-bucket/test/path/bronze_dividend_events"
    
    @patch('src.utils.data_storage.DeltaTable')
    def test_existing_data_check(self, mock_delta_table):
        """기존 데이터 확인 테스트"""
        with patch('src.utils.data_storage.storage.Client'):
            # 모킹 설정
            mock_table = Mock()
            mock_delta_table.return_value = mock_table
            
            # 테스트 데이터
            test_data = pd.DataFrame({
                'date': [date(2024, 1, 15), date(2024, 1, 16)],
                'ticker': ['AAPL', 'MSFT'],
                'close': [150.0, 300.0]
            })
            mock_table.to_pandas.return_value = test_data
            
            storage_manager = DeltaStorageManager("test-bucket")
            
            # 기존 데이터가 있는 경우
            assert storage_manager.check_existing_data("test-path", date(2024, 1, 15)) == True
            
            # 기존 데이터가 없는 경우
            assert storage_manager.check_existing_data("test-path", date(2024, 1, 17)) == False

class TestIntegration:
    """통합 테스트"""
    
    @patch('src.app.silver.silver_layer_delta.DeltaTable')
    def test_silver_layer_data_loading(self, mock_delta_table):
        """Silver Layer 데이터 로딩 통합 테스트"""
        # 모킹 설정
        mock_price_table = Mock()
        mock_dividend_table = Mock()
        
        # 테스트 데이터
        price_data = pd.DataFrame({
            'date': [date(2024, 1, 15)],
            'ticker': ['AAPL'],
            'close': [150.0]
        })
        
        dividend_data = pd.DataFrame({
            'ticker': ['AAPL'],
            'amount': [0.25],
            'ex_date': [date(2024, 1, 1)]
        })
        
        mock_price_table.to_pandas.return_value = price_data
        mock_dividend_table.to_pandas.return_value = dividend_data
        
        def delta_table_side_effect(path):
            if 'price' in path:
                return mock_price_table
            else:
                return mock_dividend_table
        
        mock_delta_table.side_effect = delta_table_side_effect
        
        silver_layer = SilverLayerDelta("test-bucket")
        
        # 가격 데이터 로드 테스트
        price_df = silver_layer.load_bronze_price_data(date(2024, 1, 15))
        assert len(price_df) == 1
        assert price_df['ticker'].iloc[0] == 'AAPL'
        
        # 배당 이벤트 데이터 로드 테스트
        dividend_df = silver_layer.load_bronze_dividend_events(date(2024, 1, 15))
        assert len(dividend_df) == 1
        assert dividend_df['ticker'].iloc[0] == 'AAPL'
    
    @patch('src.app.silver.silver_layer_delta.write_deltalake')
    @patch('src.app.silver.silver_layer_delta.DeltaTable')
    def test_silver_layer_save_with_overwrite(self, mock_delta_table, mock_write_deltalake):
        """Silver Layer 저장 및 덮어쓰기 테스트"""
        # 기존 데이터가 있는 경우 모킹
        mock_table = Mock()
        existing_data = pd.DataFrame({
            'date': [date(2024, 1, 15)],
            'ticker': ['AAPL'],
            'dividend_ttm': [0.50]
        })
        mock_table.to_pandas.return_value = existing_data
        mock_delta_table.return_value = mock_table
        
        silver_layer = SilverLayerDelta("test-bucket")
        
        # 새 데이터
        new_data = pd.DataFrame({
            'date': [date(2024, 1, 15)],
            'ticker': ['AAPL'],
            'dividend_ttm': [0.75]
        })
        
        silver_layer.save_dividend_metrics_to_delta(new_data, date(2024, 1, 15))
        
        # write_deltalake이 호출되었는지 확인
        mock_write_deltalake.assert_called_once()
        call_args = mock_write_deltalake.call_args
        assert call_args[1]['mode'] == 'overwrite'

# Fixtures
@pytest.fixture
def sample_date():
    """테스트용 날짜"""
    return date(2024, 1, 15)

@pytest.fixture
def sample_price_data():
    """테스트용 가격 데이터"""
    return pd.DataFrame({
        'ticker': ['AAPL', 'MSFT', 'GOOGL'],
        'date': [date(2024, 1, 15)] * 3,
        'close': [150.0, 300.0, 100.0]
    })

@pytest.fixture
def sample_dividend_data():
    """테스트용 배당 데이터"""
    return pd.DataFrame({
        'ticker': ['AAPL', 'MSFT'],
        'amount': [0.25, 0.75],
        'ex_date': [date(2024, 1, 1), date(2024, 1, 1)]
    })

if __name__ == "__main__":
    pytest.main([__file__])
