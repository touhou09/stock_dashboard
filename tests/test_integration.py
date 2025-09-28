"""
통합 테스트 - Bronze Layer와 Silver Layer 연동 테스트
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

class TestIntegration:
    """통합 테스트 클래스"""
    
    @patch('bronze_layer_delta.storage.Client')
    @patch('silver_layer_delta.DeltaTable')
    @patch('bronze_layer_delta.write_deltalake')
    @patch('silver_layer_delta.write_deltalake')
    def test_bronze_to_silver_data_flow(self, mock_silver_write, mock_bronze_write, 
                                      mock_silver_delta, mock_bronze_client, 
                                      sample_sp500_data, sample_price_data, sample_dividend_data, sample_date):
        """Bronze Layer에서 Silver Layer로 데이터 흐름 테스트"""
        
        # 수정: Bronze Layer 모킹 설정
        with patch('bronze_layer_delta.requests.get') as mock_get, \
             patch('bronze_layer_delta.pd.read_html') as mock_read_html, \
             patch('bronze_layer_delta.yf.Ticker') as mock_ticker_class, \
             patch('time.sleep'):
            
            # Wikipedia 응답 모킹
            mock_response = Mock()
            mock_response.text = "test html"
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            mock_read_html.return_value = [sample_sp500_data]
            
            # yfinance 모킹
            mock_ticker = Mock()
            mock_ticker_class.return_value = mock_ticker
            mock_ticker.history.return_value = sample_price_data.set_index('date')
            mock_ticker.info = {
                'longName': 'Apple Inc.',
                'sector': 'Technology',
                'dividendYield': 0.0044,
                'dividendRate': 0.96,
                'exDividendDate': None,
                'dividendDate': None,
                'dividendFrequency': 4,
                'marketCap': 3000000000000,
                'currentPrice': 188.0
            }
            
            # Silver Layer 모킹
            mock_price_table = Mock()
            mock_dividend_table = Mock()
            mock_price_table.to_pandas.return_value = sample_price_data
            mock_dividend_table.to_pandas.return_value = sample_dividend_data
            
            def delta_table_side_effect(path):
                if 'prices' in path:
                    return mock_price_table
                else:
                    return mock_dividend_table
            
            mock_silver_delta.side_effect = delta_table_side_effect
            
            # Bronze Layer 실행
            bronze_layer = BronzeLayerDelta("test-bucket")
            bronze_layer.run_daily_collection(sample_date)
            
            # Silver Layer 실행
            silver_layer = SilverLayerDelta("test-bucket")
            silver_layer.run_silver_processing(sample_date)
            
            # 수정: 데이터 흐름 확인
            assert mock_bronze_write.call_count >= 2  # 가격 데이터와 배당 데이터
            assert mock_silver_write.call_count >= 2  # 통합 테이블과 배당주 테이블
    
    @patch('bronze_layer_delta.storage.Client')
    @patch('silver_layer_delta.DeltaTable')
    def test_data_quality_validation(self, mock_silver_delta, mock_bronze_client, 
                                   sample_price_data, sample_dividend_data, sample_date):
        """데이터 품질 검증 테스트"""
        
        # 수정: 데이터 품질 검증을 위한 테스트 데이터
        price_df = sample_price_data.copy()
        dividend_df = sample_dividend_data.copy()
        
        # 결측값이 있는 데이터 추가
        price_df_with_nulls = pd.concat([
            price_df,
            pd.DataFrame({
                'ticker': ['INVALID'],
                'date': [sample_date],
                'Close': [None],  # 결측값
                'Volume': [0]
            })
        ], ignore_index=True)
        
        # Silver Layer 모킹
        mock_price_table = Mock()
        mock_dividend_table = Mock()
        mock_price_table.to_pandas.return_value = price_df_with_nulls
        mock_dividend_table.to_pandas.return_value = dividend_df
        
        def delta_table_side_effect(path):
            if 'prices' in path:
                return mock_price_table
            else:
                return mock_dividend_table
        
        mock_silver_delta.side_effect = delta_table_side_effect
        
        silver_layer = SilverLayerDelta("test-bucket")
        price_df, dividend_df = silver_layer.load_bronze_data(sample_date)
        unified_df = silver_layer.create_unified_table(price_df, dividend_df)
        
        # 수정: 데이터 품질 검증
        assert len(unified_df) == 4  # 3개 유효 + 1개 무효
        assert unified_df['Close'].isnull().sum() == 1  # 1개 결측값
        assert unified_df['is_dividend_stock'].sum() == 2  # 2개 배당주
    
    @patch('bronze_layer_delta.storage.Client')
    def test_error_handling_and_recovery(self, mock_bronze_client, sample_date):
        """에러 처리 및 복구 테스트"""
        
        # 수정: 에러 시나리오 테스트
        with patch('bronze_layer_delta.requests.get') as mock_get:
            mock_get.side_effect = Exception("Network error")
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            
            with pytest.raises(RuntimeError):
                bronze_layer.get_sp500_from_wikipedia(max_retries=1)
    
    def test_symbol_normalization_consistency(self):
        """심볼 정규화 일관성 테스트"""
        
        with patch('bronze_layer_delta.storage.Client'):
            bronze_layer = BronzeLayerDelta("test-bucket")
            
            # 수정: 다양한 심볼 형식 테스트
            test_symbols = [
                ('BRK.B', 'BRK-B'),
                ('BRK.A', 'BRK-A'),
                ('AAPL', 'AAPL'),
                ('  msft  ', 'MSFT'),
                ('googl', 'GOOGL'),
                ('', ''),
                (None, None)
            ]
            
            for input_symbol, expected in test_symbols:
                if input_symbol is None:
                    continue  # None 값은 처리하지 않음
                result = bronze_layer.to_yahoo_symbol(input_symbol)
                assert result == expected, f"Failed for input: {input_symbol}"
    
    @patch('bronze_layer_delta.storage.Client')
    @patch('silver_layer_delta.DeltaTable')
    def test_performance_with_large_dataset(self, mock_silver_delta, mock_bronze_client, sample_date):
        """대용량 데이터셋 성능 테스트"""
        
        # 수정: 대용량 데이터 생성
        large_price_data = pd.DataFrame({
            'ticker': [f'STOCK{i:03d}' for i in range(100)],
            'date': [sample_date] * 100,
            'Close': [100.0 + i for i in range(100)],
            'Volume': [1000000] * 100
        })

        # 수정: 필요한 모든 컬럼 추가
        large_dividend_data = pd.DataFrame({
            'ticker': [f'STOCK{i:03d}' for i in range(100)],
            'company_name': [f'Company {i}' for i in range(100)],
            'sector': ['Technology'] * 100,
            'has_dividend': [i % 2 == 0 for i in range(100)],  # 절반이 배당주
            'dividend_yield': [0.01 * (i % 5) for i in range(100)],
            'dividend_yield_percent': [0.01 * (i % 5) * 100 for i in range(100)],
            'dividend_rate': [0.5 + (i % 3) for i in range(100)],
            'ex_dividend_date': [sample_date + timedelta(days=30 + i) for i in range(100)],
            'payment_date': [sample_date + timedelta(days=60 + i) for i in range(100)],
            'dividend_frequency': ['quarterly'] * 100,
            'market_cap': [1000000000 + i * 10000000 for i in range(100)],
            'last_price': [100.0 + i for i in range(100)]
        })
        
        # Silver Layer 모킹
        mock_price_table = Mock()
        mock_dividend_table = Mock()
        mock_price_table.to_pandas.return_value = large_price_data
        mock_dividend_table.to_pandas.return_value = large_dividend_data
        
        def delta_table_side_effect(path):
            if 'prices' in path:
                return mock_price_table
            else:
                return mock_dividend_table
        
        mock_silver_delta.side_effect = delta_table_side_effect
        
        silver_layer = SilverLayerDelta("test-bucket")
        price_df, dividend_df = silver_layer.load_bronze_data(sample_date)
        unified_df = silver_layer.create_unified_table(price_df, dividend_df)
        
        # 수정: 성능 검증
        assert len(unified_df) == 100
        assert unified_df['is_dividend_stock'].sum() == 50  # 절반이 배당주
        assert unified_df['ticker'].nunique() == 100  # 모든 종목이 유니크 