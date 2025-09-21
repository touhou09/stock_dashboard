"""
Silver Layer Delta Lake 모듈 테스트
"""

import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from silver_layer_delta import SilverLayerDelta

class TestSilverLayerDelta:
    """SilverLayerDelta 클래스 테스트"""
    
    def test_init(self):
        """초기화 테스트"""
        silver_layer = SilverLayerDelta(
            gcs_bucket="test-bucket",
            bronze_path="test/bronze",
            silver_path="test/silver"
        )
        
        assert silver_layer.gcs_bucket == "test-bucket"
        assert silver_layer.bronze_path == "test/bronze"
        assert silver_layer.silver_path == "test/silver"
        assert silver_layer.bronze_price_path == "gs://test-bucket/test/bronze/sp500_daily_prices"
        assert silver_layer.bronze_dividend_path == "gs://test-bucket/test/bronze/sp500_dividend_info"
        assert silver_layer.silver_unified_path == "gs://test-bucket/test/silver/unified_stock_data"
        assert silver_layer.silver_dividend_path == "gs://test-bucket/test/silver/dividend_stocks"
    
    @patch('silver_layer_delta.DeltaTable')
    def test_load_bronze_data_success(self, mock_delta_table, sample_price_data, sample_dividend_data, sample_date):
        """Bronze Layer 데이터 로드 성공 테스트"""
        # 수정: Delta Table 모킹 개선
        mock_price_table = Mock()
        mock_dividend_table = Mock()
        mock_price_table.to_pandas.return_value = sample_price_data
        mock_dividend_table.to_pandas.return_value = sample_dividend_data
        
        def delta_table_side_effect(path):
            if 'prices' in path:
                return mock_price_table
            else:
                return mock_dividend_table
        
        mock_delta_table.side_effect = delta_table_side_effect
        
        silver_layer = SilverLayerDelta("test-bucket")
        price_df, dividend_df = silver_layer.load_bronze_data(sample_date)
        
        assert isinstance(price_df, pd.DataFrame)
        assert isinstance(dividend_df, pd.DataFrame)
        assert len(price_df) == 3
        assert len(dividend_df) == 3
        assert 'ticker' in price_df.columns
        assert 'ticker' in dividend_df.columns
    
    @patch('silver_layer_delta.DeltaTable')
    def test_load_bronze_data_failure(self, mock_delta_table):
        """Bronze Layer 데이터 로드 실패 테스트"""
        # 수정: 실패 시나리오 모킹
        mock_delta_table.side_effect = Exception("Delta Table not found")
        
        silver_layer = SilverLayerDelta("test-bucket")
        
        with pytest.raises(Exception, match="Bronze Layer 데이터 로드 실패"):
            silver_layer.load_bronze_data()
    
    def test_create_unified_table(self, sample_price_data, sample_dividend_data):
        """통합 테이블 생성 테스트"""
        silver_layer = SilverLayerDelta("test-bucket")
        
        # 수정: 테스트 데이터 준비
        price_df = sample_price_data.copy()
        dividend_df = sample_dividend_data.copy()
        
        result = silver_layer.create_unified_table(price_df, dividend_df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert 'is_dividend_stock' in result.columns
        assert 'processing_timestamp' in result.columns
        assert result['is_dividend_stock'].sum() == 2  # AAPL, MSFT가 배당주
        
        # 수정: 조인 결과 확인
        assert 'company_name' in result.columns
        assert 'sector' in result.columns
        assert 'dividend_yield' in result.columns
    
    def test_create_unified_table_with_missing_data(self):
        """결측 데이터가 있는 통합 테이블 생성 테스트"""
        silver_layer = SilverLayerDelta("test-bucket")
        
        # 수정: 결측 데이터가 있는 테스트 케이스
        price_df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'UNKNOWN'],
            'date': [date(2024, 1, 15)] * 3,
            'Close': [188.0, 382.0, 100.0]
        })
        
        dividend_df = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT'],
            'company_name': ['Apple Inc.', 'Microsoft Corporation'],
            'sector': ['Technology', 'Technology'],
            'has_dividend': [True, True],
            'dividend_yield': [0.0044, 0.0072]
        })
        
        result = silver_layer.create_unified_table(price_df, dividend_df)
        
        assert len(result) == 3
        assert result['is_dividend_stock'].sum() == 2
        assert result[result['ticker'] == 'UNKNOWN']['is_dividend_stock'].iloc[0] == False
    
    @patch('silver_layer_delta.write_deltalake')
    @patch('silver_layer_delta.DeltaTable')
    def test_save_unified_data_new_tables(self, mock_delta_table, mock_write_deltalake, sample_price_data, sample_dividend_data, sample_date):
        """통합 데이터 저장 테스트 (새 테이블)"""
        # 수정: Delta Table이 존재하지 않는 경우 모킹
        mock_delta_table.side_effect = Exception("Table not found")
        
        silver_layer = SilverLayerDelta("test-bucket")
        unified_df = silver_layer.create_unified_table(sample_price_data, sample_dividend_data)
        
        silver_layer.save_unified_data(unified_df, sample_date)
        
        # 수정: write_deltalake 호출 확인
        assert mock_write_deltalake.call_count == 2  # 통합 테이블과 배당주 테이블
        
        # 통합 테이블 저장 확인
        unified_call = mock_write_deltalake.call_args_list[0]
        assert unified_call[0][0] == silver_layer.silver_unified_path
        assert unified_call[1]['mode'] == 'overwrite'
        assert unified_call[1]['partition_by'] == ['date', 'is_dividend_stock']
        
        # 배당주 테이블 저장 확인
        dividend_call = mock_write_deltalake.call_args_list[1]
        assert dividend_call[0][0] == silver_layer.silver_dividend_path
        assert dividend_call[1]['mode'] == 'overwrite'
        assert dividend_call[1]['partition_by'] == ['date', 'sector']
    
    @patch('silver_layer_delta.write_deltalake')
    @patch('silver_layer_delta.DeltaTable')
    def test_save_unified_data_existing_tables(self, mock_delta_table, mock_write_deltalake, sample_price_data, sample_dividend_data, sample_date):
        """통합 데이터 저장 테스트 (기존 테이블)"""
        # 수정: Delta Table이 존재하는 경우 모킹
        mock_table = Mock()
        mock_delta_table.return_value = mock_table
        
        silver_layer = SilverLayerDelta("test-bucket")
        unified_df = silver_layer.create_unified_table(sample_price_data, sample_dividend_data)
        
        silver_layer.save_unified_data(unified_df, sample_date)
        
        # 수정: write_deltalake 호출 확인
        assert mock_write_deltalake.call_count == 2
        
        # 통합 테이블 저장 확인
        unified_call = mock_write_deltalake.call_args_list[0]
        assert unified_call[1]['mode'] == 'append'
        
        # 배당주 테이블 저장 확인
        dividend_call = mock_write_deltalake.call_args_list[1]
        assert dividend_call[1]['mode'] == 'append'
    
    def test_analyze_dividend_stocks(self, sample_price_data, sample_dividend_data):
        """배당주 분석 테스트"""
        silver_layer = SilverLayerDelta("test-bucket")
        unified_df = silver_layer.create_unified_table(sample_price_data, sample_dividend_data)
        
        # 수정: 분석 결과를 캡처하기 위한 모킹
        with patch('silver_layer_delta.logger') as mock_logger:
            silver_layer.analyze_dividend_stocks(unified_df)
            
            # 수정: 로그 호출 확인
            assert mock_logger.info.call_count > 0
    
    def test_analyze_dividend_stocks_no_dividend_stocks(self):
        """배당주가 없는 경우 분석 테스트"""
        silver_layer = SilverLayerDelta("test-bucket")
        
        # 수정: 배당주가 없는 데이터
        price_df = pd.DataFrame({
            'ticker': ['GOOGL'],
            'date': [date(2024, 1, 15)],
            'Close': [142.0]
        })
        
        dividend_df = pd.DataFrame({
            'ticker': ['GOOGL'],
            'company_name': ['Alphabet Inc.'],
            'sector': ['Technology'],
            'has_dividend': [False],
            'dividend_yield': [0.0]
        })
        
        unified_df = silver_layer.create_unified_table(price_df, dividend_df)
        
        with patch('silver_layer_delta.logger') as mock_logger:
            silver_layer.analyze_dividend_stocks(unified_df)
            
            # 수정: "배당주가 없습니다" 메시지 확인
            log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
            assert any("배당주가 없습니다" in call for call in log_calls)
    
    @patch('silver_layer_delta.SilverLayerDelta.save_unified_data')
    @patch('silver_layer_delta.SilverLayerDelta.analyze_dividend_stocks')
    @patch('silver_layer_delta.SilverLayerDelta.create_unified_table')
    @patch('silver_layer_delta.SilverLayerDelta.load_bronze_data')
    def test_run_silver_processing_success(self, mock_load_bronze, mock_create_unified, 
                                         mock_analyze, mock_save_unified, sample_price_data, sample_dividend_data, sample_date):
        """Silver Layer 처리 실행 성공 테스트"""
        # 수정: 모킹 설정 개선
        mock_load_bronze.return_value = (sample_price_data, sample_dividend_data)
        mock_unified_df = pd.DataFrame({'ticker': ['AAPL'], 'is_dividend_stock': [True]})
        mock_create_unified.return_value = mock_unified_df
        
        silver_layer = SilverLayerDelta("test-bucket")
        
        silver_layer.run_silver_processing(sample_date)
        
        # 수정: 메서드 호출 확인
        mock_load_bronze.assert_called_once_with(sample_date)
        mock_create_unified.assert_called_once()
        mock_save_unified.assert_called_once()
        mock_analyze.assert_called_once()
    
    def test_run_silver_processing_with_default_date(self):
        """기본 날짜로 Silver Layer 처리 실행 테스트"""
        with patch('silver_layer_delta.datetime') as mock_datetime:
            # 수정: 날짜 모킹 개선
            mock_datetime.now.return_value.date.return_value = date(2024, 1, 16)
            mock_datetime.now.return_value = datetime(2024, 1, 16)
            
            silver_layer = SilverLayerDelta("test-bucket")
            
            with patch.object(silver_layer, 'load_bronze_data') as mock_load_bronze:
                mock_load_bronze.side_effect = Exception("Test error")
                
                with pytest.raises(Exception):
                    silver_layer.run_silver_processing()
    
    @patch('silver_layer_delta.SilverLayerDelta.load_bronze_data')
    def test_run_silver_processing_failure(self, mock_load_bronze):
        """Silver Layer 처리 실행 실패 테스트"""
        # 수정: 실패 시나리오 모킹
        mock_load_bronze.side_effect = Exception("Data load failed")
        
        silver_layer = SilverLayerDelta("test-bucket")
        
        with pytest.raises(Exception, match="Silver Layer 처리 실패"):
            silver_layer.run_silver_processing() 