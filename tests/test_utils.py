"""
유틸리티 함수 및 헬퍼 함수 테스트
"""

import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestDataValidation:
    """데이터 검증 테스트"""
    
    def test_validate_price_data_structure(self, sample_price_data):
        """가격 데이터 구조 검증 테스트"""
        # 수정: 필수 컬럼 확인
        required_columns = ['ticker', 'date', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        for col in required_columns:
            assert col in sample_price_data.columns, f"Missing required column: {col}"
        
        # 수정: 데이터 타입 확인
        assert sample_price_data['ticker'].dtype == 'object'
        assert pd.api.types.is_datetime64_any_dtype(sample_price_data['date']) or \
               pd.api.types.is_object_dtype(sample_price_data['date'])
        assert pd.api.types.is_numeric_dtype(sample_price_data['Close'])
        assert pd.api.types.is_numeric_dtype(sample_price_data['Volume'])
    
    def test_validate_dividend_data_structure(self, sample_dividend_data):
        """배당 데이터 구조 검증 테스트"""
        # 수정: 필수 컬럼 확인
        required_columns = ['ticker', 'company_name', 'sector', 'has_dividend', 'dividend_yield']
        
        for col in required_columns:
            assert col in sample_dividend_data.columns, f"Missing required column: {col}"
        
        # 수정: 데이터 타입 확인
        assert sample_dividend_data['ticker'].dtype == 'object'
        assert sample_dividend_data['has_dividend'].dtype == 'bool'
        assert pd.api.types.is_numeric_dtype(sample_dividend_data['dividend_yield'])
    
    def test_validate_data_quality(self, sample_price_data, sample_dividend_data):
        """데이터 품질 검증 테스트"""
        # 수정: 가격 데이터 품질 검증
        assert sample_price_data['Close'].min() > 0, "Close price should be positive"
        assert sample_price_data['Volume'].min() >= 0, "Volume should be non-negative"
        assert sample_price_data['High'] >= sample_price_data['Low'], "High should be >= Low"
        
        # 수정: 배당 데이터 품질 검증
        assert sample_dividend_data['dividend_yield'].min() >= 0, "Dividend yield should be non-negative"
        assert sample_dividend_data['has_dividend'].dtype == 'bool', "has_dividend should be boolean"

class TestDataTransformation:
    """데이터 변환 테스트"""
    
    def test_symbol_normalization_edge_cases(self):
        """심볼 정규화 엣지 케이스 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            from bronze_layer_delta import BronzeLayerDelta
            bronze_layer = BronzeLayerDelta("test-bucket")
            
            # 수정: 엣지 케이스 테스트
            edge_cases = [
                ('', ''),
                ('   ', ''),
                ('A.B.C', 'A-B-C'),
                ('123.456', '123-456'),
                ('TEST.SYMBOL', 'TEST-SYMBOL')
            ]
            
            for input_symbol, expected in edge_cases:
                result = bronze_layer.to_yahoo_symbol(input_symbol)
                assert result == expected, f"Failed for input: '{input_symbol}'"
    
    def test_data_merging_logic(self, sample_price_data, sample_dividend_data):
        """데이터 병합 로직 테스트"""
        # 수정: 병합 테스트
        merged_df = sample_price_data.merge(
            sample_dividend_data[['ticker', 'has_dividend', 'dividend_yield']], 
            on='ticker', 
            how='left'
        )
        
        assert len(merged_df) == len(sample_price_data)
        assert 'has_dividend' in merged_df.columns
        assert 'dividend_yield' in merged_df.columns
        
        # 수정: 배당주 여부 확인
        dividend_stocks = merged_df[merged_df['has_dividend'] == True]
        assert len(dividend_stocks) == 2  # AAPL, MSFT