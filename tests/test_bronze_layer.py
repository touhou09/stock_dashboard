"""
분리된 Bronze Layer 모듈 테스트
"""

import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bronze_layer import BronzeLayer
from data_collectors import SP500Collector, PriceDataCollector, DividendDataCollector
from data_storage import DeltaStorageManager
from data_validators import DataValidator, BackfillValidator

class TestSP500Collector:
    """SP500Collector 클래스 테스트"""
    
    def test_to_yahoo_symbol(self):
        """심볼 변환 테스트"""
        collector = SP500Collector()
        
        assert collector.to_yahoo_symbol("BRK.B") == "BRK-B"
        assert collector.to_yahoo_symbol("BRK.A") == "BRK-A"
        assert collector.to_yahoo_symbol("AAPL") == "AAPL"
        assert collector.to_yahoo_symbol("  msft  ") == "MSFT"
    
    def test_normalize_symbols(self, sample_sp500_data):
        """심볼 정규화 테스트"""
        collector = SP500Collector()
        result = collector.normalize_symbols(sample_sp500_data)
        
        assert 'Symbol' in result.columns
        assert result['Symbol'].iloc[0] == 'AAPL'

class TestPriceDataCollector:
    """PriceDataCollector 클래스 테스트"""
    
    @patch('data_collectors.yf.Ticker')
    def test_get_daily_data_for_tickers(self, mock_ticker_class, sample_date):
        """일일 데이터 수집 테스트"""
        mock_ticker = Mock()
        mock_ticker_class.return_value = mock_ticker
        
        mock_hist = pd.DataFrame({
            'Open': [185.0],
            'High': [190.0],
            'Low': [180.0],
            'Close': [188.0],
            'Volume': [50000000]
        }, index=[sample_date])
        mock_ticker.history.return_value = mock_hist
        
        collector = PriceDataCollector()
        tickers = ['AAPL']
        
        with patch('time.sleep'):
            all_data, successful, failed = collector.get_daily_data_for_tickers(tickers, sample_date)
        
        assert len(all_data) == 1
        assert len(successful) == 1
        assert len(failed) == 0
        assert all_data[0]['ticker'].iloc[0] == 'AAPL'

class TestDataValidator:
    """DataValidator 클래스 테스트"""
    
    def test_is_trading_day(self):
        """거래일 확인 테스트"""
        validator = DataValidator()
        
        # 평일
        monday = date(2024, 1, 15)  # 월요일
        friday = date(2024, 1, 19)  # 금요일
        
        assert validator.is_trading_day(monday) == True
        assert validator.is_trading_day(friday) == True
        
        # 주말
        saturday = date(2024, 1, 20)  # 토요일
        sunday = date(2024, 1, 21)   # 일요일
        
        assert validator.is_trading_day(saturday) == False
        assert validator.is_trading_day(sunday) == False
    
    def test_validate_price_data(self):
        """가격 데이터 검증 테스트"""
        validator = DataValidator()
        
        # 정상 데이터
        valid_data = pd.DataFrame({
            'date': [date(2024, 1, 15)],
            'ticker': ['AAPL'],
            'open': [185.0],
            'high': [190.0],
            'low': [180.0],
            'close': [188.0],
            'volume': [50000000]
        })
        
        result = validator.validate_price_data(valid_data)
        assert len(result) == 1
        
        # 잘못된 데이터
        invalid_data = pd.DataFrame({
            'date': [date(2024, 1, 15)],
            'ticker': ['AAPL'],
            'open': [-185.0],  # 음수
            'high': [190.0],
            'low': [180.0],
            'close': [188.0],
            'volume': [50000000]
        })
        
        with patch('data_validators.logger') as mock_logger:
            result = validator.validate_price_data(invalid_data)
            assert mock_logger.warning.called

class TestBronzeLayer:
    """BronzeLayer 메인 클래스 테스트"""
    
    @patch('bronze_layer.DeltaStorageManager')
    def test_init(self, mock_storage):
        """초기화 테스트"""
        bronze_layer = BronzeLayer("test-bucket")
        
        assert isinstance(bronze_layer.sp500_collector, SP500Collector)
        assert isinstance(bronze_layer.price_collector, PriceDataCollector)
        assert isinstance(bronze_layer.dividend_collector, DividendDataCollector)
        assert isinstance(bronze_layer.data_validator, DataValidator)
    
    @patch('bronze_layer.DeltaStorageManager')
    @patch('bronze_layer.SP500Collector')
    @patch('bronze_layer.PriceDataCollector')
    @patch('bronze_layer.DividendDataCollector')
    @patch('bronze_layer.DataValidator')
    def test_run_daily_collection(self, mock_validator, mock_dividend_collector, 
                                 mock_price_collector, mock_sp500_collector, mock_storage):
        """일일 수집 실행 테스트"""
        # 모킹 설정
        mock_storage_instance = Mock()
        mock_storage.return_value = mock_storage_instance
        
        mock_sp500_instance = Mock()
        mock_sp500_collector.return_value = mock_sp500_instance
        mock_sp500_instance.get_sp500_from_wikipedia.return_value = pd.DataFrame({'Symbol': ['AAPL']})
        mock_sp500_instance.normalize_symbols.return_value = pd.DataFrame({'Symbol': ['AAPL']})
        
        mock_price_instance = Mock()
        mock_price_collector.return_value = mock_price_instance
        mock_price_instance.get_daily_data_for_tickers.return_value = ([pd.DataFrame()], ['AAPL'], [])
        
        mock_dividend_instance = Mock()
        mock_dividend_collector.return_value = mock_dividend_instance
        mock_dividend_instance.fetch_dividend_events_for_tickers.return_value = pd.DataFrame()
        
        mock_validator_instance = Mock()
        mock_validator.return_value = mock_validator_instance
        
        bronze_layer = BronzeLayer("test-bucket")
        
        bronze_layer.run_daily_collection(date(2024, 1, 15))
        
        # 메서드 호출 확인
        mock_sp500_instance.get_sp500_from_wikipedia.assert_called_once()
        mock_price_instance.get_daily_data_for_tickers.assert_called_once()
        mock_dividend_instance.fetch_dividend_events_for_tickers.assert_called_once()

# Fixtures
@pytest.fixture
def sample_sp500_data():
    """S&P 500 테스트 데이터"""
    return pd.DataFrame({
        'Symbol': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
        'Security': ['Apple Inc.', 'Microsoft Corporation', 'Alphabet Inc.', 'Amazon.com Inc.', 'Tesla Inc.'],
        'GICS Sector': ['Technology', 'Technology', 'Technology', 'Consumer Discretionary', 'Consumer Discretionary']
    })

@pytest.fixture
def sample_date():
    """테스트 날짜"""
    return date(2024, 1, 15)
