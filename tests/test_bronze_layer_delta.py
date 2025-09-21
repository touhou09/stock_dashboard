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
        
        result = silver_layer.create_unified_table(price_df, dividend_df)
        
        assert len(result) == 1
        assert result['is_dividend_stock'].sum() == 0
        assert result[result['ticker'] == 'GOOGL']['is_dividend_stock'].iloc[0] == False

"""
Bronze Layer Delta Lake 모듈 테스트
"""

import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bronze_layer_delta import BronzeLayerDelta

class TestBronzeLayerDelta:
    """BronzeLayerDelta 클래스 테스트"""
    
    def test_init(self, mock_gcs_bucket):
        """초기화 테스트"""
        # 수정: GCS 클라이언트 모킹을 위한 패치 추가
        with patch('bronze_layer_delta.storage.Client'):
            bronze_layer = BronzeLayerDelta(
                gcs_bucket="test-bucket",
                gcs_path="test/path"
            )
            
            assert bronze_layer.gcs_bucket == "test-bucket"
            assert bronze_layer.gcs_path == "test/path"
            assert bronze_layer.price_table_path == "gs://test-bucket/test/path/sp500_daily_prices"
            assert bronze_layer.dividend_table_path == "gs://test-bucket/test/path/sp500_dividend_info"
    
    def test_to_yahoo_symbol(self, mock_gcs_bucket):
        """심볼 변환 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            bronze_layer = BronzeLayerDelta("test-bucket")
            
            # 수정: 다양한 심볼 형식 테스트
            assert bronze_layer.to_yahoo_symbol("BRK.B") == "BRK-B"
            assert bronze_layer.to_yahoo_symbol("BRK.A") == "BRK-A"
            assert bronze_layer.to_yahoo_symbol("AAPL") == "AAPL"
            assert bronze_layer.to_yahoo_symbol("  msft  ") == "MSFT"
            assert bronze_layer.to_yahoo_symbol("googl") == "GOOGL"
    
    @patch('bronze_layer_delta.requests.get')
    @patch('bronze_layer_delta.pd.read_html')
    def test_get_sp500_from_wikipedia_success(self, mock_read_html, mock_get, mock_gcs_bucket, sample_sp500_data):
        """Wikipedia에서 S&P 500 데이터 수집 성공 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            # 수정: 모킹 설정 개선
            mock_response = Mock()
            mock_response.text = "test html"
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            mock_read_html.return_value = [sample_sp500_data]
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            result = bronze_layer.get_sp500_from_wikipedia()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 5
            assert 'Symbol' in result.columns
            assert 'AAPL' in result['Symbol'].values
    
    @patch('bronze_layer_delta.requests.get')
    def test_get_sp500_from_wikipedia_failure(self, mock_get, mock_gcs_bucket):
        """Wikipedia에서 S&P 500 데이터 수집 실패 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            # 수정: 실패 시나리오 모킹
            mock_get.side_effect = Exception("Network error")
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            
            with pytest.raises(RuntimeError, match="Wikipedia 파싱 최종 실패"):
                bronze_layer.get_sp500_from_wikipedia(max_retries=1)
    
    def test_normalize_symbols(self, mock_gcs_bucket, sample_sp500_data):
        """심볼 정규화 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            bronze_layer = BronzeLayerDelta("test-bucket")
            
            # 수정: 테스트 데이터에 다양한 심볼 형식 추가
            test_data = sample_sp500_data.copy()
            test_data.loc[0, 'Symbol'] = 'BRK.B'
            test_data.loc[1, 'Symbol'] = 'BRK.A'
            
            result = bronze_layer.normalize_symbols(test_data)
            
            assert result['Symbol'].iloc[0] == 'BRK-B'
            assert result['Symbol'].iloc[1] == 'BRK-A'
            assert result['Symbol'].iloc[2] == 'GOOGL'
    
    @patch('bronze_layer_delta.yf.Ticker')
    def test_get_daily_data_for_tickers_success(self, mock_ticker_class, mock_gcs_bucket, sample_date):
        """일일 데이터 수집 성공 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            # 수정: 모킹 개선
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
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            tickers = ['AAPL', 'MSFT']
            
            with patch('time.sleep'):  # 수정: sleep 모킹 추가
                all_data, successful, failed = bronze_layer.get_daily_data_for_tickers(tickers, sample_date)
            
            assert len(all_data) == 2
            assert len(successful) == 2
            assert len(failed) == 0
            assert all_data[0]['ticker'].iloc[0] == 'AAPL'
            assert all_data[0]['Close'].iloc[0] == 188.0
    
    @patch('bronze_layer_delta.yf.Ticker')
    def test_get_daily_data_for_tickers_failure(self, mock_ticker_class, mock_gcs_bucket, sample_date):
        """일일 데이터 수집 실패 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            # 수정: 실패 시나리오 모킹
            mock_ticker = Mock()
            mock_ticker_class.return_value = mock_ticker
            mock_ticker.history.side_effect = Exception("API Error")
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            tickers = ['INVALID']
            
            with patch('time.sleep'):  # 수정: sleep 모킹 추가
                all_data, successful, failed = bronze_layer.get_daily_data_for_tickers(tickers, sample_date)
            
            assert len(all_data) == 0
            assert len(successful) == 0
            assert len(failed) == 1
            assert failed[0] == 'INVALID'
    
    @patch('bronze_layer_delta.yf.Ticker')
    def test_get_dividend_info_for_tickers(self, mock_ticker_class, mock_gcs_bucket):
        """배당 정보 수집 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            # 수정: 배당 정보 모킹 개선
            mock_ticker = Mock()
            mock_ticker_class.return_value = mock_ticker
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
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            tickers = ['AAPL']
            
            with patch('time.sleep'):  # 수정: sleep 모킹 추가
                result = bronze_layer.get_dividend_info_for_tickers(tickers)
            
            assert len(result) == 1
            assert result[0]['ticker'] == 'AAPL'
            assert result[0]['company_name'] == 'Apple Inc.'
            assert result[0]['has_dividend'] == True
            assert result[0]['dividend_yield'] == 0.0044
    
    @patch('bronze_layer_delta.write_deltalake')
    @patch('bronze_layer_delta.DeltaTable')
    def test_save_price_data_to_delta_new_table(self, mock_delta_table, mock_write_deltalake, mock_gcs_bucket, sample_price_data, sample_date):
        """가격 데이터 Delta Table 저장 테스트 (새 테이블)"""
        with patch('bronze_layer_delta.storage.Client'):
            # 수정: Delta Table 존재하지 않는 경우 모킹
            mock_delta_table.side_effect = Exception("Table not found")
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            all_data = [sample_price_data]
            
            bronze_layer.save_price_data_to_delta(all_data, sample_date)
            
            # 수정: write_deltalake 호출 확인
            mock_write_deltalake.assert_called_once()
            call_args = mock_write_deltalake.call_args
            assert call_args[0][0] == bronze_layer.price_table_path
            assert call_args[1]['mode'] == 'overwrite'
            assert call_args[1]['partition_by'] == ['date']
    
    @patch('bronze_layer_delta.write_deltalake')
    @patch('bronze_layer_delta.DeltaTable')
    def test_save_dividend_data_to_delta_existing_table(self, mock_delta_table, mock_write_deltalake, mock_gcs_bucket, sample_dividend_data, sample_date):
        """배당 데이터 Delta Table 저장 테스트 (기존 테이블)"""
        with patch('bronze_layer_delta.storage.Client'):
            # 수정: Delta Table 존재하는 경우 모킹
            mock_table = Mock()
            mock_delta_table.return_value = mock_table
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            dividend_info = sample_dividend_data.to_dict('records')
            
            bronze_layer.save_dividend_data_to_delta(dividend_info, sample_date)
            
            # 수정: write_deltalake 호출 확인
            mock_write_deltalake.assert_called_once()
            call_args = mock_write_deltalake.call_args
            assert call_args[0][0] == bronze_layer.dividend_table_path
            assert call_args[1]['mode'] == 'append'
            assert call_args[1]['partition_by'] == ['has_dividend']
    
    @patch('bronze_layer_delta.BronzeLayerDelta.save_dividend_data_to_delta')
    @patch('bronze_layer_delta.BronzeLayerDelta.save_price_data_to_delta')
    @patch('bronze_layer_delta.BronzeLayerDelta.get_dividend_info_for_tickers')
    @patch('bronze_layer_delta.BronzeLayerDelta.get_daily_data_for_tickers')
    @patch('bronze_layer_delta.BronzeLayerDelta.get_sp500_from_wikipedia')
    def test_run_daily_collection_success(self, mock_get_sp500, mock_get_daily, mock_get_dividend, 
                                        mock_save_price, mock_save_dividend, mock_gcs_bucket, sample_sp500_data, sample_date):
        """일일 수집 실행 성공 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            # 수정: 모킹 설정 개선
            mock_get_sp500.return_value = sample_sp500_data
            mock_get_daily.return_value = ([sample_sp500_data], ['AAPL', 'MSFT'], [])
            mock_get_dividend.return_value = [{'ticker': 'AAPL', 'has_dividend': True}]
            
            bronze_layer = BronzeLayerDelta("test-bucket")
            
            bronze_layer.run_daily_collection(sample_date)
            
            # 수정: 메서드 호출 확인
            mock_get_sp500.assert_called_once()
            mock_get_daily.assert_called_once()
            mock_get_dividend.assert_called_once()
            mock_save_price.assert_called_once()
            mock_save_dividend.assert_called_once()
    
    def test_run_daily_collection_with_default_date(self, mock_gcs_bucket):
        """기본 날짜로 일일 수집 실행 테스트"""
        with patch('bronze_layer_delta.storage.Client'):
            with patch('bronze_layer_delta.datetime') as mock_datetime:
                # 수정: 날짜 모킹 개선
                mock_datetime.now.return_value.date.return_value = date(2024, 1, 16)
                mock_datetime.now.return_value = datetime(2024, 1, 16)
                
                bronze_layer = BronzeLayerDelta("test-bucket")
                
                with patch.object(bronze_layer, 'get_sp500_from_wikipedia') as mock_get_sp500:
                    mock_get_sp500.side_effect = Exception("Test error")
                    
                    with pytest.raises(Exception):
                        bronze_layer.run_daily_collection() 