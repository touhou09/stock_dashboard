"""
pytest 설정 및 공통 픽스처 정의
"""

import pytest
import pandas as pd
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch
import tempfile
import os

@pytest.fixture
def sample_sp500_data():
    """S&P 500 샘플 데이터"""
    return pd.DataFrame({
        'Symbol': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
        'Security': ['Apple Inc.', 'Microsoft Corporation', 'Alphabet Inc.', 'Amazon.com Inc.', 'Tesla Inc.'],
        'GICS Sector': ['Technology', 'Technology', 'Technology', 'Consumer Discretionary', 'Consumer Discretionary'],
        'GICS Sub Industry': ['Technology Hardware', 'Systems Software', 'Interactive Media', 'Internet Retail', 'Auto Manufacturers']
    })

@pytest.fixture
def sample_price_data():
    """샘플 주가 데이터"""
    return pd.DataFrame({
        'ticker': ['AAPL', 'MSFT', 'GOOGL'],
        'date': [date(2024, 1, 15), date(2024, 1, 15), date(2024, 1, 15)],
        'Open': [185.0, 380.0, 140.0],
        'High': [190.0, 385.0, 145.0],
        'Low': [180.0, 375.0, 135.0],
        'Close': [188.0, 382.0, 142.0],
        'Volume': [50000000, 30000000, 25000000],
        'ingestion_timestamp': [datetime.now()] * 3
    })

@pytest.fixture
def sample_dividend_data():
    """샘플 배당 데이터"""
    return pd.DataFrame({
        'ticker': ['AAPL', 'MSFT', 'GOOGL'],
        'company_name': ['Apple Inc.', 'Microsoft Corporation', 'Alphabet Inc.'],
        'sector': ['Technology', 'Technology', 'Technology'],
        'has_dividend': [True, True, False],
        'dividend_yield': [0.0044, 0.0072, 0.0],
        'dividend_yield_percent': [0.44, 0.72, 0.0],
        'dividend_rate': [0.96, 3.0, 0.0],
        'ex_dividend_date': [None, None, None],
        'payment_date': [None, None, None],
        'dividend_frequency': [4, 4, None],
        'market_cap': [3000000000000, 2800000000000, 1800000000000],
        'last_price': [188.0, 382.0, 142.0],
        'ingestion_timestamp': [datetime.now()] * 3
    })

@pytest.fixture
def mock_gcs_bucket():
    """GCS 버킷 모킹"""
    with patch('google.cloud.storage.Client') as mock_client:
        mock_bucket = Mock()
        mock_client.return_value.bucket.return_value = mock_bucket
        yield mock_bucket

@pytest.fixture
def temp_delta_path():
    """임시 Delta Table 경로"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield f"file://{temp_dir}"

@pytest.fixture
def mock_yfinance_ticker():
    """yfinance Ticker 모킹"""
    with patch('yfinance.Ticker') as mock_ticker_class:
        mock_ticker = Mock()
        mock_ticker_class.return_value = mock_ticker
        
        # 기본 정보 설정
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
        
        # 히스토리 데이터 설정
        mock_hist = pd.DataFrame({
            'Open': [185.0],
            'High': [190.0],
            'Low': [180.0],
            'Close': [188.0],
            'Volume': [50000000]
        }, index=[date(2024, 1, 15)])
        mock_ticker.history.return_value = mock_hist
        
        yield mock_ticker

@pytest.fixture
def mock_requests_get():
    """requests.get 모킹"""
    with patch('requests.get') as mock_get:
        # Wikipedia 응답 모킹
        mock_response = Mock()
        mock_response.text = """
        <html>
        <body>
        <table>
        <tr><th>Symbol</th><th>Security</th><th>GICS Sector</th></tr>
        <tr><td>AAPL</td><td>Apple Inc.</td><td>Technology</td></tr>
        <tr><td>MSFT</td><td>Microsoft Corporation</td><td>Technology</td></tr>
        </table>
        </body>
        </html>
        """
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        yield mock_get

@pytest.fixture
def sample_date():
    """테스트용 날짜"""
    return date(2024, 1, 15)

@pytest.fixture
def mock_delta_table():
    """Delta Table 모킹"""
    with patch('deltalake.DeltaTable') as mock_delta_class:
        mock_table = Mock()
        mock_delta_class.return_value = mock_table
        yield mock_table

@pytest.fixture
def mock_write_deltalake():
    """write_deltalake 모킹"""
    with patch('deltalake.write_deltalake') as mock_write:
        yield mock_write 