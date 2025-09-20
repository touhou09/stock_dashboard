"""
간단한 Finance Reader 테스트
"""

import yfinance as yf
import pandas as pd

def simple_test():
    """간단한 데이터 테스트"""
    print("=== 간단한 Finance Reader 테스트 ===\n")
    
    # 1. S&P 500 지수 데이터
    print("1. S&P 500 지수 데이터 가져오기...")
    spy = yf.Ticker("^GSPC")
    spy_hist = spy.history(period="5d")
    print(f"S&P 500 최근 5일 데이터:")
    print(spy_hist.tail())
    print()
    
    # 2. 개별 종목 테스트 (Apple, Microsoft, Tesla)
    test_tickers = ['AAPL', 'MSFT', 'TSLA']
    
    for ticker in test_tickers:
        print(f"2. {ticker} 데이터 가져오기...")
        stock = yf.Ticker(ticker)
        
        # 기본 정보
        info = stock.info
        print(f"   회사명: {info.get('longName', 'N/A')}")
        print(f"   섹터: {info.get('sector', 'N/A')}")
        print(f"   시가총액: {info.get('marketCap', 'N/A')}")
        print(f"   배당수익률: {info.get('dividendYield', 'N/A')}")
        
        # 가격 데이터
        hist = stock.history(period="5d")
        print(f"   최근 5일 가격 데이터:")
        print(f"   {hist.tail()}")
        print()

if __name__ == "__main__":
    simple_test()