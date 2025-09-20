"""
Finance Reader 패키지를 사용한 S&P 500 데이터 테스트 스크립트
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time

def get_sp500_tickers():
    """S&P 500 종목 리스트를 가져옵니다."""
    try:
        # Wikipedia에서 S&P 500 종목 리스트 가져오기
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = pd.read_html(url)
        sp500_table = tables[0]
        
        # 종목 코드와 회사명 추출
        tickers = sp500_table['Symbol'].tolist()
        companies = sp500_table['Security'].tolist()
        
        print(f"S&P 500 종목 수: {len(tickers)}")
        return tickers, companies
        
    except Exception as e:
        print(f"S&P 500 종목 리스트 가져오기 실패: {e}")
        return [], []

def get_stock_data(ticker, period="1y"):
    """개별 종목의 데이터를 가져옵니다."""
    try:
        stock = yf.Ticker(ticker)
        
        # 기본 정보
        info = stock.info
        
        # 가격 데이터
        hist = stock.history(period=period)
        
        # 배당 정보
        dividends = stock.dividends
        
        # 분할 정보
        splits = stock.splits
        
        return {
            'ticker': ticker,
            'info': info,
            'price_data': hist,
            'dividends': dividends,
            'splits': splits
        }
        
    except Exception as e:
        print(f"종목 {ticker} 데이터 가져오기 실패: {e}")
        return None

def get_sp500_summary_data():
    """S&P 500 전체 지수 데이터를 가져옵니다."""
    try:
        # S&P 500 ETF (SPY) 데이터로 대체
        spy = yf.Ticker("^GSPC")  # S&P 500 지수
        hist = spy.history(period="1y")
        
        return {
            'index': 'S&P 500',
            'ticker': '^GSPC',
            'price_data': hist
        }
        
    except Exception as e:
        print(f"S&P 500 지수 데이터 가져오기 실패: {e}")
        return None

def test_sample_stocks(tickers, sample_size=10):
    """샘플 종목들의 데이터를 테스트합니다."""
    print(f"\n=== {sample_size}개 샘플 종목 데이터 테스트 ===")
    
    sample_tickers = tickers[:sample_size]
    results = []
    
    for i, ticker in enumerate(sample_tickers):
        print(f"처리 중: {ticker} ({i+1}/{sample_size})")
        
        stock_data = get_stock_data(ticker, period="3mo")
        if stock_data:
            results.append(stock_data)
            
            # 기본 정보 출력
            info = stock_data['info']
            print(f"  - 회사명: {info.get('longName', 'N/A')}")
            print(f"  - 섹터: {info.get('sector', 'N/A')}")
            print(f"  - 시가총액: {info.get('marketCap', 'N/A')}")
            print(f"  - 배당수익률: {info.get('dividendYield', 'N/A')}")
            print(f"  - 가격 데이터 포인트: {len(stock_data['price_data'])}")
            print(f"  - 배당 데이터 포인트: {len(stock_data['dividends'])}")
            print()
        
        # API 제한을 고려한 딜레이
        time.sleep(0.1)
    
    return results

def analyze_dividend_stocks(stock_data_list):
    """배당주 분석을 수행합니다."""
    print("\n=== 배당주 분석 ===")
    
    dividend_stocks = []
    
    for stock_data in stock_data_list:
        info = stock_data['info']
        dividend_yield = info.get('dividendYield')
        
        if dividend_yield and dividend_yield > 0:
            dividend_stocks.append({
                'ticker': stock_data['ticker'],
                'name': info.get('longName', 'N/A'),
                'dividend_yield': dividend_yield,
                'sector': info.get('sector', 'N/A'),
                'market_cap': info.get('marketCap', 0)
            })
    
    # 배당수익률 순으로 정렬
    dividend_stocks.sort(key=lambda x: x['dividend_yield'], reverse=True)
    
    print(f"배당주 종목 수: {len(dividend_stocks)}")
    print("\n상위 10개 배당주:")
    for i, stock in enumerate(dividend_stocks[:10]):
        print(f"{i+1:2d}. {stock['ticker']:6s} - {stock['name'][:30]:30s} "
              f"배당수익률: {stock['dividend_yield']:.2%} "
              f"섹터: {stock['sector']}")
    
    return dividend_stocks

def main():
    """메인 실행 함수"""
    print("=== Finance Reader S&P 500 데이터 테스트 ===\n")
    
    # 1. S&P 500 종목 리스트 가져오기
    print("1. S&P 500 종목 리스트 가져오기...")
    tickers, companies = get_sp500_tickers()
    
    if not tickers:
        print("S&P 500 종목 리스트를 가져올 수 없습니다.")
        return
    
    # 2. S&P 500 지수 데이터 가져오기
    print("\n2. S&P 500 지수 데이터 가져오기...")
    sp500_data = get_sp500_summary_data()
    if sp500_data:
        print(f"S&P 500 지수 데이터 포인트: {len(sp500_data['price_data'])}")
    
    # 3. 샘플 종목 데이터 테스트
    print("\n3. 샘플 종목 데이터 테스트...")
    sample_data = test_sample_stocks(tickers, sample_size=20)
    
    # 4. 배당주 분석
    print("\n4. 배당주 분석...")
    dividend_stocks = analyze_dividend_stocks(sample_data)
    
    # 5. 데이터 요약
    print(f"\n=== 데이터 수집 요약 ===")
    print(f"전체 S&P 500 종목 수: {len(tickers)}")
    print(f"테스트한 종목 수: {len(sample_data)}")
    print(f"배당주 종목 수: {len(dividend_stocks)}")
    
    # 6. 데이터 저장 (선택사항)
    if sample_data:
        print(f"\n5. 데이터 저장...")
        
        # 가격 데이터 저장
        price_data_list = []
        for stock_data in sample_data:
            price_df = stock_data['price_data'].copy()
            price_df['ticker'] = stock_data['ticker']
            price_data_list.append(price_df)
        
        combined_price_data = pd.concat(price_data_list, ignore_index=True)
        combined_price_data.to_csv('sp500_sample_price_data.csv', index=True)
        print(f"가격 데이터 저장 완료: sp500_sample_price_data.csv")
        
        # 배당주 정보 저장
        if dividend_stocks:
            dividend_df = pd.DataFrame(dividend_stocks)
            dividend_df.to_csv('sp500_dividend_stocks.csv', index=False)
            print(f"배당주 정보 저장 완료: sp500_dividend_stocks.csv")

if __name__ == "__main__":
    main() 