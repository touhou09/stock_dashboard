"""
Wikipedia 기반 S&P 500 데이터 수집 및 CSV 저장 테스트 스크립트
전체 S&P 500 하루치 데이터 수집
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import requests
from io import StringIO

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

def to_yahoo_symbol(sym: str) -> str:
    """클래스 주식 표기: BRK.B -> BRK-B"""
    return sym.strip().upper().replace(".", "-")

def get_sp500_from_wikipedia(max_retries: int = 3, timeout: int = 15) -> pd.DataFrame:
    """Wikipedia에서 S&P500 구성종목 테이블 파싱 (403 오류 우회)"""
    headers_pool = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/121.0",
    ]
    last_err = None

    for i in range(max_retries):
        try:
            print(f"Wikipedia 접근 시도 {i+1}/{max_retries}...")
            headers = {
                "User-Agent": random.choice(headers_pool),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.7",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
            resp = requests.get(WIKI_URL, headers=headers, timeout=timeout)
            resp.raise_for_status()
            
            tables = pd.read_html(StringIO(resp.text))
            spx = tables[0]
            
            if "Symbol" not in spx.columns:
                candidates = [c for c in spx.columns if "symbol" in c.lower() or "ticker" in c.lower()]
                if candidates:
                    spx = spx.rename(columns={candidates[0]: "Symbol"})
                else:
                    raise ValueError("Symbol 컬럼을 찾지 못했습니다.")
            
            print(f"✅ Wikipedia에서 S&P 500 데이터 수집 성공: {len(spx)}개 종목")
            return spx
            
        except Exception as e:
            last_err = e
            print(f"❌ Wikipedia 접근 실패 (시도 {i+1}): {e}")
            if i < max_retries - 1:
                wait_time = 1.5 * (i + 1)
                print(f"⏳ {wait_time}초 후 재시도...")
                time.sleep(wait_time)
    
    raise RuntimeError(f"Wikipedia 파싱 최종 실패: {last_err}")

def normalize_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """Yahoo 형식으로 심볼 정규화"""
    df = df.copy()
    df["Symbol"] = df["Symbol"].astype(str).map(to_yahoo_symbol)
    return df

def get_daily_data_for_all_tickers(tickers, target_date=None):
    """전체 S&P 500의 하루치 데이터를 조회합니다."""
    if target_date is None:
        target_date = datetime.now().date() - timedelta(days=1)  # 어제 날짜
    
    print(f"📅 {target_date} 하루치 데이터 수집 시작...")
    print(f" 전체 종목 수: {len(tickers)}개")
    
    all_daily_data = []
    successful_tickers = []
    failed_tickers = []
    
    for i, ticker in enumerate(tickers):
        print(f"  처리 중: {ticker} ({i+1}/{len(tickers)})")
        
        try:
            stock = yf.Ticker(ticker)
            
            # 하루치 데이터 가져오기
            start_date = target_date
            end_date = target_date + timedelta(days=1)
            
            hist = stock.history(start=start_date, end=end_date)
            
            if not hist.empty and hist['Close'].notna().any():
                # 데이터 처리
                hist['ticker'] = ticker
                hist['date'] = hist.index
                hist = hist.reset_index(drop=True)
                all_daily_data.append(hist)
                successful_tickers.append(ticker)
                
                print(f"    ✅ {ticker}: ${hist['Close'].iloc[-1]:.2f}")
            else:
                failed_tickers.append(ticker)
                print(f"    ❌ {ticker}: 데이터 없음")
                
        except Exception as e:
            failed_tickers.append(ticker)
            print(f"    ❌ {ticker}: {e}")
        
        # API 제한 고려한 딜레이
        time.sleep(0.5)  # 0.5초 딜레이
        
        # 진행 상황 표시 (50개마다)
        if (i + 1) % 50 == 0:
            print(f"    📊 진행률: {i+1}/{len(tickers)} ({((i+1)/len(tickers)*100):.1f}%)")
            print(f"    ✅ 성공: {len(successful_tickers)}개, ❌ 실패: {len(failed_tickers)}개")
    
    print(f"\n📈 최종 수집 결과:")
    print(f"  ✅ 성공: {len(successful_tickers)}개")
    print(f"  ❌ 실패: {len(failed_tickers)}개")
    print(f"  데이터 포인트: {sum(len(df) for df in all_daily_data)}개")
    
    return all_daily_data, successful_tickers, failed_tickers

def get_dividend_data_for_tickers(tickers, sample_size=200):
    """배당 데이터 수집 (샘플링)"""
    print(f"\n💰 배당 데이터 수집 (상위 {sample_size}개 종목)...")
    
    all_dividend_data = []
    dividend_stocks = []
    
    for i, ticker in enumerate(tickers[:sample_size]):
        print(f"  처리 중: {ticker} ({i+1}/{min(sample_size, len(tickers))})")
        
        try:
            stock = yf.Ticker(ticker)
            dividend_data = stock.dividends
            info = stock.info
            
            # 배당 데이터가 있는 경우
            if not dividend_data.empty:
                dividend_df = dividend_data.to_frame().reset_index()
                dividend_df.columns = ['date', 'dividend_amount']
                dividend_df['ticker'] = ticker
                all_dividend_data.append(dividend_df)
            
            # 배당주 정보 수집
            dividend_yield = info.get('dividendYield', 0)
            if dividend_yield and dividend_yield > 0:
                dividend_stocks.append({
                    'ticker': ticker,
                    'company_name': info.get('longName', 'N/A'),
                    'sector': info.get('sector', 'N/A'),
                    'dividend_yield': dividend_yield,
                    'market_cap': info.get('marketCap', 0)
                })
            
            print(f"    ✅ {info.get('longName', 'N/A')[:30]}")
            if dividend_yield:
                print(f"    💰 배당수익률: {dividend_yield:.2%}")
            
        except Exception as e:
            print(f"    ❌ {ticker}: {e}")
        
        time.sleep(0.3)  # API 제한 고려
        
        # 진행 상황 표시 (25개마다)
        if (i + 1) % 25 == 0:
            print(f"     배당 데이터 진행률: {i+1}/{min(sample_size, len(tickers))} ({((i+1)/min(sample_size, len(tickers))*100):.1f}%)")
    
    return all_dividend_data, dividend_stocks

def save_daily_data_to_csv(all_daily_data, all_dividend_data, dividend_stocks, target_date):
    """하루치 데이터를 CSV 파일로 저장"""
    date_str = target_date.strftime('%Y%m%d')
    
    print(f"\n💾 CSV 파일 저장 중...")
    
    # 1. 하루치 가격 데이터 저장
    if all_daily_data:
        daily_combined = pd.concat(all_daily_data, ignore_index=True)
        filename = f'sp500_daily_data_{date_str}.csv'
        daily_combined.to_csv(filename, index=False)
        print(f"✅ 하루치 가격 데이터: {filename} ({len(daily_combined)}행)")
    
    # 2. 배당 데이터 저장
    if all_dividend_data:
        dividend_combined = pd.concat(all_dividend_data, ignore_index=True)
        filename = f'sp500_dividend_data_{date_str}.csv'
        dividend_combined.to_csv(filename, index=False)
        print(f"✅ 배당 데이터: {filename} ({len(dividend_combined)}행)")
    
    # 3. 배당주 정보 저장
    if dividend_stocks:
        dividend_stocks_df = pd.DataFrame(dividend_stocks)
        dividend_stocks_df = dividend_stocks_df.sort_values('dividend_yield', ascending=False)
        filename = f'sp500_dividend_stocks_{date_str}.csv'
        dividend_stocks_df.to_csv(filename, index=False)
        print(f"✅ 배당주 정보: {filename} ({len(dividend_stocks_df)}행)")

def test_sp500_full_collection():
    """S&P 500 전체 데이터 수집 테스트"""
    print("=" * 80)
    print(" S&P 500 전체 데이터 수집 테스트 (Bronze Layer)")
    print("=" * 80)
    
    # 1. Wikipedia에서 S&P 500 종목 리스트 가져오기
    print("\n1️⃣ S&P 500 종목 리스트 수집...")
    try:
        spx_raw = get_sp500_from_wikipedia()
        spx = normalize_symbols(spx_raw)
        tickers = spx["Symbol"].dropna().unique().tolist()
        print(f"✅ 수집 완료: {len(tickers)}개 종목")
        print(f"📋 상위 10개 종목: {tickers[:10]}")
    except Exception as e:
        print(f"❌ 종목 리스트 수집 실패: {e}")
        return
    
    # 2. 전체 하루치 데이터 수집
    print(f"\n2️⃣ 전체 S&P 500 하루치 데이터 수집...")
    target_date = datetime.now().date() - timedelta(days=1)  # 어제 날짜
    all_daily_data, successful_tickers, failed_tickers = get_daily_data_for_all_tickers(tickers, target_date)
    
    if not all_daily_data:
        print("❌ 하루치 데이터 수집에 실패했습니다.")
        return
    
    # 3. 배당 데이터 수집 (샘플링)
    print(f"\n3️⃣ 배당 데이터 수집...")
    all_dividend_data, dividend_stocks = get_dividend_data_for_tickers(successful_tickers, sample_size=200)
    
    # 4. 데이터 저장
    print(f"\n4️⃣ 데이터 저장...")
    save_daily_data_to_csv(all_daily_data, all_dividend_data, dividend_stocks, target_date)
    
    # 5. 요약
    print("\n" + "=" * 80)
    print("📈 테스트 결과 요약")
    print("=" * 80)
    print(f"📅 수집 날짜: {target_date}")
    print(f"📊 전체 종목 수: {len(tickers)}개")
    print(f"✅ 성공한 종목: {len(successful_tickers)}개")
    print(f"❌ 실패한 종목: {len(failed_tickers)}개")
    print(f"💰 배당주 종목: {len(dividend_stocks)}개")
    print(f" 저장된 파일: 3개")
    print("  - sp500_daily_data_YYYYMMDD.csv (하루치 가격 데이터)")
    print("  - sp500_dividend_data_YYYYMMDD.csv (배당 데이터)")
    print("  - sp500_dividend_stocks_YYYYMMDD.csv (배당주 정보)")
    print("=" * 80)

if __name__ == "__main__":
    test_sp500_full_collection() 