"""
Silver Layer 테스트 스크립트
Bronze Layer에서 수집한 데이터를 기반으로 배당주 필터링 및 통합 테이블 생성
"""

import pandas as pd
import yfinance as yf
import time
from datetime import datetime, timedelta
import os

def load_bronze_data(date_str):
    """Bronze Layer에서 수집한 데이터 로드"""
    print(f"📂 Bronze Layer 데이터 로드 중...")
    
    # 가격 데이터 로드
    price_file = f'sp500_daily_data_{date_str}.csv'
    if not os.path.exists(price_file):
        raise FileNotFoundError(f"가격 데이터 파일을 찾을 수 없습니다: {price_file}")
    
    price_data = pd.read_csv(price_file)
    print(f"✅ 가격 데이터 로드 완료: {len(price_data)}행")
    
    # 배당주 정보 로드 (있는 경우)
    dividend_file = f'sp500_dividend_stocks_{date_str}.csv'
    dividend_stocks = None
    if os.path.exists(dividend_file):
        dividend_stocks = pd.read_csv(dividend_file)
        print(f"✅ 배당주 정보 로드 완료: {len(dividend_stocks)}행")
    else:
        print("⚠️ 배당주 정보 파일이 없습니다. 새로 수집합니다.")
    
    return price_data, dividend_stocks

def get_dividend_info_for_tickers(tickers, sample_size=None):
    """전체 종목에 대한 배당 정보 수집"""
    if sample_size is None:
        sample_size = len(tickers)
    
    print(f"\n💰 배당 정보 수집 중... (상위 {min(sample_size, len(tickers))}개 종목)")
    
    dividend_info = []
    successful_count = 0
    
    for i, ticker in enumerate(tickers[:sample_size]):
        print(f"  처리 중: {ticker} ({i+1}/{min(sample_size, len(tickers))})")
        
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            # 기본 정보 수집
            dividend_yield = info.get('dividendYield', 0) or 0
            dividend_rate = info.get('dividendRate', 0) or 0
            ex_dividend_date = info.get('exDividendDate', None)
            payment_date = info.get('dividendDate', None)
            dividend_frequency = info.get('dividendFrequency', None)
            
            # 배당주 여부 판단
            has_dividend = dividend_yield > 0 or dividend_rate > 0
            
            dividend_info.append({
                'ticker': ticker,
                'company_name': info.get('longName', 'N/A'),
                'sector': info.get('sector', 'N/A'),
                'has_dividend': has_dividend,
                'dividend_yield': dividend_yield,
                # 수정: 이미 퍼센트로 저장된 데이터이므로 100을 곱하지 않음
                'dividend_yield_percent': dividend_yield * 100 if dividend_yield else 0,
                'dividend_rate': dividend_rate,
                'ex_dividend_date': ex_dividend_date,
                'payment_date': payment_date,
                'dividend_frequency': dividend_frequency,
                'market_cap': info.get('marketCap', 0),
                'last_price': info.get('currentPrice', 0)
            })
            
            successful_count += 1
            print(f"    ✅ {info.get('longName', 'N/A')[:30]}")
            if has_dividend:
                print(f"    💰 배당수익률: {dividend_yield:.2%}")
            
        except Exception as e:
            print(f"    ❌ {ticker}: {e}")
            # 실패한 경우에도 기본 정보는 추가
            dividend_info.append({
                'ticker': ticker,
                'company_name': 'N/A',
                'sector': 'N/A',
                'has_dividend': False,
                'dividend_yield': 0,
                'dividend_yield_percent': 0,
                'dividend_rate': 0,
                'ex_dividend_date': None,
                'payment_date': None,
                'dividend_frequency': None,
                'market_cap': 0,
                'last_price': 0
            })
        
        # API 제한 고려
        time.sleep(0.3)
        
        # 진행 상황 표시 (50개마다)
        if (i + 1) % 50 == 0:
            print(f"    📊 진행률: {i+1}/{min(sample_size, len(tickers))} ({((i+1)/min(sample_size, len(tickers))*100):.1f}%)")
            print(f"    ✅ 성공: {successful_count}개")
    
    return pd.DataFrame(dividend_info)

def create_silver_layer_table(price_data, dividend_info):
    """Silver Layer 통합 테이블 생성"""
    print(f"\n🏗️ Silver Layer 통합 테이블 생성 중...")
    
    # 가격 데이터에 배당 정보 병합
    merged_data = price_data.merge(
        dividend_info[['ticker', 'company_name', 'sector', 'has_dividend', 
                     'dividend_yield', 'dividend_yield_percent', 'dividend_rate',
                     'ex_dividend_date', 'payment_date', 'dividend_frequency']], 
        on='ticker', 
        how='left'
    )
    
    # 컬럼명 정리 및 추가 컬럼 생성
    silver_data = merged_data.copy()
    
    # 날짜 컬럼 정리
    if 'date' in silver_data.columns:
        silver_data['date'] = pd.to_datetime(silver_data['date']).dt.date
    
    # 배당주 필터링 (선택사항 - 모든 데이터를 유지하되 배당주 여부 표시)
    silver_data['is_dividend_stock'] = silver_data['has_dividend'].fillna(False)
    
    # 수정: 배당수익률 퍼센트 계산 (기존 데이터가 이미 퍼센트인 경우)
    if 'dividend_yield_percent' not in silver_data.columns:
        silver_data['dividend_yield_percent'] = silver_data['dividend_yield'].fillna(0) * 100
    else:
        # 이미 퍼센트로 저장된 경우 그대로 사용
        silver_data['dividend_yield_percent'] = silver_data['dividend_yield_percent'].fillna(0)
    
    # 데이터 품질 검증
    print(f"📊 데이터 품질 검증:")
    print(f"  전체 레코드 수: {len(silver_data)}")
    print(f"  배당주 레코드 수: {silver_data['is_dividend_stock'].sum()}")
    print(f"  배당주 비율: {(silver_data['is_dividend_stock'].sum() / len(silver_data) * 100):.1f}%")
    
    # 결측값 확인
    missing_data = silver_data.isnull().sum()
    print(f"  결측값 현황:")
    for col, missing_count in missing_data[missing_data > 0].items():
        print(f"    {col}: {missing_count}개 ({missing_count/len(silver_data)*100:.1f}%)")
    
    return silver_data

def save_silver_layer_data(silver_data, date_str):
    """Silver Layer 데이터 저장"""
    print(f"\n💾 Silver Layer 데이터 저장 중...")
    
    # 전체 통합 테이블 저장
    filename = f'silver_unified_stock_data_{date_str}.csv'
    silver_data.to_csv(filename, index=False)
    print(f"✅ 통합 테이블: {filename} ({len(silver_data)}행)")
    
    # 배당주만 필터링한 테이블 저장
    dividend_stocks = silver_data[silver_data['is_dividend_stock'] == True]
    if not dividend_stocks.empty:
        dividend_filename = f'silver_dividend_stocks_{date_str}.csv'
        dividend_stocks.to_csv(dividend_filename, index=False)
        print(f"✅ 배당주 테이블: {dividend_filename} ({len(dividend_stocks)}행)")
    
    # 배당주 통계 저장
    stats = {
        'total_stocks': len(silver_data['ticker'].unique()),
        'dividend_stocks': len(dividend_stocks['ticker'].unique()),
        'dividend_ratio': len(dividend_stocks['ticker'].unique()) / len(silver_data['ticker'].unique()) * 100,
        'avg_dividend_yield': dividend_stocks['dividend_yield'].mean() if not dividend_stocks.empty else 0,
        'max_dividend_yield': dividend_stocks['dividend_yield'].max() if not dividend_stocks.empty else 0,
        'data_date': date_str,
        'created_at': datetime.now().isoformat()
    }
    
    stats_df = pd.DataFrame([stats])
    stats_filename = f'silver_layer_stats_{date_str}.csv'
    stats_df.to_csv(stats_filename, index=False)
    print(f"✅ 통계 정보: {stats_filename}")
    
    return filename, dividend_filename if not dividend_stocks.empty else None, stats_filename

def analyze_dividend_stocks(silver_data):
    """배당주 분석"""
    print(f"\n📈 배당주 분석 결과:")
    
    dividend_stocks = silver_data[silver_data['is_dividend_stock'] == True]
    
    if dividend_stocks.empty:
        print("  배당주가 없습니다.")
        return
    
    # 섹터별 배당주 분포
    sector_dist = dividend_stocks.groupby('sector').size().sort_values(ascending=False)
    print(f"\n💰 섹터별 배당주 분포:")
    for sector, count in sector_dist.head(10).items():
        print(f"  {sector}: {count}개")
    
    # 배당수익률 상위 10개
    top_dividend = dividend_stocks.nlargest(10, 'dividend_yield')[['ticker', 'company_name', 'dividend_yield_percent', 'sector']]
    print(f"\n💰 배당수익률 상위 10개:")
    for _, row in top_dividend.iterrows():
        print(f"  {row['ticker']} ({row['company_name'][:30]}): {row['dividend_yield_percent']:.2f}% - {row['sector']}")
    
    # 배당수익률 통계
    print(f"\n📊 배당수익률 통계:")
    print(f"  평균: {dividend_stocks['dividend_yield_percent'].mean():.2f}%")
    print(f"  중간값: {dividend_stocks['dividend_yield_percent'].median():.2f}%")
    print(f"  최대값: {dividend_stocks['dividend_yield_percent'].max():.2f}%")
    print(f"  최소값: {dividend_stocks['dividend_yield_percent'].min():.2f}%")

def test_silver_layer():
    """Silver Layer 테스트 실행"""
    print("=" * 80)
    print(" Silver Layer 테스트 (배당주 필터링 및 통합 테이블 생성)")
    print("=" * 80)
    
    # 날짜 설정 (어제 날짜)
    target_date = datetime.now().date() - timedelta(days=1)
    date_str = target_date.strftime('%Y%m%d')
    
    try:
        # 1. Bronze Layer 데이터 로드
        print(f"\n1️⃣ Bronze Layer 데이터 로드...")
        price_data, existing_dividend_stocks = load_bronze_data(date_str)
        
        # 2. 배당 정보 수집 (기존 데이터가 없거나 부족한 경우)
        print(f"\n2️⃣ 배당 정보 수집...")
        if existing_dividend_stocks is None or len(existing_dividend_stocks) < len(price_data['ticker'].unique()):
            tickers = price_data['ticker'].unique().tolist()
            dividend_info = get_dividend_info_for_tickers(tickers)
        else:
            print("✅ 기존 배당 정보 사용")
            dividend_info = existing_dividend_stocks
        
        # 3. Silver Layer 통합 테이블 생성
        print(f"\n3️⃣ Silver Layer 통합 테이블 생성...")
        silver_data = create_silver_layer_table(price_data, dividend_info)
        
        # 4. 데이터 저장
        print(f"\n4️⃣ 데이터 저장...")
        unified_file, dividend_file, stats_file = save_silver_layer_data(silver_data, date_str)
        
        # 5. 배당주 분석
        print(f"\n5️⃣ 배당주 분석...")
        analyze_dividend_stocks(silver_data)
        
        # 6. 최종 요약
        print("\n" + "=" * 80)
        print("📈 Silver Layer 테스트 결과 요약")
        print("=" * 80)
        print(f"📅 처리 날짜: {target_date}")
        print(f"📊 전체 종목 수: {len(silver_data['ticker'].unique())}개")
        print(f"💰 배당주 종목 수: {silver_data['is_dividend_stock'].sum()}개")
        print(f"📈 배당주 비율: {(silver_data['is_dividend_stock'].sum() / len(silver_data) * 100):.1f}%")
        print(f"💾 저장된 파일:")
        print(f"  - {unified_file} (통합 테이블)")
        if dividend_file:
            print(f"  - {dividend_file} (배당주 테이블)")
        print(f"  - {stats_file} (통계 정보)")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Silver Layer 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_silver_layer()
