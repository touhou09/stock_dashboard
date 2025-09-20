"""
Silver Layer 간단 테스트 스크립트
기존 Bronze Layer 데이터를 활용한 빠른 테스트
"""

import pandas as pd
from datetime import datetime, timedelta
import os

def test_silver_layer_simple():
    """Silver Layer 간단 테스트 실행"""
    print("=" * 60)
    print(" Silver Layer 간단 테스트")
    print("=" * 60)
    
    # 날짜 설정
    target_date = datetime.now().date() - timedelta(days=1)
    date_str = target_date.strftime('%Y%m%d')
    
    try:
        # 1. Bronze Layer 데이터 로드
        print(f"\n1️⃣ Bronze Layer 데이터 로드...")
        price_data = pd.read_csv(f'sp500_daily_data_{date_str}.csv')
        dividend_data = pd.read_csv(f'sp500_dividend_stocks_{date_str}.csv')
        
        print(f"✅ 가격 데이터: {len(price_data)}행")
        print(f"✅ 배당주 데이터: {len(dividend_data)}행")
        
        # 2. Silver Layer 통합 테이블 생성
        print(f"\n2️⃣ Silver Layer 통합 테이블 생성...")
        
        # 가격 데이터에 배당주 정보 병합
        merged_data = price_data.merge(
            dividend_data[['ticker', 'company_name', 'sector', 'dividend_yield']], 
            on='ticker', 
            how='left'
        )
        
        # 배당주 여부 플래그 추가
        merged_data['has_dividend'] = merged_data['dividend_yield'].fillna(0) > 0
        merged_data['dividend_yield_percent'] = merged_data['dividend_yield'].fillna(0)
        
        # 날짜 컬럼 정리
        merged_data['date'] = pd.to_datetime(merged_data['date']).dt.date
        
        # 핵심 컬럼만 선택
        silver_data = merged_data[[
            'ticker',           # 종목코드
            'company_name',     # 회사명
            'date',             # 날짜
            'Open',             # 시가
            'High',             # 고가
            'Low',              # 저가
            'Close',            # 종가
            'Volume',           # 거래량
            'sector',           # 섹터 (분석용)
            'dividend_yield_percent',  # 배당률
            'has_dividend'      # 배당여부
        ]].copy()
        
        # 3. 데이터 품질 검증
        print(f"\n3️⃣ 데이터 품질 검증:")
        print(f"  전체 레코드 수: {len(silver_data)}")
        print(f"  배당주 레코드 수: {silver_data['has_dividend'].sum()}")
        print(f"  배당주 비율: {(silver_data['has_dividend'].sum() / len(silver_data) * 100):.1f}%")
        
        # 4. 배당주 분석
        print(f"\n4️⃣ 배당주 분석:")
        dividend_stocks = silver_data[silver_data['has_dividend'] == True]
        
        if not dividend_stocks.empty:
            # 섹터별 배당주 분포
            sector_dist = dividend_stocks.groupby('sector').size().sort_values(ascending=False)
            print(f"\n🏢 섹터별 배당주 분포 (상위 5개):")
            for sector, count in sector_dist.head(5).items():
                print(f"  {sector}: {count}개")
            
            # 배당수익률 상위 5개
            top_dividend = dividend_stocks.nlargest(5, 'dividend_yield_percent')[['ticker', 'company_name', 'dividend_yield_percent', 'sector']]
            print(f"\n💰 배당수익률 상위 5개:")
            for _, row in top_dividend.iterrows():
                print(f"  {row['ticker']} ({row['company_name'][:25]}): {row['dividend_yield_percent']:.2f}% - {row['sector']}")
            
            # 배당수익률 통계
            print(f"\n📊 배당수익률 통계:")
            print(f"  평균: {dividend_stocks['dividend_yield_percent'].mean():.2f}%")
            print(f"  중간값: {dividend_stocks['dividend_yield_percent'].median():.2f}%")
            print(f"  최대값: {dividend_stocks['dividend_yield_percent'].max():.2f}%")
            print(f"  최소값: {dividend_stocks['dividend_yield_percent'].min():.2f}%")
        
        # 5. 데이터 저장
        print(f"\n5️⃣ 데이터 저장...")
        
        # 전체 통합 테이블 저장
        unified_filename = f'silver_unified_stock_data_{date_str}.csv'
        silver_data.to_csv(unified_filename, index=False)
        print(f"✅ 통합 테이블: {unified_filename} ({len(silver_data)}행)")
        
        # 배당주만 필터링한 테이블 저장
        if not dividend_stocks.empty:
            dividend_filename = f'silver_dividend_stocks_{date_str}.csv'
            dividend_stocks.to_csv(dividend_filename, index=False)
            print(f"✅ 배당주 테이블: {dividend_filename} ({len(dividend_stocks)}행)")
        
        # 6. 최종 요약
        print("\n" + "=" * 60)
        print("📈 Silver Layer 테스트 결과 요약")
        print("=" * 60)
        print(f"📅 처리 날짜: {target_date}")
        print(f"📊 전체 종목 수: {len(silver_data['ticker'].unique())}개")
        print(f"💰 배당주 종목 수: {silver_data['has_dividend'].sum()}개")
        print(f"📈 배당주 비율: {(silver_data['has_dividend'].sum() / len(silver_data) * 100):.1f}%")
        print(f"💾 저장된 파일:")
        print(f"  - {unified_filename} (통합 테이블)")
        if not dividend_stocks.empty:
            print(f"  - {dividend_filename} (배당주 테이블)")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Silver Layer 테스트 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_silver_layer_simple()
