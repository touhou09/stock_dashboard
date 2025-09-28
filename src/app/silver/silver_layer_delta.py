"""
Silver Layer - Delta Lake 기반 계산/집계 지표 생성
Bronze Layer의 원천 데이터를 기반으로 배당 지표를 계산하여 저장
- TTM 배당수익률, 배당횟수, 최근 배당일 등 계산된 지표
"""

import pandas as pd
from datetime import datetime, timedelta, date, timezone
from typing import Optional, List, Tuple
import logging
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
from google.cloud import storage
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverLayerDelta:
    """Silver Layer Delta Lake 기반 관리 클래스 - 계산된 지표만 저장"""
    
    def __init__(self, gcs_bucket: str, bronze_path: str = "stock_dashboard/bronze", silver_path: str = "stock_dashboard/silver"):
        """
        Silver Layer 초기화
        
        Args:
            gcs_bucket: GCS 버킷 이름
            bronze_path: Bronze Layer 경로
            silver_path: Silver Layer 경로
        """
        self.gcs_bucket = gcs_bucket
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        
        # Bronze Layer Delta Table 경로 (권장 구조)
        self.bronze_price_path = f"gs://{gcs_bucket}/{bronze_path}/bronze_price_daily"
        self.bronze_dividend_events_path = f"gs://{gcs_bucket}/{bronze_path}/bronze_dividend_events"
        
        # Silver Layer Delta Table 경로 (권장 구조)
        self.silver_dividend_metrics_path = f"gs://{gcs_bucket}/{silver_path}/silver_dividend_metrics_daily"
    
    def load_bronze_price_data(self, target_date: date) -> pd.DataFrame:
        """Bronze Layer에서 가격 데이터 로드"""
        logger.info(f" Bronze Layer 가격 데이터 로드 중... (날짜: {target_date})")
        
        try:
            # Delta Table에서 데이터 로드
            price_delta = DeltaTable(self.bronze_price_path)
            price_df = price_delta.to_pandas()
            
            # 날짜 필터링
            price_df['date'] = pd.to_datetime(price_df['date']).dt.date
            price_df = price_df[price_df['date'] == target_date]
            
            logger.info(f"✅ 가격 데이터 로드 완료: {len(price_df)}행")
            return price_df
            
        except Exception as e:
            logger.error(f"❌ Bronze 가격 데이터 로드 실패: {e}")
            raise Exception(f"Bronze 가격 데이터 로드 실패: {e}") from e
    
    def load_bronze_dividend_events(self, target_date: date, lookback_days: int = 365) -> pd.DataFrame:
        """Bronze Layer에서 배당 이벤트 데이터 로드 (TTM 계산용)"""
        logger.info(f" Bronze Layer 배당 이벤트 데이터 로드 중... (TTM: {lookback_days}일)")
        
        try:
            # Delta Table에서 데이터 로드
            dividend_delta = DeltaTable(self.bronze_dividend_events_path)
            dividend_df = dividend_delta.to_pandas()
            
            # TTM 기간 계산
            start_date = target_date - timedelta(days=lookback_days)
            dividend_df['ex_date'] = pd.to_datetime(dividend_df['ex_date']).dt.date
            
            # TTM 기간 내 배당 이벤트만 필터링
            dividend_df = dividend_df[
                (dividend_df['ex_date'] >= start_date) & 
                (dividend_df['ex_date'] <= target_date)
            ]
            
            logger.info(f"✅ 배당 이벤트 데이터 로드 완료: {len(dividend_df)}행 (TTM 기간)")
            return dividend_df
            
        except Exception as e:
            logger.error(f"❌ Bronze 배당 이벤트 데이터 로드 실패: {e}")
            raise Exception(f"Bronze 배당 이벤트 데이터 로드 실패: {e}") from e
    
    def build_dividend_metrics_daily(self, price_df: pd.DataFrame, dividend_events_df: pd.DataFrame, target_date: date) -> pd.DataFrame:
        """
        배당 지표 계산 (Silver Layer 핵심 함수)
        
        Args:
            price_df: Bronze 가격 데이터
            dividend_events_df: Bronze 배당 이벤트 데이터 (TTM 기간)
            target_date: 계산 기준일
            
        Returns:
            pd.DataFrame: 계산된 배당 지표
        """
        logger.info(f"\n📊 배당 지표 계산 중... (기준일: {target_date})")
        
        metrics_list = []
        
        for _, price_row in price_df.iterrows():
            ticker = price_row['ticker']
            last_price = price_row['close']  # 기준일 종가
            
            # 해당 티커의 TTM 배당 이벤트 필터링
            ticker_dividends = dividend_events_df[
                dividend_events_df['ticker'] == ticker
            ].copy()
            
            if ticker_dividends.empty:
                # 배당 이벤트가 없는 경우
                metrics = {
                    'as_of_date': target_date,
                    'ticker': ticker,
                    'last_price': last_price,
                    'market_cap': 0,  # Bronze에 없으므로 0으로 설정
                    'dividend_ttm': 0.0,
                    'dividend_yield_ttm': 0.0,
                    'div_count_1y': 0,
                    'last_div_date': None,
                    'updated_at': datetime.now(timezone.utc)
                }
            else:
                # 배당 지표 계산
                dividend_ttm = ticker_dividends['amount'].sum()
                dividend_yield_ttm = (dividend_ttm / last_price) * 100 if last_price > 0 else 0.0
                div_count_1y = len(ticker_dividends)
                last_div_date = ticker_dividends['ex_date'].max()
                
                metrics = {
                    'as_of_date': target_date,
                    'ticker': ticker,
                    'last_price': last_price,
                    'market_cap': 0,  # Bronze에 없으므로 0으로 설정
                    'dividend_ttm': dividend_ttm,
                    'dividend_yield_ttm': dividend_yield_ttm,
                    'div_count_1y': div_count_1y,
                    'last_div_date': last_div_date,
                    'updated_at': datetime.now(timezone.utc)
                }
            
            metrics_list.append(metrics)
        
        # DataFrame으로 변환
        metrics_df = pd.DataFrame(metrics_list)
        
        # 배당주 필터링 (TTM 배당이 있는 종목)
        dividend_stocks = metrics_df[metrics_df['dividend_ttm'] > 0]
        
        logger.info(f" 계산 결과:")
        logger.info(f"  전체 종목 수: {len(metrics_df)}개")
        logger.info(f"  배당주 종목 수: {len(dividend_stocks)}개")
        
        if not dividend_stocks.empty:
            logger.info(f"  TTM 배당수익률 평균: {dividend_stocks['dividend_yield_ttm'].mean():.2f}%")
            logger.info(f"  TTM 배당수익률 최대: {dividend_stocks['dividend_yield_ttm'].max():.2f}%")
            
            # 배당수익률 상위 5개
            top_dividend = dividend_stocks.nlargest(5, 'dividend_yield_ttm')
            logger.info(f"  배당수익률 상위 5개:")
            for _, row in top_dividend.iterrows():
                logger.info(f"    {row['ticker']}: {row['dividend_yield_ttm']:.2f}% (TTM: ${row['dividend_ttm']:.2f})")
        
        return metrics_df
    
    def save_dividend_metrics_to_delta(self, metrics_df: pd.DataFrame, target_date: date):
        """배당 지표를 Delta Table에 저장 (Silver 스키마)"""
        logger.info(f"\n💾 배당 지표를 Silver Delta Table에 저장 중...")
        
        try:
            # Delta Table이 존재하는지 확인
            delta_table = DeltaTable(self.silver_dividend_metrics_path)
            mode = "append"
            logger.info("✅ 기존 Silver 배당 지표 테이블에 데이터 추가")
        except Exception:
            mode = "overwrite"
            logger.info("�� 새로운 Silver 배당 지표 테이블 생성")
        
        # Delta Table에 저장
        write_deltalake(
            self.silver_dividend_metrics_path,
            metrics_df,
            mode=mode,
            partition_by=["as_of_date"]  # 기준일별 파티셔닝
        )
        
        logger.info(f"✅ Silver 배당 지표 저장 완료: {len(metrics_df)}행")
        logger.info(f"📍 저장 위치: {self.silver_dividend_metrics_path}")
    
    def analyze_dividend_metrics(self, metrics_df: pd.DataFrame):
        """배당 지표 분석"""
        logger.info(f"\n📈 배당 지표 분석 결과:")
        
        dividend_stocks = metrics_df[metrics_df['dividend_ttm'] > 0]
        
        if dividend_stocks.empty:
            logger.info("배당주가 없습니다.")
            return
        
        # 배당수익률 분포
        logger.info(f"\n📊 배당수익률 분포:")
        logger.info(f"  평균: {dividend_stocks['dividend_yield_ttm'].mean():.2f}%")
        logger.info(f"  중간값: {dividend_stocks['dividend_yield_ttm'].median():.2f}%")
        logger.info(f"  최대값: {dividend_stocks['dividend_yield_ttm'].max():.2f}%")
        logger.info(f"  최소값: {dividend_stocks['dividend_yield_ttm'].min():.2f}%")
        
        # 배당수익률 구간별 분포
        bins = [0, 1, 2, 3, 5, 10, float('inf')]
        labels = ['0-1%', '1-2%', '2-3%', '3-5%', '5-10%', '10%+']
        dividend_stocks['yield_range'] = pd.cut(dividend_stocks['dividend_yield_ttm'], bins=bins, labels=labels, right=False)
        
        logger.info(f"\n📊 배당수익률 구간별 분포:")
        yield_dist = dividend_stocks['yield_range'].value_counts().sort_index()
        for range_label, count in yield_dist.items():
            logger.info(f"  {range_label}: {count}개")
        
        # 배당 횟수 분포
        logger.info(f"\n📊 연간 배당 횟수 분포:")
        div_count_dist = dividend_stocks['div_count_1y'].value_counts().sort_index()
        for count, freq in div_count_dist.items():
            logger.info(f"  {count}회: {freq}개 종목")
        
        # 배당수익률 상위 10개
        top_dividend = dividend_stocks.nlargest(10, 'dividend_yield_ttm')
        logger.info(f"\n💰 배당수익률 상위 10개:")
        for i, (_, row) in enumerate(top_dividend.iterrows(), 1):
            last_div = row['last_div_date'].strftime('%Y-%m-%d') if pd.notna(row['last_div_date']) else 'N/A'
            logger.info(f"  {i:2d}. {row['ticker']}: {row['dividend_yield_ttm']:.2f}% "
                       f"(TTM: ${row['dividend_ttm']:.2f}, 횟수: {row['div_count_1y']}회, "
                       f"최근: {last_div})")
    
    def run_silver_processing(self, target_date: Optional[date] = None):
        """Silver Layer 전체 처리 실행 (권장 구조)"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info(" Silver Layer 처리 시작 (권장 구조)")
        logger.info("=" * 80)
        logger.info(f" 처리 날짜: {target_date}")
        
        try:
            # 1. Bronze Layer 원천 데이터 로드
            logger.info(f"\n1️⃣ Bronze Layer 원천 데이터 로드...")
            price_df = self.load_bronze_price_data(target_date)
            dividend_events_df = self.load_bronze_dividend_events(target_date, lookback_days=365)
            
            # 2. 배당 지표 계산 (Silver Layer 핵심)
            logger.info(f"\n2️⃣ 배당 지표 계산 (Silver)...")
            metrics_df = self.build_dividend_metrics_daily(price_df, dividend_events_df, target_date)
            
            # 3. Silver Layer 저장
            logger.info(f"\n3️⃣ Silver Layer 저장...")
            self.save_dividend_metrics_to_delta(metrics_df, target_date)
            
            # 4. 배당 지표 분석
            logger.info(f"\n4️⃣ 배당 지표 분석...")
            self.analyze_dividend_metrics(metrics_df)
            
            # 5. 최종 요약
            logger.info("\n" + "=" * 80)
            logger.info("📈 Silver Layer 처리 결과 요약")
            logger.info("=" * 80)
            logger.info(f" 처리 날짜: {target_date}")
            logger.info(f"📊 전체 종목 수: {len(metrics_df)}개")
            logger.info(f"📊 배당주 종목 수: {len(metrics_df[metrics_df['dividend_ttm'] > 0])}개")
            logger.info(f" 저장된 Silver Delta Table:")
            logger.info(f"  - {self.silver_dividend_metrics_path}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Silver Layer 처리 실패: {e}")
            raise Exception(f"Silver Layer 처리 실패: {e}") from e

def main():
    """메인 실행 함수"""
    import os
    
    # GCS 설정 (환경변수에서 가져오기)
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    
    silver_layer = SilverLayerDelta(gcs_bucket=gcs_bucket)
    
    try:
        silver_layer.run_silver_processing()
    except Exception as e:
        logger.error(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
