"""
Silver Layer - Delta Lake 기반 데이터 정제 및 통합
Bronze Layer에서 수집한 Delta Table을 기반으로 배당주 필터링 및 통합 테이블 생성
"""

import pandas as pd
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple
import logging
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverLayerDelta:
    """Silver Layer Delta Lake 기반 관리 클래스"""
    
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
        
        # Delta Table 경로 설정
        self.bronze_price_path = f"gs://{gcs_bucket}/{bronze_path}/sp500_daily_prices"
        self.bronze_dividend_path = f"gs://{gcs_bucket}/{bronze_path}/sp500_dividend_info"
        self.silver_unified_path = f"gs://{gcs_bucket}/{silver_path}/unified_stock_data"
        self.silver_dividend_path = f"gs://{gcs_bucket}/{silver_path}/dividend_stocks"
    
    def load_bronze_data(self, target_date: Optional[date] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Bronze Layer에서 Delta Table 데이터 로드"""
        logger.info(f" Bronze Layer Delta Table 데이터 로드 중...")
        
        try:
            # Delta Table에서 데이터 로드
            price_delta = DeltaTable(self.bronze_price_path)
            dividend_delta = DeltaTable(self.bronze_dividend_path)
            
            # pandas DataFrame으로 변환
            price_df = price_delta.to_pandas()
            dividend_df = dividend_delta.to_pandas()
            
            # 날짜 필터링
            if target_date:
                price_df = price_df[price_df['date'] == target_date]
            
            logger.info(f"✅ 가격 데이터 로드 완료: {len(price_df)}행")
            logger.info(f"✅ 배당 정보 로드 완료: {len(dividend_df)}행")
            
            return price_df, dividend_df
            
        except Exception as e:
            logger.error(f"❌ Bronze Layer 데이터 로드 실패: {e}")
            # 수정: 일관성 있는 에러 메시지로 예외 재발생
            raise Exception(f"Bronze Layer 데이터 로드 실패: {e}") from e
    
    def create_unified_table(self, price_df: pd.DataFrame, dividend_df: pd.DataFrame) -> pd.DataFrame:
        """통합 테이블 생성"""
        logger.info(f"\n️ Silver Layer 통합 테이블 생성 중...")
        
        # Delta Lake의 스키마 진화를 고려한 유연한 컬럼 선택
        # 필수 컬럼만 확인하고, 나머지는 선택적
        required_columns = ['ticker', 'company_name', 'sector', 'has_dividend', 'dividend_yield']
        
        # 필수 컬럼 존재 확인
        missing_required = [col for col in required_columns if col not in dividend_df.columns]
        if missing_required:
            raise ValueError(f"필수 컬럼이 누락되었습니다: {missing_required}")
        
        # 존재하는 컬럼만 선택 (Delta Lake의 스키마 진화 지원)
        available_columns = [col for col in dividend_df.columns if col in required_columns or 
                            col in ['dividend_yield_percent', 'dividend_rate', 'ex_dividend_date', 
                                   'payment_date', 'dividend_frequency', 'market_cap', 'last_price']]
        
        # 가격 데이터와 배당 정보 조인
        unified_df = price_df.merge(
            dividend_df[available_columns], 
            on='ticker', 
            how='left'
        )
        
        # 배당주 여부 플래그
        unified_df['is_dividend_stock'] = unified_df['has_dividend'].fillna(False)
        unified_df['processing_timestamp'] = datetime.now()
        
        # 데이터 품질 검증
        total_count = len(unified_df)
        dividend_count = unified_df['is_dividend_stock'].sum()
        
        logger.info(f"📊 데이터 품질 검증:")
        logger.info(f"  전체 레코드 수: {total_count}")
        logger.info(f"  배당주 레코드 수: {dividend_count}")
        logger.info(f"  배당주 비율: {(dividend_count / total_count * 100):.1f}%")
        
        # 결측값 확인
        logger.info(f"  결측값 현황:")
        for col in unified_df.columns:
            null_count = unified_df[col].isnull().sum()
            if null_count > 0:
                logger.info(f"    {col}: {null_count}개 ({(null_count/total_count*100):.1f}%)")
        
        return unified_df
    
    def save_unified_data(self, unified_df: pd.DataFrame, target_date: Optional[date] = None):
        """통합 데이터를 Delta Table에 저장"""
        logger.info(f"\n💾 Silver Layer 통합 데이터 저장 중...")
        
        try:
            # 기존 Delta Table 확인
            unified_delta = DeltaTable(self.silver_unified_path)
            mode = "append"
            logger.info("✅ 기존 통합 Delta Table에 데이터 추가")
        except Exception:
            mode = "overwrite"
            logger.info("🆕 새로운 통합 Delta Table 생성")
        
        # Delta Table에 저장
        write_deltalake(
            self.silver_unified_path,
            unified_df,
            mode=mode,
            partition_by=["date", "is_dividend_stock"],  # 날짜와 배당주 여부별 파티셔닝
            engine="pyarrow"
        )
        
        logger.info(f"✅ 통합 테이블 저장 완료: {len(unified_df)}행")
        logger.info(f"📍 저장 위치: {self.silver_unified_path}")
        
        # 배당주만 필터링한 테이블 저장
        dividend_stocks_df = unified_df[unified_df['is_dividend_stock'] == True]
        
        if not dividend_stocks_df.empty:
            try:
                dividend_delta = DeltaTable(self.silver_dividend_path)
                mode = "append"
                logger.info("✅ 기존 배당주 Delta Table에 데이터 추가")
            except Exception:
                mode = "overwrite"
                logger.info("🆕 새로운 배당주 Delta Table 생성")
            
            # Delta Table에 저장
            write_deltalake(
                self.silver_dividend_path,
                dividend_stocks_df,
                mode=mode,
                partition_by=["date", "sector"],  # 날짜와 섹터별 파티셔닝
                engine="pyarrow"
            )
            
            logger.info(f"✅ 배당주 테이블 저장 완료: {len(dividend_stocks_df)}행")
            logger.info(f"📍 저장 위치: {self.silver_dividend_path}")
        else:
            logger.warning("배당주 데이터가 없습니다.")
    
    def analyze_dividend_stocks(self, unified_df: pd.DataFrame):
        """배당주 분석 (pandas 기반)"""
        logger.info(f"\n📈 배당주 분석 결과:")
        
        dividend_stocks = unified_df[unified_df['is_dividend_stock'] == True]
        
        if dividend_stocks.empty:
            logger.info("배당주가 없습니다.")
            return
        
        # 섹터별 배당주 분포
        sector_dist = dividend_stocks.groupby('sector').size().sort_values(ascending=False)
        logger.info(f"\n 섹터별 배당주 분포:")
        for sector, count in sector_dist.head(10).items():
            logger.info(f"  {sector}: {count}개")
        
        # 배당수익률 상위 10개
        top_dividend = dividend_stocks.nlargest(10, 'dividend_yield_percent')[
            ['ticker', 'company_name', 'dividend_yield_percent', 'sector']
        ]
        logger.info(f"\n💰 배당수익률 상위 10개:")
        for _, row in top_dividend.iterrows():
            logger.info(f"  {row['ticker']} ({row['company_name'][:30]}): {row['dividend_yield_percent']:.2f}% - {row['sector']}")
        
        # 배당수익률 통계
        logger.info(f"\n📊 배당수익률 통계:")
        logger.info(f"  평균: {dividend_stocks['dividend_yield_percent'].mean():.2f}%")
        logger.info(f"  중간값: {dividend_stocks['dividend_yield_percent'].median():.2f}%")
        logger.info(f"  최대값: {dividend_stocks['dividend_yield_percent'].max():.2f}%")
        logger.info(f"  최소값: {dividend_stocks['dividend_yield_percent'].min():.2f}%")
    
    def run_silver_processing(self, target_date: Optional[date] = None):
        """Silver Layer 전체 처리 실행"""
        if target_date is None:
            target_date = date.today()
        
        logger.info("=" * 80)
        logger.info(f" Silver Layer 처리 시작")
        logger.info("=" * 80)
        logger.info(f" 처리 날짜: {target_date}")
        
        try:
            # 1. Bronze Layer 데이터 로드
            logger.info(f"\n1️⃣ Bronze Layer 데이터 로드...")
            price_df, dividend_df = self.load_bronze_data(target_date)
            
            # 2. 통합 테이블 생성
            logger.info(f"\n2️⃣ 통합 테이블 생성...")
            unified_df = self.create_unified_table(price_df, dividend_df)
            
            # 3. 데이터 저장
            logger.info(f"\n3️⃣ 데이터 저장...")
            self.save_unified_data(unified_df, target_date)
            
            # 4. 배당주 분석
            logger.info(f"\n4️⃣ 배당주 분석...")
            self.analyze_dividend_stocks(unified_df)
            
            # 5. 최종 요약
            logger.info("\n" + "=" * 80)
            logger.info("📈 Silver Layer 처리 결과 요약")
            logger.info("=" * 80)
            logger.info(f" 처리 날짜: {target_date}")
            logger.info(f"📊 전체 종목 수: {unified_df['ticker'].nunique()}개")
            logger.info(f"💰 배당주 종목 수: {unified_df['is_dividend_stock'].sum()}개")
            logger.info(f" 저장된 Delta Table:")
            logger.info(f"  - {self.silver_unified_path} (통합 테이블)")
            logger.info(f"  - {self.silver_dividend_path} (배당주 테이블)")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Silver Layer 처리 실패: {e}")
            # 수정: 일관성 있는 에러 메시지로 예외 재발생
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

    
    try:
        silver_layer.run_silver_processing()
    except Exception as e:
        logger.error(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
