"""
Bronze Layer 메인 클래스
- 전체 파이프라인 조율
- Backfill 기능
"""

import os
from datetime import datetime, timedelta
from typing import Optional, List
import logging
from dotenv import load_dotenv

# 모듈 import
from ...utils.data_collectors import SP500Collector, PriceDataCollector, DividendDataCollector
from ...utils.data_storage import DeltaStorageManager
from ...utils.data_validators import DataValidator, BackfillValidator

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeLayer:
    """Bronze Layer 메인 클래스 - 원천 데이터만 저장"""
    
    def __init__(self, gcs_bucket: str, gcs_path: str = "stock_dashboard/bronze"):
        """
        Bronze Layer 초기화
        
        Args:
            gcs_bucket: GCS 버킷 이름
            gcs_path: GCS 내 경로
        """
        self.storage_manager = DeltaStorageManager(gcs_bucket, gcs_path)
        self.sp500_collector = SP500Collector()
        self.price_collector = PriceDataCollector()
        self.dividend_collector = DividendDataCollector()
        self.data_validator = DataValidator()
        self.backfill_validator = BackfillValidator(self.storage_manager)
    
    def run_daily_collection(self, target_date: Optional[datetime.date] = None):
        """일일 데이터 수집 실행 (Bronze - 원천 데이터만)"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info("📊 S&P 500 Bronze Layer 일일 데이터 수집 (원천 데이터)")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        
        try:
            # 1. S&P 500 종목 리스트 수집
            logger.info("\n1️⃣ S&P 500 종목 리스트 수집...")
            spx_raw = self.sp500_collector.get_sp500_from_wikipedia()
            spx = self.sp500_collector.normalize_symbols(spx_raw)
            tickers = spx["Symbol"].dropna().unique().tolist()
            logger.info(f"✅ 수집 완료: {len(tickers)}개 종목")
            
            # 2. 가격 데이터 수집 (Bronze)
            logger.info(f"\n2️⃣ 가격 데이터 수집 (Bronze)...")
            all_daily_data, successful_tickers, failed_tickers = self.price_collector.get_daily_data_for_tickers(tickers, target_date)
            
            if all_daily_data:
                # 데이터 검증
                for data in all_daily_data:
                    self.data_validator.validate_price_data(data)
                
                self.storage_manager.save_price_data_to_delta(all_daily_data, target_date)
            else:
                logger.error("❌ 가격 데이터 수집에 실패했습니다.")
                return
            
            # 3. 배당 이벤트 수집 (Bronze) - 최근 400일 범위
            logger.info(f"\n3️⃣ 배당 이벤트 수집 (Bronze)...")
            since = target_date - timedelta(days=400)
            dividend_events_df = self.dividend_collector.fetch_dividend_events_for_tickers(successful_tickers, since, target_date)
            
            if not dividend_events_df.empty:
                # 데이터 검증
                self.data_validator.validate_dividend_data(dividend_events_df)
                self.storage_manager.save_dividend_events_to_delta(dividend_events_df)
            
            # 4. 최종 요약
            logger.info("\n" + "=" * 80)
            logger.info("📈 Bronze Layer 수집 결과 요약")
            logger.info("=" * 80)
            logger.info(f" 수집 날짜: {target_date}")
            logger.info(f" 전체 종목 수: {len(tickers)}개")
            logger.info(f"✅ 성공한 종목: {len(successful_tickers)}개")
            logger.info(f"❌ 실패한 종목: {len(failed_tickers)}개")
            logger.info(f" 배당 이벤트: {len(dividend_events_df)}개")
            logger.info(f" 저장된 Bronze Delta Table:")
            logger.info(f"  - {self.storage_manager.price_table_path}")
            logger.info(f"  - {self.storage_manager.dividend_events_table_path}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Bronze Layer 수집 실패: {e}")
            raise
    
    def run_backfill(self, start_date: datetime.date, end_date: Optional[datetime.date] = None, 
                     batch_size: int = 30, delay_between_batches: int = 60):
        """
        지정된 기간의 누락된 데이터를 backfill합니다.
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜 (None이면 오늘까지)
            batch_size: 배치당 처리할 날짜 수
            delay_between_batches: 배치 간 대기 시간 (초)
        """
        if end_date is None:
            end_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info("🔄 Bronze Layer Backfill 시작")
        logger.info("=" * 80)
        logger.info(f" 기간: {start_date} ~ {end_date}")
        logger.info(f" 배치 크기: {batch_size}일")
        logger.info(f" 배치 간 대기: {delay_between_batches}초")
        
        # 가장 이른 누락된 날짜 찾기
        earliest_missing = self.backfill_validator.find_earliest_missing_date(start_date, end_date)
        if earliest_missing is None:
            logger.info("✅ 지정된 기간에 누락된 데이터가 없습니다.")
            return
        
        # 처리할 날짜 목록 생성 (영업일만)
        dates_to_process = self.backfill_validator.generate_trading_dates(earliest_missing, end_date)
        
        logger.info(f"📅 처리할 영업일 수: {len(dates_to_process)}일")
        
        # 배치별로 처리
        total_batches = (len(dates_to_process) + batch_size - 1) // batch_size
        successful_dates = []
        failed_dates = []
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(dates_to_process))
            batch_dates = dates_to_process[start_idx:end_idx]
            
            logger.info(f"\n📦 배치 {batch_num + 1}/{total_batches} 처리 중...")
            logger.info(f" 처리할 날짜: {batch_dates[0]} ~ {batch_dates[-1]} ({len(batch_dates)}일)")
            
            batch_successful = []
            batch_failed = []
            
            for date in batch_dates:
                try:
                    logger.info(f"\n📊 {date} 데이터 수집 시작...")
                    
                    # 해당 날짜 데이터가 이미 있는지 확인
                    if self.storage_manager.check_existing_data(self.storage_manager.price_table_path, date):
                        logger.info(f"⏭️ {date} 데이터가 이미 존재합니다. 건너뜁니다.")
                        continue
                    
                    # S&P 500 종목 리스트 수집 (배치당 한 번만)
                    if batch_successful == [] and batch_failed == []:  # 배치의 첫 번째 날짜
                        logger.info("📋 S&P 500 종목 리스트 수집...")
                        spx_raw = self.sp500_collector.get_sp500_from_wikipedia()
                        spx = self.sp500_collector.normalize_symbols(spx_raw)
                        tickers = spx["Symbol"].dropna().unique().tolist()
                        logger.info(f"✅ 종목 리스트 수집 완료: {len(tickers)}개")
                    
                    # 가격 데이터 수집
                    all_daily_data, successful_tickers, failed_tickers = self.price_collector.get_daily_data_for_tickers(tickers, date)
                    
                    if all_daily_data:
                        # 데이터 검증
                        for data in all_daily_data:
                            self.data_validator.validate_price_data(data)
                        
                        self.storage_manager.save_price_data_to_delta(all_daily_data, date)
                        batch_successful.append(date)
                        logger.info(f"✅ {date} 데이터 수집 완료")
                    else:
                        batch_failed.append(date)
                        logger.error(f"❌ {date} 데이터 수집 실패")
                    
                except Exception as e:
                    batch_failed.append(date)
                    logger.error(f"❌ {date} 처리 중 오류: {e}")
                
                # 날짜 간 짧은 대기
                import time
                time.sleep(2)
            
            successful_dates.extend(batch_successful)
            failed_dates.extend(batch_failed)
            
            logger.info(f"📦 배치 {batch_num + 1} 완료:")
            logger.info(f"  ✅ 성공: {len(batch_successful)}일")
            logger.info(f"  ❌ 실패: {len(batch_failed)}일")
            
            # 마지막 배치가 아니면 대기
            if batch_num < total_batches - 1:
                logger.info(f"⏳ {delay_between_batches}초 대기 후 다음 배치 시작...")
                import time
                time.sleep(delay_between_batches)
        
        # 최종 결과 요약
        logger.info("\n" + "=" * 80)
        logger.info("📈 Backfill 완료 요약")
        logger.info("=" * 80)
        logger.info(f" 처리 기간: {start_date} ~ {end_date}")
        logger.info(f" 전체 영업일: {len(dates_to_process)}일")
        logger.info(f"✅ 성공: {len(successful_dates)}일")
        logger.info(f"❌ 실패: {len(failed_dates)}일")
        
        if successful_dates:
            logger.info(f" 성공한 날짜: {successful_dates[0]} ~ {successful_dates[-1]}")
        if failed_dates:
            logger.info(f" 실패한 날짜: {failed_dates}")
        
        logger.info("=" * 80)
    
    def run_smart_backfill(self, days_back: int = 365):
        """
        스마트 backfill: 최근 N일 중에서 누락된 데이터만 수집
        
        Args:
            days_back: 몇 일 전부터 확인할지
        """
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"🧠 스마트 Backfill 시작 (최근 {days_back}일 확인)")
        self.run_backfill(start_date, end_date)

def main():
    """메인 실행 함수"""
    # GCS 설정 (환경변수에서 가져오기)
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    
    bronze_layer = BronzeLayer(gcs_bucket=gcs_bucket)
    
    try:
        # Bronze Layer 원천 데이터 수집 실행
        bronze_layer.run_daily_collection()
    except Exception as e:
        logger.error(f"❌ 실행 실패: {e}")
        raise

def main_backfill():
    """Backfill 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Bronze Layer Backfill")
    parser.add_argument("--start-date", type=str, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--days-back", type=int, default=365, help="스마트 backfill 시 확인할 일수")
    parser.add_argument("--smart", action="store_true", help="스마트 backfill 사용")
    parser.add_argument("--batch-size", type=int, default=30, help="배치 크기")
    parser.add_argument("--delay", type=int, default=60, help="배치 간 대기 시간(초)")
    
    args = parser.parse_args()
    
    # GCS 설정
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    bronze_layer = BronzeLayer(gcs_bucket=gcs_bucket)
    
    try:
        if args.smart:
            # 스마트 backfill
            bronze_layer.run_smart_backfill(args.days_back)
        else:
            # 일반 backfill
            if not args.start_date:
                raise ValueError("일반 backfill을 위해서는 --start-date가 필요합니다.")
            
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            end_date = None
            if args.end_date:
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
            
            bronze_layer.run_backfill(start_date, end_date, args.batch_size, args.delay)
            
    except Exception as e:
        logger.error(f"❌ Backfill 실패: {e}")
        raise

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        # backfill 모드
        sys.argv.pop(1)  # "backfill" 제거
        main_backfill()
    else:
        # 일반 모드
        main()
