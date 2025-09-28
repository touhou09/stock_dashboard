"""
Bronze Layer 조율자 - 가격/배당 데이터 수집을 옵션별로 분리
"""

import os
from datetime import datetime, timedelta
from typing import Optional
import logging
from dotenv import load_dotenv

from ...utils.data_collectors import SP500Collector, PriceDataCollector, DividendDataCollector
from ...utils.data_storage import DeltaStorageManager
from ...utils.data_validators import DataValidator, BackfillValidator

load_dotenv()
logger = logging.getLogger(__name__)

class BronzeLayerOrchestrator:
    """Bronze Layer 조율자 - 옵션별 데이터 수집 관리"""
    
    def __init__(self, gcs_bucket: str, gcs_path: str = "stock_dashboard/bronze"):
        self.storage_manager = DeltaStorageManager(gcs_bucket, gcs_path)
        self.sp500_collector = SP500Collector()
        self.price_collector = PriceDataCollector()
        self.dividend_collector = DividendDataCollector()
        self.data_validator = DataValidator()
        self.backfill_validator = BackfillValidator(self.storage_manager)
    
    def get_sp500_tickers(self) -> list:
        """S&P 500 종목 리스트 수집 (공통 함수)"""
        logger.info("📋 S&P 500 종목 리스트 수집...")
        spx_raw = self.sp500_collector.get_sp500_from_wikipedia()
        spx = self.sp500_collector.normalize_symbols(spx_raw)
        tickers = spx["Symbol"].dropna().unique().tolist()
        logger.info(f"✅ 종목 리스트 수집 완료: {len(tickers)}개")
        return tickers
    
    def run_price_only_collection(self, target_date: Optional[datetime.date] = None):
        """가격 데이터만 수집"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info("📊 Bronze Layer 가격 데이터 수집")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        
        try:
            # 1. S&P 500 종목 리스트 수집
            tickers = self.get_sp500_tickers()
            
            # 2. 가격 데이터 수집
            logger.info(f"\n📈 가격 데이터 수집 시작...")
            all_daily_data, successful_tickers, failed_tickers = self.price_collector.get_daily_data_for_tickers(tickers, target_date)
            
            if all_daily_data:
                # 데이터 검증
                for data in all_daily_data:
                    self.data_validator.validate_price_data(data)
                
                self.storage_manager.save_price_data_to_delta(all_daily_data, target_date)
                logger.info(f"✅ 가격 데이터 수집 완료: {len(successful_tickers)}개 종목")
            else:
                logger.error("❌ 가격 데이터 수집 실패")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 가격 데이터 수집 실패: {e}")
            return False
    
    def run_dividend_only_collection(self, target_date: Optional[datetime.date] = None):
        """배당 데이터만 수집"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info("💰 Bronze Layer 배당 데이터 수집")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        
        try:
            # 1. S&P 500 종목 리스트 수집
            tickers = self.get_sp500_tickers()
            
            # 2. 배당 이벤트 수집 (최근 400일 범위)
            logger.info(f"\n💰 배당 이벤트 수집 시작...")
            since = target_date - timedelta(days=400)
            dividend_events_df = self.dividend_collector.fetch_dividend_events_for_tickers(tickers, since, target_date)
            
            if not dividend_events_df.empty:
                # 데이터 검증
                self.data_validator.validate_dividend_data(dividend_events_df)
                self.storage_manager.save_dividend_events_to_delta(dividend_events_df)
                logger.info(f"✅ 배당 데이터 수집 완료: {len(dividend_events_df)}개 이벤트")
            else:
                logger.info("✅ 배당 데이터 수집 완료: 0개 이벤트")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 배당 데이터 수집 실패: {e}")
            return False
    
    def run_full_collection(self, target_date: Optional[datetime.date] = None):
        """전체 데이터 수집 (가격 + 배당)"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info("📊 Bronze Layer 전체 데이터 수집 (가격 + 배당)")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        
        # 가격 데이터 수집
        price_success = self.run_price_only_collection(target_date)
        
        # 배당 데이터 수집
        dividend_success = self.run_dividend_only_collection(target_date)
        
        # 최종 결과
        logger.info("\n" + "=" * 80)
        logger.info("📈 Bronze Layer 전체 수집 결과")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        logger.info(f"✅ 가격 데이터: {'성공' if price_success else '실패'}")
        logger.info(f"✅ 배당 데이터: {'성공' if dividend_success else '실패'}")
        logger.info("=" * 80)
        
        return price_success and dividend_success

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Bronze Layer 데이터 수집")
    parser.add_argument("--mode", choices=["full", "price", "dividend"], 
                       default="full", help="수집 모드")
    parser.add_argument("--date", type=str, help="수집 날짜 (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # GCS 설정
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    orchestrator = BronzeLayerOrchestrator(gcs_bucket=gcs_bucket)
    
    # 날짜 파싱
    target_date = None
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    
    try:
        if args.mode == "full":
            orchestrator.run_full_collection(target_date)
        elif args.mode == "price":
            orchestrator.run_price_only_collection(target_date)
        elif args.mode == "dividend":
            orchestrator.run_dividend_only_collection(target_date)
            
    except Exception as e:
        logger.error(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
