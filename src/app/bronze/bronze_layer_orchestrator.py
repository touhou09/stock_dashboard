"""
Bronze Layer 조율자 - 가격/배당 데이터 수집을 옵션별로 분리
"""

import os
from datetime import datetime, timedelta
from typing import Optional
import logging
import pandas as pd
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
    
    def run_price_only_collection(self, target_date: Optional[datetime.date] = None, batch_size: int = 50):
        """가격 데이터만 수집 (배치 단위 저장)"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info("📊 Bronze Layer 가격 데이터 수집")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        logger.info(f" 배치 크기: {batch_size}개씩 처리")
        
        try:
            # 1. S&P 500 종목 리스트 수집
            tickers = self.get_sp500_tickers()
            total_tickers = len(tickers)
            
            # 2. 배치 단위로 가격 데이터 수집 및 저장
            logger.info(f"\n📈 가격 데이터 수집 시작... (총 {total_tickers}개 → {(total_tickers + batch_size - 1) // batch_size}개 배치)")
            
            total_successful = 0
            total_failed = 0
            
            # [수정] 배치 단위로 처리
            for batch_num in range(0, total_tickers, batch_size):
                batch_tickers = tickers[batch_num:batch_num + batch_size]
                batch_idx = batch_num // batch_size + 1
                total_batches = (total_tickers + batch_size - 1) // batch_size
                
                logger.info(f"\n🔄 배치 {batch_idx}/{total_batches} 처리 중... ({len(batch_tickers)}개 종목)")
                
                # 배치 데이터 수집
                batch_data, successful_tickers, failed_tickers = self.price_collector.get_daily_data_for_tickers(batch_tickers, target_date)
                
                if batch_data:
                    # 데이터 검증
                    for data in batch_data:
                        self.data_validator.validate_price_data(data)
                    
                    # [수정] 배치별로 즉시 저장 (append 모드)
                    self.storage_manager.save_price_data_to_delta(batch_data, target_date)
                    logger.info(f"✅ 배치 {batch_idx} 저장 완료: {len(successful_tickers)}개 성공, {len(failed_tickers)}개 실패")
                
                total_successful += len(successful_tickers)
                total_failed += len(failed_tickers)
            
            logger.info(f"\n✅ 전체 가격 데이터 수집 완료: 성공 {total_successful}개, 실패 {total_failed}개")
            return True
            
        except Exception as e:
            logger.error(f"❌ 가격 데이터 수집 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_latest_dividend_date(self) -> Optional[datetime.date]:
        """Delta Table에서 가장 최근 배당 이벤트 날짜 조회"""
        try:
            from deltalake import DeltaTable
            table_path = self.storage_manager.dividend_events_table_path
            
            delta_table = DeltaTable(table_path)
            df = delta_table.to_pandas()
            
            if df.empty:
                return None
            
            # ex_date 컬럼에서 최근 날짜 찾기
            df['ex_date'] = pd.to_datetime(df['ex_date']).dt.date
            latest_date = df['ex_date'].max()
            
            logger.info(f"📅 기존 배당 데이터 최근 날짜: {latest_date}")
            return latest_date
            
        except Exception as e:
            logger.info(f"📅 기존 배당 데이터 없음 (테이블 없거나 비어있음): {e}")
            return None
    
    def run_dividend_only_collection(self, target_date: Optional[datetime.date] = None):
        """배당 데이터만 수집 (증분 수집)"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info("💰 Bronze Layer 배당 데이터 수집 (증분 수집)")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        
        try:
            # 1. S&P 500 종목 리스트 수집
            tickers = self.get_sp500_tickers()
            
            # 2. [수정] 기존 데이터 최근 날짜 확인 (증분 수집)
            latest_date = self.get_latest_dividend_date()
            
            if latest_date is not None:
                # 증분 수집: 최근 날짜 다음날부터 수집
                since = latest_date + timedelta(days=1)
                logger.info(f"🔄 증분 수집: {since} ~ {target_date}")
                
                if since > target_date:
                    logger.info(f"✅ 이미 최신 데이터 보유 (최근: {latest_date})")
                    return True
            else:
                # 초기 수집: 400일치 전체 수집
                since = target_date - timedelta(days=400)
                logger.info(f"🆕 초기 수집: {since} ~ {target_date} (400일치)")
            
            # 3. 배당 이벤트 수집
            logger.info(f"\n💰 배당 이벤트 수집 시작...")
            dividend_events_df = self.dividend_collector.fetch_dividend_events_for_tickers(tickers, since, target_date, target_date)
            
            if not dividend_events_df.empty:
                # 데이터 검증
                self.data_validator.validate_dividend_data(dividend_events_df)
                self.storage_manager.save_dividend_events_to_delta(dividend_events_df)
                logger.info(f"✅ 배당 데이터 수집 완료: {len(dividend_events_df)}개 이벤트")
            else:
                logger.info(f"✅ 배당 데이터 수집 완료: 0개 이벤트 (기간: {since} ~ {target_date})")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 배당 데이터 수집 실패: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def run_full_collection(self, target_date: Optional[datetime.date] = None, batch_size: int = 50):
        """전체 데이터 수집 (가격 + 배당)"""
        if target_date is None:
            target_date = datetime.now().date() - timedelta(days=1)
        
        logger.info("=" * 80)
        logger.info("📊 Bronze Layer 전체 데이터 수집 (가격 + 배당)")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        logger.info(f" 배치 크기: {batch_size}개씩 처리")
        
        # 가격 데이터 수집 (배치 단위)
        price_success = self.run_price_only_collection(target_date, batch_size=batch_size)
        
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
