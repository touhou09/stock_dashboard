"""
Point-in-Time Bronze Layer - 편입일 기준 백필 지원
생존 편향 문제 해결을 위한 시점별 정확한 데이터 수집
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
from dotenv import load_dotenv
import time

from src.app.membership.sp500_membership_tracker import SP500MembershipTracker
from src.utils.data_storage import DeltaStorageManager

try:
    load_dotenv()
except Exception:
    pass
logger = logging.getLogger(__name__)

class BronzeLayerPointInTime:
    """Point-in-Time Bronze Layer - 편입일 기준 백필 지원"""
    
    def __init__(self, gcs_bucket: str, gcs_path: str = "stock_dashboard/bronze"):
        """
        초기화
        
        Args:
            gcs_bucket: GCS 버킷 이름
            gcs_path: GCS 경로
        """
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        
        # 멤버십 추적기 초기화
        self.membership_tracker = SP500MembershipTracker(gcs_bucket)
        
        # Delta Storage Manager 초기화
        self.storage_manager = DeltaStorageManager(gcs_bucket, gcs_path)
    
    def get_constituents_for_date(self, target_date: date) -> List[str]:
        """
        특정 날짜의 S&P 500 구성 종목 조회
        
        Args:
            target_date: 대상 날짜
            
        Returns:
            List[str]: 해당 날짜의 구성 종목 리스트
        """
        logger.info(f"📋 {target_date} 날짜의 S&P 500 구성 종목 조회 중...")
        
        try:
            # 일자별 멤버십 조회
            daily_membership = self.membership_tracker.get_daily_membership(target_date)
            
            if not daily_membership.empty:
                # 해당 날짜에 멤버인 종목들만 필터링
                members = daily_membership[daily_membership['is_member'] == True]
                tickers = members['ticker'].unique().tolist()
                
                logger.info(f"✅ {target_date} 구성 종목: {len(tickers)}개")
                return tickers
            else:
                logger.warning(f"⚠️ {target_date} 멤버십 데이터가 없습니다. 해당 연도 구성으로 대체합니다.")
                # 멤버십 데이터가 없는 경우 해당 연도 구성으로 대체
                target_year = target_date.year
                current_sp500 = self.membership_tracker.get_sp500_for_year(target_year)
                return current_sp500['Symbol'].tolist()
                
        except Exception as e:
            logger.error(f"❌ {target_date} 구성 종목 조회 실패: {e}")
            # 에러 시 해당 연도 구성으로 대체
            target_year = target_date.year
            current_sp500 = self.membership_tracker.get_sp500_for_year(target_year)
            return current_sp500['Symbol'].tolist()
    
    def get_price_data_for_date(self, tickers: List[str], target_date: date, batch_size: int = 50) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
        """
        특정 날짜의 가격 데이터 수집 (배치 처리)
        
        Args:
            tickers: 종목 리스트
            target_date: 대상 날짜
            batch_size: 배치 크기
            
        Returns:
            Tuple[List[pd.DataFrame], List[str], List[str]]: (데이터, 성공종목, 실패종목)
        """
        logger.info(f"📊 {target_date} 가격 데이터 수집 중... (총 {len(tickers)}개 종목)")
        
        all_data = []
        successful = []
        failed = []
        
        # 배치 단위로 처리
        for batch_num in range(0, len(tickers), batch_size):
            batch_tickers = tickers[batch_num:batch_num + batch_size]
            batch_idx = batch_num // batch_size + 1
            total_batches = (len(tickers) + batch_size - 1) // batch_size
            
            logger.info(f"🔄 배치 {batch_idx}/{total_batches} 처리 중... ({len(batch_tickers)}개 종목)")
            
            batch_data, batch_successful, batch_failed = self._collect_batch_price_data(batch_tickers, target_date)
            
            all_data.extend(batch_data)
            successful.extend(batch_successful)
            failed.extend(batch_failed)
            
            # API 제한 방지
            time.sleep(0.5)
        
        logger.info(f"✅ {target_date} 가격 데이터 수집 완료: 성공 {len(successful)}개, 실패 {len(failed)}개")
        return all_data, successful, failed
    
    def _collect_batch_price_data(self, batch_tickers: List[str], target_date: date) -> Tuple[List[pd.DataFrame], List[str], List[str]]:
        """배치 단위 가격 데이터 수집"""
        batch_data = []
        batch_successful = []
        batch_failed = []
        
        for ticker in batch_tickers:
            try:
                yf_ticker = yf.Ticker(ticker)
                hist = yf_ticker.history(start=target_date, end=target_date + timedelta(days=1))
                
                if not hist.empty:
                    hist_df = hist.reset_index()
                    
                    # 필수 컬럼 검증 및 추가
                    required_columns = ['open', 'high', 'low', 'close', 'volume', 'adj_close']
                    for col in required_columns:
                        if col not in hist_df.columns:
                            if col == 'adj_close':
                                hist_df[col] = hist_df.get('Close', hist_df.get('close', 0))  # 조정주가 없으면 종가 사용
                            else:
                                hist_df[col] = 0  # 기본값 설정
                    
                    hist_df['ticker'] = ticker
                    hist_df['date'] = target_date
                    hist_df['ingest_at'] = datetime.now()  # 기존 스키마에 맞춰 복원
                    
                    batch_data.append(hist_df)
                    batch_successful.append(ticker)
                    logger.debug(f"✅ {ticker} 가격 데이터 수집 성공")
                else:
                    batch_failed.append(ticker)
                    logger.warning(f"⚠️ {ticker} 가격 데이터 없음")
                
                # API 제한 방지
                time.sleep(0.1)
                
            except Exception as e:
                batch_failed.append(ticker)
                logger.error(f"❌ {ticker} 가격 데이터 수집 실패: {e}")
        
        return batch_data, batch_successful, batch_failed
    
    def get_dividend_data_for_date(self, tickers: List[str], target_date: date, lookback_days: int = 400) -> pd.DataFrame:
        """
        특정 날짜의 배당 데이터 수집
        
        Args:
            tickers: 종목 리스트
            target_date: 대상 날짜
            lookback_days: 배당 이력 조회 일수
            
        Returns:
            pd.DataFrame: 배당 이벤트 데이터
        """
        logger.info(f"💰 {target_date} 배당 데이터 수집 중... (TTM: {lookback_days}일)")
        
        dividend_events_list = []
        since_date = target_date - timedelta(days=lookback_days)
        
        for i, ticker in enumerate(tickers):
            try:
                logger.info(f"배당 정보 수집 중: {ticker} ({i+1}/{len(tickers)})")
                
                yf_ticker = yf.Ticker(ticker)
                
                # 배당 이력 조회
                dividend_history = yf_ticker.dividends
                
                if not dividend_history.empty:
                    # 기간 필터링
                    dividend_history = dividend_history[
                        (dividend_history.index.date >= since_date) & 
                        (dividend_history.index.date <= target_date)
                    ]
                    
                    # 배당 이벤트로 변환
                    for ex_date, amount in dividend_history.items():
                        dividend_events_list.append({
                            'ex_date': ex_date.date(),
                            'ticker': ticker,
                            'amount': amount,
                            'date': target_date,  # 수집 날짜
                            'ingest_at': datetime.now()
                        })
                
                # API 제한 방지
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"❌ {ticker} 배당 데이터 수집 실패: {e}")
        
        dividend_df = pd.DataFrame(dividend_events_list)
        logger.info(f"✅ {target_date} 배당 데이터 수집 완료: {len(dividend_df)}개 이벤트")
        
        return dividend_df
    
    def run_point_in_time_collection(self, target_date: date, batch_size: int = 50) -> bool:
        """
        Point-in-Time 데이터 수집 실행
        
        Args:
            target_date: 대상 날짜
            batch_size: 배치 크기
            
        Returns:
            bool: 성공 여부
        """
        logger.info("=" * 80)
        logger.info("📊 Point-in-Time Bronze Layer 데이터 수집 시작")
        logger.info("=" * 80)
        logger.info(f" 수집 날짜: {target_date}")
        logger.info(f" 배치 크기: {batch_size}개씩 처리")
        
        try:
            # 1. 해당 날짜의 S&P 500 구성 종목 조회
            logger.info(f"\n1️⃣ {target_date} S&P 500 구성 종목 조회...")
            tickers = self.get_constituents_for_date(target_date)
            
            if not tickers:
                logger.error("❌ 구성 종목을 찾을 수 없습니다.")
                return False
            
            # 2. 가격 데이터 수집
            logger.info(f"\n2️⃣ 가격 데이터 수집...")
            price_data, successful_tickers, failed_tickers = self.get_price_data_for_date(tickers, target_date, batch_size)
            
            if price_data:
                # 가격 데이터 저장
                self.storage_manager.save_price_data_to_delta(price_data, target_date)
                logger.info(f"✅ 가격 데이터 저장 완료: {len(price_data)}개")
            
            # 3. 배당 데이터 수집 (전체 종목 대상)
            logger.info(f"\n3️⃣ 배당 데이터 수집...")
            dividend_df = self.get_dividend_data_for_date(tickers, target_date)
            
            if not dividend_df.empty:
                # 배당 데이터 저장
                self.storage_manager.save_dividend_events_to_delta(dividend_df)
                logger.info(f"✅ 배당 데이터 저장 완료: {len(dividend_df)}개 이벤트")
            
            # 4. 수집 결과 요약
            logger.info("\n" + "=" * 80)
            logger.info("📈 Point-in-Time 수집 결과 요약")
            logger.info("=" * 80)
            logger.info(f" 수집 날짜: {target_date}")
            logger.info(f"📊 구성 종목 수: {len(tickers)}개")
            logger.info(f"📊 가격 데이터 수집 성공: {len(successful_tickers)}개")
            logger.info(f"📊 가격 데이터 수집 실패: {len(failed_tickers)}개")
            logger.info(f"📊 배당 이벤트 수집: {len(dividend_df)}개")
            logger.info("=" * 80)
            
            # 성공률이 90% 이상이면 성공으로 처리 (일부 종목 실패 허용)
            success_rate = len(successful_tickers) / (len(successful_tickers) + len(failed_tickers)) if (len(successful_tickers) + len(failed_tickers)) > 0 else 0
            is_success = success_rate >= 0.9  # 90% 이상 성공률
            
            if is_success:
                logger.info(f"✅ {target_date} 처리 성공 (성공률: {success_rate:.1%})")
            else:
                logger.warning(f"⚠️ {target_date} 처리 부분 성공 (성공률: {success_rate:.1%})")
            
            return is_success
            
        except Exception as e:
            logger.error(f"❌ Point-in-Time 수집 실패: {e}")
            return False
    
    def run_point_in_time_backfill(self, start_date: date, end_date: date, batch_size: int = 50) -> bool:
        """
        Point-in-Time 백필 실행
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            batch_size: 배치 크기
            
        Returns:
            bool: 성공 여부
        """
        logger.info("=" * 80)
        logger.info("🔄 Point-in-Time Bronze Layer 백필 시작")
        logger.info("=" * 80)
        logger.info(f" 백필 기간: {start_date} ~ {end_date}")
        logger.info(f" 배치 크기: {batch_size}개씩 처리")
        
        try:
            # 날짜 리스트 생성 (평일만)
            date_list = []
            current_date = start_date
            while current_date <= end_date:
                if current_date.weekday() < 5:  # 0-4: 월-금
                    date_list.append(current_date)
                current_date += timedelta(days=1)
            
            total_dates = len(date_list)
            logger.info(f"📊 백필할 날짜 수: {total_dates}개 (평일만)")
            
            if total_dates == 0:
                logger.info("✅ 처리할 날짜가 없습니다.")
                return True
            
            successful_dates = []
            failed_dates = []
            
            # 각 날짜별로 Point-in-Time 수집
            for i, target_date in enumerate(date_list, 1):
                logger.info(f"\n{'='*60}")
                logger.info(f"📅 Point-in-Time {i}/{total_dates} 처리 중: {target_date}")
                logger.info(f"{'='*60}")
                
                try:
                    success = self.run_point_in_time_collection(target_date, batch_size)
                    
                    if success:
                        successful_dates.append(target_date)
                        logger.info(f"✅ {target_date} Point-in-Time 처리 완료")
                    else:
                        failed_dates.append((target_date, "Point-in-Time 수집 실패"))
                        logger.error(f"❌ {target_date} Point-in-Time 처리 실패")
                        
                except Exception as e:
                    failed_dates.append((target_date, str(e)))
                    logger.error(f"❌ {target_date} Point-in-Time 처리 실패: {e}")
                    continue
            
            # 백필 결과 요약
            logger.info("\n" + "=" * 80)
            logger.info("📈 Point-in-Time 백필 결과 요약")
            logger.info("=" * 80)
            logger.info(f" 전체 처리 날짜: {total_dates}개")
            logger.info(f" 성공한 날짜: {len(successful_dates)}개")
            logger.info(f" 실패한 날짜: {len(failed_dates)}개")
            
            if failed_dates:
                logger.info(f"\n❌ 실패한 날짜:")
                for date, error in failed_dates[:10]:  # 최대 10개만 표시
                    logger.info(f"   - {date}: {error}")
                if len(failed_dates) > 10:
                    logger.info(f"   ... 외 {len(failed_dates) - 10}개")
            
            logger.info("=" * 80)
            return len(failed_dates) == 0
            
        except Exception as e:
            logger.error(f"❌ Point-in-Time 백필 실패: {e}")
            return False

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Point-in-Time Bronze Layer")
    parser.add_argument("--mode", choices=["single", "backfill"], default="single", help="실행 모드")
    parser.add_argument("--date", type=str, help="처리 날짜 (YYYY-MM-DD)")
    parser.add_argument("--start-date", type=str, help="백필 시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="백필 종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--batch-size", type=int, default=50, help="배치 크기")
    
    args = parser.parse_args()
    
    # GCS 설정
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    bronze_pit = BronzeLayerPointInTime(gcs_bucket)
    
    try:
        if args.mode == "single":
            if not args.date:
                print("❌ single 모드에서는 --date가 필요합니다.")
                return 1
            
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
            success = bronze_pit.run_point_in_time_collection(target_date, args.batch_size)
            
        elif args.mode == "backfill":
            if not args.start_date or not args.end_date:
                print("❌ backfill 모드에서는 --start-date와 --end-date가 필요합니다.")
                return 1
            
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
            success = bronze_pit.run_point_in_time_backfill(start_date, end_date, args.batch_size)
        
        if success:
            print("🎉 Point-in-Time Bronze Layer 완료!")
            return 0
        else:
            print("❌ Point-in-Time Bronze Layer 실패!")
            return 1
            
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
