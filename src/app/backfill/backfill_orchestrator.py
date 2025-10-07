"""
데이터 백필 오케스트레이터 - Bronze, Silver, Gold Layer 전체 백필 관리
과거 데이터부터 현재까지 모든 데이터를 채우는 통합 백필 시스템
"""

import os
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple
import logging
from dotenv import load_dotenv

from src.app.bronze.bronze_layer_orchestrator import BronzeLayerOrchestrator
from src.app.bronze.bronze_layer_point_in_time import BronzeLayerPointInTime
from src.app.silver.silver_layer_delta import SilverLayerDelta
from src.app.membership.sp500_membership_tracker import SP500MembershipTracker
# Gold Layer는 이미 BigQuery에서 쿼리문으로 구현되어 있음

try:
    load_dotenv()
except Exception:
    pass
logger = logging.getLogger(__name__)

class BackfillOrchestrator:
    """데이터 백필 오케스트레이터 - 전체 레이어 백필 관리"""
    
    def __init__(self, gcs_bucket: str):
        """
        백필 오케스트레이터 초기화
        
        Args:
            gcs_bucket: GCS 버킷 이름
        """
        self.gcs_bucket = gcs_bucket
        
        # 각 레이어 초기화
        self.bronze_orchestrator = BronzeLayerOrchestrator(gcs_bucket)
        self.bronze_pit = BronzeLayerPointInTime(gcs_bucket)  # Point-in-Time Bronze Layer
        self.silver_layer = SilverLayerDelta(gcs_bucket)
        self.membership_tracker = SP500MembershipTracker(gcs_bucket)  # 멤버십 추적기
        # Gold Layer는 이미 BigQuery에서 구현되어 있음
    
    def get_backfill_date_range(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Tuple[date, date]:
        """
        백필 날짜 범위 결정
        
        Args:
            start_date: 시작 날짜 (None이면 2년 전)
            end_date: 종료 날짜 (None이면 어제)
            
        Returns:
            Tuple[date, date]: (시작날짜, 종료날짜)
        """
        if end_date is None:
            end_date = datetime.now().date() - timedelta(days=1)
        
        if start_date is None:
            # 기본값: 2년 전부터 시작
            start_date = end_date - timedelta(days=730)
        
        logger.info(f"📅 백필 날짜 범위: {start_date} ~ {end_date}")
        return start_date, end_date
    
    def generate_date_list(self, start_date: date, end_date: date, include_weekends: bool = False) -> List[date]:
        """
        백필할 날짜 리스트 생성
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            include_weekends: 주말 포함 여부 (False면 평일만)
            
        Returns:
            List[date]: 처리할 날짜 리스트
        """
        date_list = []
        current_date = start_date
        
        while current_date <= end_date:
            if include_weekends or current_date.weekday() < 5:  # 0-4: 월-금
                date_list.append(current_date)
            current_date += timedelta(days=1)
        
        logger.info(f"📊 백필 날짜 리스트 생성: {len(date_list)}개 날짜")
        return date_list
    
    def setup_membership_tracking(self, start_date: date, end_date: date, use_manual: bool = True) -> bool:
        """
        멤버십 추적 시스템 설정
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            use_manual: 수동 데이터 사용 여부
            
        Returns:
            bool: 성공 여부
        """
        logger.info("=" * 80)
        logger.info("📋 S&P 500 멤버십 추적 시스템 설정")
        logger.info("=" * 80)
        logger.info(f" 설정 기간: {start_date} ~ {end_date}")
        logger.info(f" 데이터 소스: {'수동' if use_manual else 'Wikipedia 스크래핑'}")
        
        try:
            self.membership_tracker.run_membership_setup(start_date, end_date, use_manual)
            logger.info("✅ 멤버십 추적 시스템 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ 멤버십 추적 시스템 설정 실패: {e}")
            return False
    
    def run_bronze_backfill(self, start_date: date, end_date: date, batch_size: int = 50, use_pit: bool = False, overwrite: bool = False) -> bool:
        """
        Bronze Layer 백필 실행 - 편입일 기준 백필 지원
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            batch_size: 배치 크기
            use_pit: Point-in-Time 백필 사용 여부 (편입일 기준)
            overwrite: 기존 데이터 덮어쓰기 여부
            
        Returns:
            bool: 성공 여부
        """
        logger.info("=" * 80)
        logger.info("🥉 Bronze Layer 백필 시작")
        logger.info("=" * 80)
        logger.info(f" 백필 기간: {start_date} ~ {end_date}")
        logger.info(f" 배치 크기: {batch_size}개씩 처리")
        logger.info(f" Point-in-Time 모드: {use_pit} (편입일 기준 백필)")
        logger.info(f" 덮어쓰기 모드: {overwrite}")
        
        try:
            if use_pit:
                # Point-in-Time 백필 실행 (편입일 기준, 생존 편향 해결) - 수정된 부분
                success = self.bronze_pit.run_point_in_time_backfill(start_date, end_date, batch_size)
            else:
                # 기존 백필 실행 (생존 편향 있음)
                success = self.bronze_orchestrator.run_bronze_backfill(start_date, end_date, batch_size)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Bronze Layer 백필 실패: {e}")
            return False
    
    def run_silver_backfill(self, start_date: date, end_date: date) -> bool:
        """
        Silver Layer 백필 실행
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            
        Returns:
            bool: 성공 여부
        """
        logger.info("=" * 80)
        logger.info("🥈 Silver Layer 백필 시작")
        logger.info("=" * 80)
        logger.info(f" 백필 기간: {start_date} ~ {end_date}")
        
        try:
            # Silver Layer의 기존 백필 기능 활용
            self.silver_layer.run_silver_backfill(start_date, end_date)
            logger.info("✅ Silver Layer 백필 완료")
            return True
            
        except Exception as e:
            logger.error(f"❌ Silver Layer 백필 실패: {e}")
            return False
    
    def run_gold_backfill(self, start_date: date, end_date: date) -> bool:
        """
        Gold Layer 백필 실행 (BigQuery View는 이미 구현되어 있음)
        
        Args:
            start_date: 시작 날짜
            end_date: 종료 날짜
            
        Returns:
            bool: 성공 여부
        """
        logger.info("=" * 80)
        logger.info("🥇 Gold Layer 백필 확인")
        logger.info("=" * 80)
        logger.info(f" 백필 기간: {start_date} ~ {end_date}")
        
        try:
            # Gold Layer는 이미 BigQuery에서 쿼리문으로 구현되어 있으므로
            # Silver Layer 데이터가 준비되면 자동으로 뷰가 업데이트됨
            logger.info("✅ Gold Layer는 이미 BigQuery에서 구현되어 있음")
            logger.info("✅ Silver Layer 데이터 준비 완료 시 자동으로 뷰 업데이트됨")
            return True
            
        except Exception as e:
            logger.error(f"❌ Gold Layer 확인 실패: {e}")
            return False
    
    def run_full_backfill(self, start_date: Optional[date] = None, end_date: Optional[date] = None, 
                         batch_size: int = 50, skip_gold: bool = False, use_pit: bool = True, 
                         setup_membership: bool = True, use_manual_membership: bool = True) -> bool:
        """
        전체 레이어 백필 실행 (Bronze → Silver → Gold)
        
        Args:
            start_date: 시작 날짜 (None이면 2년 전)
            end_date: 종료 날짜 (None이면 어제)
            batch_size: 배치 크기
            skip_gold: Gold Layer 건너뛰기 여부
            use_pit: Point-in-Time 백필 사용 여부 (생존 편향 해결)
            setup_membership: 멤버십 추적 시스템 설정 여부
            use_manual_membership: 수동 멤버십 데이터 사용 여부
            
        Returns:
            bool: 성공 여부
        """
        # 백필 날짜 범위 결정
        start_date, end_date = self.get_backfill_date_range(start_date, end_date)
        
        logger.info("=" * 80)
        logger.info("🚀 전체 레이어 백필 시작")
        logger.info("=" * 80)
        logger.info(f" 백필 기간: {start_date} ~ {end_date}")
        logger.info(f" 배치 크기: {batch_size}개씩 처리")
        logger.info(f" Point-in-Time 모드: {use_pit}")
        logger.info(f" 멤버십 설정: {setup_membership}")
        logger.info(f" Gold Layer 건너뛰기: {skip_gold}")
        logger.info("=" * 80)
        
        try:
            # 0. 멤버십 추적 시스템 설정 (Point-in-Time 모드인 경우)
            if use_pit and setup_membership:
                logger.info(f"\n0️⃣ 멤버십 추적 시스템 설정...")
                membership_success = self.setup_membership_tracking(start_date, end_date, use_manual_membership)
                
                if not membership_success:
                    logger.error("❌ 멤버십 추적 시스템 설정 실패로 백필 중단")
                    return False
            
            # 1. Bronze Layer 백필
            logger.info(f"\n1️⃣ Bronze Layer 백필 실행...")
            bronze_success = self.run_bronze_backfill(start_date, end_date, batch_size, use_pit)
            
            if not bronze_success:
                logger.error("❌ Bronze Layer 백필 실패로 전체 백필 중단")
                return False
            
            # 2. Silver Layer 백필
            logger.info(f"\n2️⃣ Silver Layer 백필 실행...")
            silver_success = self.run_silver_backfill(start_date, end_date)
            
            if not silver_success:
                logger.error("❌ Silver Layer 백필 실패로 전체 백필 중단")
                return False
            
            # 3. Gold Layer 백필 (선택적)
            if not skip_gold:
                logger.info(f"\n3️⃣ Gold Layer 백필 실행...")
                gold_success = self.run_gold_backfill(start_date, end_date)
                
                if not gold_success:
                    logger.warning("⚠️ Gold Layer 백필 실패 (계속 진행)")
            else:
                logger.info(f"\n3️⃣ Gold Layer 백필 건너뛰기")
                gold_success = True
            
            # 4. 최종 결과 요약
            logger.info("\n" + "=" * 80)
            logger.info("🎉 전체 레이어 백필 결과 요약")
            logger.info("=" * 80)
            logger.info(f" 백필 기간: {start_date} ~ {end_date}")
            logger.info(f"✅ Bronze Layer: {'성공' if bronze_success else '실패'}")
            logger.info(f"✅ Silver Layer: {'성공' if silver_success else '실패'}")
            logger.info(f"✅ Gold Layer: {'성공' if gold_success else '실패' if not skip_gold else '건너뜀'}")
            logger.info("=" * 80)
            
            return bronze_success and silver_success and gold_success
            
        except Exception as e:
            logger.error(f"❌ 전체 백필 실패: {e}")
            return False
    
    
    def run_incremental_backfill(self, days_back: int = 7, batch_size: int = 50) -> bool:
        """
        증분 백필 실행 (최근 N일치만 백필)
        
        Args:
            days_back: 백필할 일수
            batch_size: 배치 크기
            
        Returns:
            bool: 성공 여부
        """
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=days_back - 1)
        
        logger.info(f"🔄 증분 백필 실행: {days_back}일치 ({start_date} ~ {end_date})")
        
        return self.run_full_backfill(start_date, end_date, batch_size, skip_gold=True)
    

def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="데이터 백필 오케스트레이터")
    parser.add_argument("--mode", choices=["full", "bronze", "silver", "gold", "incremental"], 
                       default="full", help="백필 모드")
    parser.add_argument("--start-date", type=str, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--days-back", type=int, default=7, help="증분 백필 일수")
    parser.add_argument("--batch-size", type=int, default=50, help="배치 크기")
    parser.add_argument("--skip-gold", action="store_true", help="Gold Layer 건너뛰기")
    
    args = parser.parse_args()
    
    # GCS 설정
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    orchestrator = BackfillOrchestrator(gcs_bucket)
    
    # 날짜 파싱
    start_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    
    end_date = None
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    try:
        if args.mode == "full":
            success = orchestrator.run_full_backfill(start_date, end_date, args.batch_size, args.skip_gold)
        elif args.mode == "bronze":
            success = orchestrator.run_bronze_backfill(start_date, end_date, args.batch_size)
        elif args.mode == "silver":
            success = orchestrator.run_silver_backfill(start_date, end_date)
        elif args.mode == "gold":
            success = orchestrator.run_gold_backfill(start_date, end_date)
        elif args.mode == "incremental":
            success = orchestrator.run_incremental_backfill(args.days_back, args.batch_size)
        
        if success:
            logger.info("🎉 백필 완료!")
        else:
            logger.error("❌ 백필 실패!")
            
    except Exception as e:
        logger.error(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
