"""
Stock Dashboard 메인 실행 파일
"""

import os
import sys
from datetime import datetime, timedelta
import argparse
from dotenv import load_dotenv

# PYTHONPATH 환경변수로 경로 설정 (Dockerfile에서 설정됨)

# 모듈 import (절대 import로 변경)
from src.app.bronze.bronze_layer_orchestrator import BronzeLayerOrchestrator
from src.app.silver.silver_layer_delta import SilverLayerDelta

# .env 파일 로드
load_dotenv()

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="Stock Dashboard 데이터 파이프라인")
    parser.add_argument("--mode", choices=["bronze-price", "bronze-dividend", "bronze-full", "silver", "silver-backfill", "bronze-backfill", "full-backfill", "setup-membership", "pit-backfill"], 
                       default="bronze-full", help="실행 모드")
    parser.add_argument("--date", type=str, help="처리 날짜 (YYYY-MM-DD)")
    parser.add_argument("--start-date", type=str, help="Backfill 시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Backfill 종료 날짜 (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # GCS 설정
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    
    # 날짜 파싱
    target_date = None
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    
    start_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    
    end_date = None
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    
    try:
        if args.mode in ["bronze-price", "bronze-dividend", "bronze-full"]:
            # Bronze Layer 실행
            orchestrator = BronzeLayerOrchestrator(gcs_bucket=gcs_bucket)
            
            if args.mode == "bronze-price":
                orchestrator.run_price_only_collection(target_date)
            elif args.mode == "bronze-dividend":
                orchestrator.run_dividend_only_collection(target_date)
            elif args.mode == "bronze-full":
                orchestrator.run_full_collection(target_date)
                
        elif args.mode == "silver":
            # Silver Layer 실행
            silver_layer = SilverLayerDelta(gcs_bucket=gcs_bucket)
            silver_layer.run_silver_processing(target_date)
            
        elif args.mode == "silver-backfill":
            # Silver Layer Backfill 실행
            silver_layer = SilverLayerDelta(gcs_bucket=gcs_bucket)
            silver_layer.run_silver_backfill(start_date, end_date)
            
        elif args.mode == "bronze-backfill":
            # Bronze Layer Backfill 실행
            from src.app.backfill.backfill_orchestrator import BackfillOrchestrator
            backfill_orchestrator = BackfillOrchestrator(gcs_bucket=gcs_bucket)
            backfill_orchestrator.run_bronze_backfill(start_date, end_date)
            
        elif args.mode == "full-backfill":
            # 전체 레이어 Backfill 실행 (기존 방식 - 생존 편향 있음)
            from src.app.backfill.backfill_orchestrator import BackfillOrchestrator
            backfill_orchestrator = BackfillOrchestrator(gcs_bucket=gcs_bucket)
            backfill_orchestrator.run_full_backfill(start_date, end_date, use_pit=False)
            
        elif args.mode == "setup-membership":
            # 멤버십 추적 시스템 설정
            from src.app.backfill.backfill_orchestrator import BackfillOrchestrator
            backfill_orchestrator = BackfillOrchestrator(gcs_bucket=gcs_bucket)
            backfill_orchestrator.setup_membership_tracking(start_date, end_date, use_manual=True)
            
        elif args.mode == "pit-backfill":
            # Point-in-Time 백필 실행 (생존 편향 해결)
            from src.app.backfill.backfill_orchestrator import BackfillOrchestrator
            backfill_orchestrator = BackfillOrchestrator(gcs_bucket=gcs_bucket)
            backfill_orchestrator.run_full_backfill(start_date, end_date, use_pit=True, setup_membership=True)
            
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
