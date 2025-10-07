#!/usr/bin/env python3
"""
데이터 백필 실행 스크립트
과거 데이터부터 현재까지 모든 데이터를 채우는 백필 실행
"""

import os
import sys
from datetime import datetime, timedelta
import argparse
from dotenv import load_dotenv

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, '/opt/app')

# 모듈 import
from src.app.backfill.backfill_orchestrator import BackfillOrchestrator

# .env 파일 로드
load_dotenv()

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="데이터 백필 실행 스크립트")
    parser.add_argument("--mode", choices=["full", "bronze", "silver", "gold", "incremental", "pit", "setup-membership"], 
                       default="full", help="백필 모드")
    parser.add_argument("--start-date", type=str, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="종료 날짜 (YYYY-MM-DD)")
    parser.add_argument("--days-back", type=int, default=7, help="증분 백필 일수")
    parser.add_argument("--batch-size", type=int, default=50, help="배치 크기")
    parser.add_argument("--skip-gold", action="store_true", help="Gold Layer 건너뛰기")
    
    args = parser.parse_args()
    
    # GCS 설정
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    
    if not gcs_bucket or gcs_bucket == "your-stock-dashboard-bucket":
        print("❌ GCS_BUCKET 환경변수를 설정해주세요.")
        print("예: export GCS_BUCKET=your-actual-bucket-name")
        return 1
    
    print(f"🚀 데이터 백필 시작 - GCS 버킷: {gcs_bucket}")
    
    # 백필 오케스트레이터 초기화
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
            print("🔄 전체 레이어 백필 실행 중...")
            success = orchestrator.run_full_backfill(start_date, end_date, args.batch_size, args.skip_gold)
        elif args.mode == "bronze":
            print("🥉 Bronze Layer 백필 실행 중...")
            success = orchestrator.run_bronze_backfill(start_date, end_date, args.batch_size)
        elif args.mode == "silver":
            print("🥈 Silver Layer 백필 실행 중...")
            success = orchestrator.run_silver_backfill(start_date, end_date)
        elif args.mode == "gold":
            print("🥇 Gold Layer 백필 실행 중...")
            success = orchestrator.run_gold_backfill(start_date, end_date)
        elif args.mode == "incremental":
            print(f"🔄 증분 백필 실행 중... ({args.days_back}일치)")
            success = orchestrator.run_incremental_backfill(args.days_back, args.batch_size)
        elif args.mode == "setup-membership":
            print("📋 멤버십 추적 시스템 설정 중...")
            success = orchestrator.setup_membership_tracking(start_date, end_date, use_manual=True)
        elif args.mode == "pit":
            print("🎯 Point-in-Time 백필 실행 중... (생존 편향 해결)")
            success = orchestrator.run_full_backfill(start_date, end_date, args.batch_size, args.skip_gold, use_pit=True, setup_membership=True)
        
        if success:
            print("🎉 백필 완료!")
            return 0
        else:
            print("❌ 백필 실패!")
            return 1
            
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
