"""
Stock Dashboard 메인 실행 파일
"""

import os
import sys
from datetime import datetime, timedelta
import argparse
from dotenv import load_dotenv

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, '/opt/app')

# 모듈 import
from bronze_layer_orchestrator import BronzeLayerOrchestrator
from silver_layer_delta import SilverLayerDelta

# .env 파일 로드
load_dotenv()

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="Stock Dashboard 데이터 파이프라인")
    parser.add_argument("--mode", choices=["bronze-price", "bronze-dividend", "bronze-full", "silver"], 
                       default="bronze-full", help="실행 모드")
    parser.add_argument("--date", type=str, help="처리 날짜 (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # GCS 설정
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    
    # 날짜 파싱
    target_date = None
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    
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
            
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
