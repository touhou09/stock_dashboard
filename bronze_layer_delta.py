"""
Bronze Layer Delta Lake 기반 관리 클래스 - 원천 데이터만 저장
기존 파일을 새로운 모듈 구조로 리팩토링
"""

# 새로운 모듈 구조 import
from bronze_layer import BronzeLayer

# 기존 클래스명과의 호환성을 위한 별칭
BronzeLayerDelta = BronzeLayer

def main():
    """메인 실행 함수"""
    import os
    from dotenv import load_dotenv
    
    # .env 파일 로드
    load_dotenv()
    
    # GCS 설정 (환경변수에서 가져오기)
    gcs_bucket = os.getenv("GCS_BUCKET", "your-stock-dashboard-bucket")
    
    bronze_layer = BronzeLayerDelta(gcs_bucket=gcs_bucket)
    
    try:
        # Bronze Layer 원천 데이터 수집 실행
        bronze_layer.run_daily_collection()
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        raise

if __name__ == "__main__":
    main()
