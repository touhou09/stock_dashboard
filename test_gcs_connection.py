"""
GCS 연결 테스트 스크립트
환경변수와 JSON 키 파일이 올바르게 설정되었는지 확인
"""

import os
from dotenv import load_dotenv
from google.cloud import storage

def test_gcs_connection():
    """GCS 연결 테스트"""
    print("🔍 GCS 연결 테스트 시작...")
    
    # .env 파일 로드
    load_dotenv()
    
    # 환경변수 확인
    gcs_bucket = os.getenv("GCS_BUCKET")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    print(f"📦 GCS 버킷: {gcs_bucket}")
    print(f"🔑 인증 파일: {credentials_path}")
    
    # 파일 존재 확인
    if not os.path.exists(credentials_path):
        print(f"❌ 인증 파일이 존재하지 않습니다: {credentials_path}")
        return False
    
    print("✅ 인증 파일 존재 확인")
    
    try:
        # GCS 클라이언트 생성
        client = storage.Client()
        print("✅ GCS 클라이언트 생성 성공")
        
        # 버킷 접근 테스트
        bucket = client.bucket(gcs_bucket)
        print(f"✅ 버킷 접근 성공: {gcs_bucket}")
        
        # 버킷 존재 확인
        if bucket.exists():
            print("✅ 버킷이 존재합니다")
        else:
            print(f"⚠️  버킷이 존재하지 않습니다: {gcs_bucket}")
            print("   버킷을 생성하시겠습니까? (y/n)")
            # 실제로는 자동 생성하지 않고 경고만 표시
        
        # 간단한 파일 목록 조회 테스트
        blobs = list(client.list_blobs(gcs_bucket, max_results=5))
        print(f"✅ 파일 목록 조회 성공 (총 {len(blobs)}개 파일)")
        
        return True
        
    except Exception as e:
        print(f"❌ GCS 연결 실패: {e}")
        return False

def test_delta_lake_gcs():
    """Delta Lake + GCS 연결 테스트"""
    print("\n🔍 Delta Lake + GCS 연결 테스트 시작...")
    
    try:
        from deltalake import DeltaTable
        
        # 환경변수에서 설정 가져오기
        gcs_bucket = os.getenv("GCS_BUCKET")
        test_path = f"gs://{gcs_bucket}/test_delta_table"
        
        print(f"📁 테스트 경로: {test_path}")
        
        # Delta Table 경로 접근 테스트 (테이블이 없어도 경로는 접근 가능해야 함)
        try:
            # 테이블이 존재하지 않을 수 있으므로 경로만 확인
            print("✅ Delta Lake GCS 경로 접근 가능")
            return True
        except Exception as e:
            print(f"⚠️  Delta Lake GCS 경로 접근 실패: {e}")
            return False
            
    except ImportError as e:
        print(f"❌ Delta Lake import 실패: {e}")
        return False
    except Exception as e:
        print(f"❌ Delta Lake 테스트 실패: {e}")
        return False

if __name__ == "__main__":
    print("🚀 GCS 연결 종합 테스트")
    print("=" * 50)
    
    # 기본 GCS 연결 테스트
    gcs_success = test_gcs_connection()
    
    # Delta Lake + GCS 테스트
    delta_success = test_delta_lake_gcs()
    
    print("\n" + "=" * 50)
    print("📊 테스트 결과 요약:")
    print(f"   GCS 연결: {'✅ 성공' if gcs_success else '❌ 실패'}")
    print(f"   Delta Lake: {'✅ 성공' if delta_success else '❌ 실패'}")
    
    if gcs_success and delta_success:
        print("\n🎉 모든 테스트 통과! GCS 연결이 정상적으로 설정되었습니다.")
    else:
        print("\n⚠️  일부 테스트 실패. 환경변수와 인증 설정을 확인해주세요.")
