# Cloud Run Jobs 설정 파일

이 디렉토리는 Stock Dashboard 프로젝트의 Bronze와 Silver 레이어를 위한 GCP Cloud Run Job 설정 파일들을 포함합니다.

## 📁 파일 구조

```
cloud-run-jobs/
├── bronze-job.yaml              # Bronze Layer 일일 실행 Job
├── silver-job.yaml              # Silver Layer 일일 실행 Job
├── bronze-backfill-job.yaml     # Bronze Layer 백필 Job
├── silver-backfill-job.yaml     # Silver Layer 백필 Job
├── deploy.sh                    # 배포 스크립트
└── README.md                    # 이 파일
```

## 🚀 Job 종류별 설명

### 1. Bronze Layer Jobs

#### `bronze-job.yaml`
- **목적**: 일일 Bronze Layer 데이터 수집
- **실행 모드**: `bronze-full` (가격 + 배당 데이터)
- **리소스**: 2 vCPU, 4GB 메모리
- **타임아웃**: 1시간 (3600초)
- **스케줄**: 매일 오전 2시 (한국시간 오전 11시)

#### `bronze-backfill-job.yaml`
- **목적**: 과거 데이터 백필
- **실행 모드**: `bronze-backfill`
- **리소스**: 2 vCPU, 4GB 메모리
- **타임아웃**: 3시간 (10800초)
- **사용법**: 수동 실행 또는 특정 기간 백필

### 2. Silver Layer Jobs

#### `silver-job.yaml`
- **목적**: 일일 Silver Layer 데이터 처리
- **실행 모드**: `silver`
- **리소스**: 4 vCPU, 8GB 메모리
- **타임아웃**: 2시간 (7200초)
- **스케줄**: 매일 오전 4시 (한국시간 오후 1시)

#### `silver-backfill-job.yaml`
- **목적**: 과거 Silver 데이터 백필
- **실행 모드**: `silver-backfill`
- **리소스**: 4 vCPU, 8GB 메모리
- **타임아웃**: 4시간 (14400초)
- **사용법**: 수동 실행 또는 특정 기간 백필

## 🔧 주요 설정 옵션

### 환경 변수
- `GCS_BUCKET`: Google Cloud Storage 버킷명
- `PYTHONPATH`: Python 경로 설정
- `LOG_LEVEL`: 로깅 레벨 (INFO)
- `BATCH_SIZE`: 배치 처리 크기
- `MAX_RETRIES`: 최대 재시도 횟수

### 리소스 설정
- **Bronze Layer**: 2 vCPU, 4GB 메모리 (가격 데이터 수집용)
- **Silver Layer**: 4 vCPU, 8GB 메모리 (복잡한 계산용)
- **Backfill**: 더 긴 타임아웃과 안정성 우선 설정

### 보안 설정
- Google Cloud 서비스 계정 인증
- GCS 버킷 마운트 (gcsfuse)
- 최소 권한 원칙 적용

## 📋 배포 방법

### 1. 수동 배포
```bash
# Bronze Job 배포
gcloud run jobs replace cloud-run-jobs/bronze-job.yaml --region=us-east1

# Silver Job 배포
gcloud run jobs replace cloud-run-jobs/silver-job.yaml --region=us-east1

# Backfill Jobs 배포
gcloud run jobs replace cloud-run-jobs/bronze-backfill-job.yaml --region=us-east1
gcloud run jobs replace cloud-run-jobs/silver-backfill-job.yaml --region=us-east1
```

### 2. 자동 배포 스크립트
```bash
# 모든 Jobs 배포
./cloud-run-jobs/deploy.sh
```

## 🕐 스케줄링 설정

### Cloud Scheduler 설정 예시
```yaml
# Bronze Layer 스케줄 (매일 오전 2시)
schedule: "0 2 * * *"
timeZone: "Asia/Seoul"

# Silver Layer 스케줄 (매일 오전 4시)
schedule: "0 4 * * *"
timeZone: "Asia/Seoul"
```

## 🔍 모니터링

### 주요 메트릭
- CPU 사용률
- 메모리 사용률
- 실행 시간
- 성공/실패율
- 에러 로그

### 로그 확인
```bash
# Bronze Job 로그 확인
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=stock-dashboard-bronze" --limit=50

# Silver Job 로그 확인
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=stock-dashboard-silver" --limit=50
```

## ⚠️ 주의사항

1. **보안**: 서비스 계정 키는 Secret Manager로 관리 권장
2. **비용**: 리소스 설정은 실제 사용량에 따라 조정
3. **타임아웃**: 데이터 양에 따라 타임아웃 조정 필요
4. **재시도**: 네트워크 오류 시 자동 재시도 설정
5. **백필**: 대용량 백필 시 리소스 모니터링 필요

## 🔄 업데이트 방법

1. YAML 파일 수정
2. `gcloud run jobs replace` 명령으로 배포
3. 로그 확인 및 모니터링
4. 필요시 리소스 조정

## 📞 문제 해결

### 일반적인 문제
- **타임아웃**: 리소스 증가 또는 타임아웃 연장
- **메모리 부족**: 메모리 할당량 증가
- **인증 오류**: 서비스 계정 권한 확인
- **GCS 접근 오류**: 버킷 권한 및 마운트 설정 확인

