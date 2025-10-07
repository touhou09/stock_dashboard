#!/bin/bash

# [수정] Stock Dashboard Cloud Run Jobs 배포 스크립트
# 사용법: ./deploy.sh [bronze|silver|backfill|all]

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 프로젝트 설정
PROJECT_ID="stock-dashboard-472700"
REGION="us-east1"
NAMESPACE="240269058578"

# 함수 정의
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# [수정] gcloud 설정 확인
check_gcloud() {
    log_info "gcloud 설정 확인 중..."
    
    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI가 설치되지 않았습니다."
        exit 1
    fi
    
    # 현재 프로젝트 확인
    CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
    if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
        log_warning "현재 프로젝트가 $PROJECT_ID가 아닙니다. ($CURRENT_PROJECT)"
        log_info "프로젝트를 $PROJECT_ID로 설정합니다..."
        gcloud config set project $PROJECT_ID
    fi
    
    log_success "gcloud 설정 완료"
}

# [수정] Docker 이미지 확인
check_docker_image() {
    log_info "Docker 이미지 확인 중..."
    
    IMAGE_NAME="docker.io/touhou09/stockprocessing:v0.0.15"
    
    # 이미지 존재 여부 확인 (간단한 체크)
    if ! docker manifest inspect $IMAGE_NAME &> /dev/null; then
        log_warning "Docker 이미지 $IMAGE_NAME를 찾을 수 없습니다."
        log_info "이미지가 존재하는지 확인해주세요."
    else
        log_success "Docker 이미지 확인 완료: $IMAGE_NAME"
    fi
}

# [수정] Job 배포 함수
deploy_job() {
    local job_file=$1
    local job_name=$2
    
    log_info "$job_name 배포 중..."
    
    if [ ! -f "$job_file" ]; then
        log_error "Job 파일을 찾을 수 없습니다: $job_file"
        return 1
    fi
    
    # Job 배포
    if gcloud run jobs replace "$job_file" --region="$REGION" --quiet; then
        log_success "$job_name 배포 완료"
    else
        log_error "$job_name 배포 실패"
        return 1
    fi
}

# [수정] Bronze Layer Job 배포
deploy_bronze() {
    log_info "Bronze Layer Jobs 배포 시작..."
    
    deploy_job "bronze-job.yaml" "Bronze Layer Job"
    
    log_success "Bronze Layer Jobs 배포 완료"
}

# [수정] Silver Layer Job 배포
deploy_silver() {
    log_info "Silver Layer Jobs 배포 시작..."
    
    deploy_job "silver-job.yaml" "Silver Layer Job"
    
    log_success "Silver Layer Jobs 배포 완료"
}

# [수정] Backfill Jobs 배포
deploy_backfill() {
    log_info "Backfill Jobs 배포 시작..."
    
    deploy_job "bronze-backfill-job.yaml" "Bronze Backfill Job"
    deploy_job "silver-backfill-job.yaml" "Silver Backfill Job"
    
    log_success "Backfill Jobs 배포 완료"
}

# [수정] 모든 Jobs 배포
deploy_all() {
    log_info "모든 Cloud Run Jobs 배포 시작..."
    
    deploy_bronze
    deploy_silver
    deploy_backfill
    
    log_success "모든 Cloud Run Jobs 배포 완료"
}

# [수정] Job 상태 확인
check_jobs() {
    log_info "배포된 Jobs 상태 확인 중..."
    
    echo ""
    echo "=== Bronze Layer Jobs ==="
    gcloud run jobs list --region="$REGION" --filter="metadata.name~bronze" --format="table(metadata.name,status.conditions[0].type,status.conditions[0].status)"
    
    echo ""
    echo "=== Silver Layer Jobs ==="
    gcloud run jobs list --region="$REGION" --filter="metadata.name~silver" --format="table(metadata.name,status.conditions[0].type,status.conditions[0].status)"
    
    echo ""
    echo "=== Backfill Jobs ==="
    gcloud run jobs list --region="$REGION" --filter="metadata.name~backfill" --format="table(metadata.name,status.conditions[0].type,status.conditions[0].status)"
}

# [수정] Job 실행 함수
run_job() {
    local job_name=$1
    local description=$2
    
    log_info "$description 실행 중..."
    
    if gcloud run jobs execute "$job_name" --region="$REGION" --wait; then
        log_success "$description 실행 완료"
    else
        log_error "$description 실행 실패"
        return 1
    fi
}

# [수정] 메인 함수
main() {
    local action=${1:-"all"}
    
    echo "=========================================="
    echo "Stock Dashboard Cloud Run Jobs 배포 스크립트"
    echo "=========================================="
    echo ""
    
    # 사전 체크
    check_gcloud
    check_docker_image
    
    echo ""
    log_info "배포 작업 시작: $action"
    echo ""
    
    case $action in
        "bronze")
            deploy_bronze
            ;;
        "silver")
            deploy_silver
            ;;
        "backfill")
            deploy_backfill
            ;;
        "all")
            deploy_all
            ;;
        "status")
            check_jobs
            ;;
        "run-bronze")
            run_job "stock-dashboard-bronze" "Bronze Layer Job"
            ;;
        "run-silver")
            run_job "stock-dashboard-silver" "Silver Layer Job"
            ;;
        "run-bronze-backfill")
            run_job "stock-dashboard-bronze-backfill" "Bronze Backfill Job"
            ;;
        "run-silver-backfill")
            run_job "stock-dashboard-silver-backfill" "Silver Backfill Job"
            ;;
        *)
            echo "사용법: $0 [bronze|silver|backfill|all|status|run-bronze|run-silver|run-bronze-backfill|run-silver-backfill]"
            echo ""
            echo "옵션:"
            echo "  bronze              - Bronze Layer Job만 배포"
            echo "  silver              - Silver Layer Job만 배포"
            echo "  backfill            - Backfill Jobs만 배포"
            echo "  all                 - 모든 Jobs 배포 (기본값)"
            echo "  status              - 배포된 Jobs 상태 확인"
            echo "  run-bronze          - Bronze Layer Job 실행"
            echo "  run-silver          - Silver Layer Job 실행"
            echo "  run-bronze-backfill - Bronze Backfill Job 실행"
            echo "  run-silver-backfill - Silver Backfill Job 실행"
            exit 1
            ;;
    esac
    
    echo ""
    log_success "작업 완료!"
    
    # 상태 확인 (배포 작업인 경우)
    if [[ "$action" =~ ^(bronze|silver|backfill|all)$ ]]; then
        echo ""
        check_jobs
    fi
}

# [수정] 스크립트 실행
main "$@"
