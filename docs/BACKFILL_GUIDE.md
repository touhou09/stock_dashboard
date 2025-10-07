# 📊 백필 가이드 - 생존 편향 해결을 위한 Point-in-Time 백필

## 🎯 개요

현재 2025-09-30일 데이터만 있는 상황에서 과거 데이터부터 현재까지 모든 데이터를 채우는 백필 시스템입니다. **생존 편향(survivorship bias)** 문제를 해결하기 위해 **편입일 기준 백필** 방식을 지원합니다.

## 🚨 생존 편향 문제

### 문제점
- **기존 방식**: 현재 S&P 500 구성 종목만으로 과거 데이터 수집
- **결과**: 퇴출된 종목들이 누락되어 지수 성과가 과대평가됨
- **예시**: NVDA, MSFT 등은 최근에 편입되었지만, 과거 전체 히스토리를 사용하면 부정확

### 해결책
- **Point-in-Time 방식**: 각 종목의 편입일 이후 데이터만 사용
- **멤버십 추적**: S&P 500 편입/퇴출 이력을 정확히 추적
- **시점별 구성**: 각 날짜별로 정확한 구성 종목만 수집

## 🏗️ 백필 모드

### 1. 기존 방식 (생존 편향 있음)
```bash
# 전체 백필 (현재 구성 종목으로 과거 전체 수집)
python -m src.app.main --mode full-backfill --start-date 2023-01-01 --end-date 2024-12-31

# Bronze Layer만 백필
python -m src.app.main --mode bronze-backfill --start-date 2023-01-01 --end-date 2024-12-31
```

### 2. Point-in-Time 방식 (생존 편향 해결) ⭐ **권장**
```bash
# Point-in-Time 전체 백필
python -m src.app.main --mode pit-backfill --start-date 2023-01-01 --end-date 2024-12-31

# 멤버십 시스템 먼저 설정
python -m src.app.main --mode setup-membership --start-date 2023-01-01 --end-date 2024-12-31
```

### 3. 스크립트 사용 (더 편리함)
```bash
# Point-in-Time 백필
python scripts/run_backfill.py --mode pit --start-date 2023-01-01 --end-date 2024-12-31

# 멤버십 설정
python scripts/run_backfill.py --mode setup-membership --start-date 2023-01-01 --end-date 2024-12-31

# 증분 백필 (최근 7일)
python scripts/run_backfill.py --mode incremental --days-back 7
```

## 📋 백필 프로세스

### Point-in-Time 백필 실행 시:

1. **멤버십 추적 시스템 설정** (0단계)
   - S&P 500 편입/퇴출 이력 수집
   - 일자별 멤버십 스냅샷 생성
   - Delta Table에 저장

2. **Bronze Layer 백필** (1단계)
   - 각 날짜별로 정확한 구성 종목 조회
   - 편입일 이후 데이터만 수집
   - 가격/배당 데이터 수집 및 저장

3. **Silver Layer 백필** (2단계)
   - Bronze 데이터를 기반으로 배당 지표 계산
   - TTM 배당수익률, 배당 횟수 등 계산

4. **Gold Layer 확인** (3단계)
   - BigQuery View는 이미 구현되어 있음
   - Silver 데이터 준비 시 자동으로 뷰 업데이트

## 🛠️ 사용 예시

### 2년치 데이터 백필
```bash
# Point-in-Time 방식 (권장)
python scripts/run_backfill.py --mode pit --start-date 2023-01-01 --end-date 2024-12-31

# 기존 방식 (빠르지만 부정확)
python scripts/run_backfill.py --mode full --start-date 2023-01-01 --end-date 2024-12-31
```

### 특정 기간 백필
```bash
# 2024년 1월만 백필
python scripts/run_backfill.py --mode pit --start-date 2024-01-01 --end-date 2024-01-31

# 최근 30일 백필
python scripts/run_backfill.py --mode incremental --days-back 30
```

### 단계별 실행
```bash
# 1단계: 멤버십 설정
python scripts/run_backfill.py --mode setup-membership --start-date 2023-01-01 --end-date 2024-12-31

# 2단계: Bronze Layer만 백필
python -m src.app.main --mode bronze-backfill --start-date 2023-01-01 --end-date 2024-12-31

# 3단계: Silver Layer만 백필
python -m src.app.main --mode silver-backfill --start-date 2023-01-01 --end-date 2024-12-31
```

## 📊 예상 결과

### 2년치 백필 시:
- **약 500개 거래일** × **평균 500개 구성 종목** = **약 25만 건의 가격 데이터**
- **편입/퇴출 이력**: 주요 변경사항 추적
- **배당 지표**: TTM 배당수익률, 배당 횟수, 최근 배당일 등

### 데이터 품질:
- **Point-in-Time**: 시점별 정확한 구성 반영 ✅
- **기존 방식**: 현재 구성으로 과거 수집 ❌

## ⚙️ 고급 옵션

### 배치 크기 조정
```bash
# API 제한 고려하여 배치 크기 조정
python scripts/run_backfill.py --mode pit --batch-size 30 --start-date 2023-01-01 --end-date 2024-12-31
```

### Gold Layer 건너뛰기
```bash
# Bronze + Silver만 백필
python scripts/run_backfill.py --mode pit --skip-gold --start-date 2023-01-01 --end-date 2024-12-31
```

### 환경변수 설정
```bash
# GCS 버킷 설정
export GCS_BUCKET=your-stock-dashboard-bucket

# 백필 실행
python scripts/run_backfill.py --mode pit --start-date 2023-01-01 --end-date 2024-12-31
```

## 🔍 백필 검증

### 데이터 확인
```python
# Bronze Layer 데이터 확인
from deltalake import DeltaTable
bronze_delta = DeltaTable("gs://your-bucket/stock_dashboard/bronze/bronze_price_daily")
bronze_df = bronze_delta.to_pandas()
print(f"Bronze 데이터: {len(bronze_df)}개 레코드")

# Silver Layer 데이터 확인
silver_delta = DeltaTable("gs://your-bucket/stock_dashboard/silver/silver_dividend_metrics_daily")
silver_df = silver_delta.to_pandas()
print(f"Silver 데이터: {len(silver_df)}개 레코드")
```

### 멤버십 확인
```python
# 멤버십 데이터 확인
membership_delta = DeltaTable("gs://your-bucket/stock_dashboard/membership/sp500_membership_daily")
membership_df = membership_delta.to_pandas()
print(f"멤버십 데이터: {len(membership_df)}개 레코드")
```

## 🚀 권장 실행 순서

1. **Point-in-Time 백필** (생존 편향 해결)
   ```bash
   python scripts/run_backfill.py --mode pit --start-date 2023-01-01 --end-date 2024-12-31
   ```

2. **데이터 검증**
   - Bronze/Silver Layer 데이터 확인
   - 멤버십 데이터 확인

3. **BigQuery View 확인**
   - Gold Layer 뷰가 자동으로 업데이트되었는지 확인

4. **분석 시작**
   - 정확한 시점별 데이터로 분석 진행

## ⚠️ 주의사항

- **처리 시간**: Point-in-Time 방식은 더 정확하지만 처리 시간이 오래 걸림
- **API 제한**: yfinance API 제한을 고려하여 배치 크기 조정 필요
- **데이터 품질**: 멤버십 데이터는 수동으로 입력된 주요 변경사항만 포함
- **저장 공간**: 2년치 데이터는 상당한 저장 공간 필요

## 🎯 결론

**Point-in-Time 백필**을 사용하여 생존 편향 문제를 해결하고, 정확한 시점별 S&P 500 데이터를 구축할 수 있습니다. 이를 통해 더 정확한 배당주 분석과 백테스팅이 가능합니다.
