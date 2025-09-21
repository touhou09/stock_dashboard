# Stock Dashboard 프로젝트 문서

## 현재 구현 현황 (2025년 1월 기준)

### ✅ 완료된 구현

#### Bronze Layer 구현
- **파일**: `bronze_layer.py`
- **기술 스택**: Delta Lake + PyArrow + Google Cloud Storage
- **구현 기능**:
  - Wikipedia 기반 S&P 500 종목 리스트 자동 수집
  - yfinance API를 통한 일일 가격 데이터 수집 (OHLCV)
  - 배당 정보 수집 (배당률, 배당주 여부, 회사 정보)
  - GCS Delta Table 형태로 데이터 저장
  - ACID 트랜잭션, 타임트래블, 스키마 진화 지원
  - 403 오류 우회 및 재시도 메커니즘
  - API 제한 고려한 딜레이 처리

#### Silver Layer 구현
- **파일**: `silver_layer.py`
- **기술 스택**: Delta Lake + PyArrow + Google Cloud Storage
- **구현 기능**:
  - Bronze Layer Delta Table 데이터 로드 및 통합
  - 배당주 필터링 및 통합 테이블 생성
  - 데이터 품질 검증 및 결측값 처리
  - pandas 기반 배당주 분석 (섹터별 분포, 배당률 통계)
  - GCS Delta Table 형태로 정제된 데이터 저장
  - 파티셔닝을 통한 성능 최적화

#### 테스트 스크립트
- **파일**: `test_bronze_layer.py`, `test_silver_layer.py`, `test_silver_simple.py`
- **기능**: 
  - Bronze Layer 전체 데이터 수집 테스트
  - Silver Layer 데이터 정제 및 분석 테스트
  - 간단한 CSV 기반 테스트

### 현재 파일 구조
stock_dashboard/
├── bronze_layer.py # Bronze Layer 구현 (Delta Lake + GCS)
├── silver_layer.py # Silver Layer 구현 (Delta Lake + GCS)
├── test_bronze_layer.py # Bronze Layer 테스트 (CSV 기반)
├── test_silver_layer.py # Silver Layer 테스트 (CSV 기반)
├── test_silver_simple.py # 간단한 Silver Layer 테스트
├── edition.md # 프로젝트 문서
├── README.md # 프로젝트 개요
├── pyproject.toml # 프로젝트 설정
├── requirements.txt # 의존성 목록
└── uv.lock # uv 잠금 파일


### 기술 스택 현황
- **데이터 수집**: yfinance, requests, pandas
- **데이터 처리**: Delta Lake, PyArrow, pandas
- **클라우드 저장**: Google Cloud Storage
- **데이터 포맷**: Delta Table (GCS), CSV (로컬 테스트)
- **패키지 관리**: uv (Python 패키지 매니저)

### 📊 구현된 데이터 파이프라인
1. **데이터 수집**: Wikipedia → yfinance → pandas DataFrame
2. **데이터 저장**: pandas DataFrame → Delta Table → GCS
3. **데이터 정제**: GCS Delta Table → pandas → 통합 테이블
4. **데이터 분석**: pandas → 배당주 통계 및 인사이트

### 데이터 스키마 (현재 구현)

#### Bronze Layer (원시 데이터)
**가격 데이터 스키마**
- ticker: 종목코드 (문자열)
- date: 날짜 (날짜형)
- Open, High, Low, Close: 시가, 고가, 저가, 종가 (실수)
- Volume: 거래량 (정수)
- ingestion_timestamp: 수집 시각 (날짜시간)

**배당 정보 스키마**
- ticker: 종목코드 (문자열)
- company_name: 회사명 (문자열)
- sector: 섹터 (문자열)
- has_dividend: 배당주 여부 (불린)
- dividend_yield: 배당률 소수 (실수)
- dividend_yield_percent: 배당률 퍼센트 (실수)
- dividend_rate: 배당금액 (실수)
- ex_dividend_date: 배당락일 (날짜)
- payment_date: 배당지급일 (날짜)
- dividend_frequency: 배당주기 (문자열)
- market_cap: 시가총액 (정수)
- last_price: 현재가 (실수)
- ingestion_timestamp: 수집 시각 (날짜시간)

#### Silver Layer (정제된 데이터)
**통합 테이블 스키마**
- ticker: 종목코드 (문자열)
- company_name: 회사명 (문자열)
- date: 날짜 (날짜형)
- Open, High, Low, Close: 시가, 고가, 저가, 종가 (실수)
- Volume: 거래량 (정수)
- sector: 섹터 (문자열)
- dividend_yield_percent: 배당률 퍼센트 (실수)
- is_dividend_stock: 배당주 여부 (불린)
- processing_timestamp: 처리 시각 (날짜시간)

### 🔄 현재 데이터 흐름
1. **수집**: Wikipedia → S&P 500 종목 리스트 → yfinance API
2. **저장**: pandas DataFrame → Delta Table → GCS Bronze Layer
3. **정제**: GCS Delta Table → pandas → 통합 테이블 → GCS Silver Layer
4. **분석**: pandas → 배당주 통계 및 인사이트

### ✨ Delta Lake 장점
1. **ACID 트랜잭션**: 데이터 일관성 보장
2. **타임트래블**: 과거 데이터 버전 조회 가능
3. **스키마 진화**: 스키마 변경 시 자동 처리
4. **파티셔닝**: 성능 최적화를 위한 자동 파티셔닝
5. **메타데이터 관리**: 자동 메타데이터 추적 및 관리

### ⚠️ 현재 한계점
1. **배치 처리만**: 실시간 스트리밍 처리 미구현
2. **Gold Layer 미구현**: BigQuery BigLake View 아직 구현 안됨
3. **모니터링 부족**: 데이터 품질 모니터링 및 알림 시스템 미구현

---

## 🏗️ Bronze Layer (Raw Data)
- **SNP500 전체 종목 데이터의 Delta Table**: 모든 SNP500 종목의 OHLCV 데이터를 매일 수집하여 누적
- **미국 배당주 데이터의 Delta Table**: 배당 이력, 배당 수익률, 배당 성장률 등

### 데이터 수집 방식
- **수집 주기**: 매일 1회 배치 형태로 수집
- **SNP500 데이터**: INSERT 방식 (매일 모든 종목의 하루치 데이터를 추가)
- **배당주 데이터**: UPDATE 방식 (배당주 정보 변경 가능성)

### 검증된 데이터 소스 및 접근 방법
- **주요 데이터 소스**: yfinance (Yahoo Finance API)
- **대체 데이터 소스**: Wikipedia, SlickCharts, FinanceDataReader
- **데이터 형태**: pandas DataFrame
- **인덱스**: DatetimeIndex (날짜별)
- **컬럼**: Open, High, Low, Close, Volume, Adj Close
- **데이터 타입**: float64 (가격), int64 (거래량)

## 🏗️ Silver Layer (Cleaned Data)
- **통합 테이블**: SNP500 데이터 중 배당주 관련 정보를 필터링하여 저장
- **저장 방식**: Delta Table 형태로 저장
- **필터링 기준**: SNP500 데이터 중 배당주인 데이터만 선별

## 🏗️ Gold Layer (Analytics)
- **BigQuery BigLake**를 활용한 View 기반 처리
- **저장 방식**: 버킷 저장 대신 BigQuery BigLake View 형태로 구현

---

### Phase 1: Bronze Layer 구축
- [x] **완료**: Delta Lake + GCS 기반 SNP500 원시 데이터 저장
- [x] **완료**: 배당 데이터 수집 및 저장 파이프라인 구축
- [x] **완료**: 403 오류 우회 및 재시도 메커니즘 구현
- [x] **완료**: ACID 트랜잭션 및 타임트래블 지원
- [ ] 매일 배치 수집 스케줄링 구현
- [ ] 데이터 품질 검증 자동화

### Phase 2: Silver Layer 구축
- [x] **완료**: SNP500 데이터 중 배당주 필터링 로직 구현
- [x] **완료**: 배당주 통합 테이블 생성 (Delta Table)
- [x] **완료**: 배당 유무 플래그 및 배당 수익률 계산
- [x] **완료**: 데이터 정제 및 변환 로직 구현
- [x] **완료**: 파티셔닝을 통한 성능 최적화

### Phase 3: Gold Layer 구축
- [ ] BigQuery BigLake View 생성
- [ ] 실시간 분석 및 모니터링 대시보드
- [ ] View 기반 쿼리 최적화

## 핵심 인사이트

1. **데이터 품질**: yfinance를 통한 데이터 수집이 안정적이고 일관성 있음
2. **확장성**: 현재 구조로 SNP500 전체 확장 가능 (503개 종목 검증)
3. **배당 통합**: 기존 가격 데이터에 배당 정보 통합 시 추가 컬럼만 필요
4. **성능**: Delta Lake 파티셔닝으로 실시간 분석 가능
5. **비용 효율성**: BigQuery BigLake 활용으로 비용 최적화 가능
6. **배치 처리**: 매일 1회 수집으로 안정적인 데이터 파이프라인 구축
7. **견고성**: 다중 대체 소스와 재시도 메커니즘으로 안정성 확보
8. **데이터 무결성**: Delta Lake ACID 트랜잭션으로 데이터 일관성 보장

## 📊 샘플 데이터 형태

### SNP500 가격 데이터 예시
날짜별 OHLCV 데이터가 포함된 CSV 형태로 저장되며, 각 종목별로 시가, 고가, 저가, 종가, 거래량 정보를 포함합니다.

### Delta Table 구조 (Silver Layer)
통합 주식 데이터 테이블은 ticker, company_name, date, OHLCV 데이터, sector, dividend_yield_percent, is_dividend_stock, processing_timestamp 컬럼으로 구성되며, date와 is_dividend_stock으로 파티셔닝됩니다.

### Delta Lake 쿼리 기능
- **타임트래블**: 과거 특정 시점의 데이터 조회 가능
- **배당수익률 분석**: 상위 종목 및 섹터별 분포 조회
- **ACID 트랜잭션**: 데이터 일관성 보장된 쿼리 실행

## 다음 구현 우선순위

1. **단기 목표**: Gold Layer BigQuery BigLake View 기반 분석 시스템
2. **중기 목표**: 실시간 모니터링 및 알림 시스템
3. **장기 목표**: ML 파이프라인 및 예측 모델 구축