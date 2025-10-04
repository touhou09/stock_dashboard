# Stock Dashboard - Medallion Architecture 기반 배당주 데이터 파이프라인

미국 배당주 데이터와 S&P 500 데이터를 처리하는 확장 가능한 데이터 파이프라인 프로젝트입니다. Medallion Architecture 패턴을 적용하여 Bronze → Silver → Gold 레이어로 구성되어 있습니다.

## 🏗️ 아키텍처 개요

### Medallion Architecture
- **Bronze Layer**: 원시 데이터 수집 및 저장 (Delta Lake)
- **Silver Layer**: 데이터 정제 및 배당 지표 계산 (Delta Lake)
- **Gold Layer**: 분석용 뷰 생성 (BigQuery View - 예정)

### 기술 스택
- **언어**: Python 3.12
- **패키지 관리**: uv
- **데이터 저장**: Delta Lake (Google Cloud Storage)
- **데이터 수집**: yfinance, Wikipedia API
- **데이터 처리**: pandas, pyarrow
- **배포**: Docker, Cloud Run

## 📁 프로젝트 구조

```
stock_dashboard/
├── src/
│   ├── app/                           # 애플리케이션 레이어
│   │   ├── main.py                   # 메인 실행 파일
│   │   ├── bronze/                   # Bronze Layer
│   │   │   ├── bronze_layer_delta.py # Bronze Layer 핵심 로직
│   │   │   └── bronze_layer_orchestrator.py # Bronze Layer 오케스트레이션
│   │   └── silver/                   # Silver Layer
│   │       └── silver_layer_delta.py # Silver Layer 핵심 로직
│   └── utils/                        # 공통 기능 모듈
│       ├── data_collectors.py        # 데이터 수집 함수들
│       ├── data_storage.py           # Delta Lake 저장 함수들
│       └── data_validators.py        # 데이터 검증 함수들
├── tests/                            # 테스트 파일들
│   ├── test_stock_dashboard.py       # 통합 테스트
│   └── conftest.py                   # 테스트 설정
├── pyproject.toml                    # 프로젝트 설정
├── dockerfile                        # Docker 설정
└── README.md
```

## 📊 데이터 파이프라인 스키마

### 🥉 Bronze Layer (원시 데이터)

#### 1. Bronze 가격 데이터 (`bronze_price_daily`)
- **파티션**: `date={수집일}` (예: `date=2025-09-29`)
- **스키마**:
  - `date`: 수집일 (date)
  - `ticker`: 종목 코드 (string)
  - `open`: 시가 (double)
  - `high`: 고가 (double)
  - `low`: 저가 (double)
  - `close`: 종가 (double)
  - `volume`: 거래량 (long)
  - `adj_close`: 수정종가 (double)
  - `ingest_at`: 수집 시각 (timestamp)

#### 2. Bronze 배당 이벤트 (`bronze_dividend_events`)
- **파티션**: `date={수집일}` (예: `date=2025-09-29`)
- **스키마**:
  - `ex_date`: 배당 지급일 (date) - 실제 배당이 지급된 날짜
  - `ticker`: 종목 코드 (string)
  - `amount`: 배당금액 (double)
  - `date`: 수집일 (date) - 데이터를 수집한 날짜
  - `ingest_at`: 수집 시각 (timestamp)

### 🥈 Silver Layer (정제된 데이터)

#### 3. Silver 배당 지표 (`silver_dividend_metrics_daily`)
- **파티션**: `date={수집일}` (예: `date=2025-09-29`)
- **스키마**:
  - `date`: 수집일 (date)
  - `ticker`: 종목 코드 (string)
  - `last_price`: 최근 주가 (double)
  - `market_cap`: 시가총액 (long) - 현재 0으로 설정
  - `dividend_ttm`: TTM 배당금 (double) - 최근 12개월 배당 총액
  - `dividend_yield_ttm`: TTM 배당수익률 (double) - (TTM 배당금 / 주가) × 100
  - `div_count_1y`: 연간 배당 횟수 (long)
  - `last_div_date`: 최근 배당일 (date)
  - `updated_at`: 업데이트 시각 (timestamp)

### 🔑 주요 특징
- **통일된 파티션 구조**: 모든 테이블이 `date={수집일}` 형태로 파티셔닝
- **배당 이벤트 테이블**: `ex_date`(배당 지급일)와 `date`(수집일) 두 컬럼 모두 포함
- **압축 최적화**: ZSTD 압축 적용으로 저장 공간 효율성 향상
- **자동 최적화**: Delta Lake의 autoOptimize 기능으로 성능 최적화

### 🥇 Gold Layer (분석/집계용 View)

#### 1. 가격 타임시리즈 베이스 (`vw_price_timeseries_base`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_price_timeseries_base`
* **윈도우(기본)**: 최근 3년
* **설명**: 일별 가격 시계열의 표준 형태. 전일 대비 수익률(종가/조정종가 기준)을 포함해 다른 뷰의 공용 베이스로 사용.
* **스키마**:

  * `date` (DATE): 거래일
  * `ticker` (STRING): 종목 코드
  * `open` (FLOAT64): 시가
  * `high` (FLOAT64): 고가
  * `low` (FLOAT64): 저가
  * `close` (FLOAT64): 종가
  * `adj_close` (FLOAT64): 조정종가
  * `volume` (INT64): 거래량
  * `ingest_at` (TIMESTAMP): 수집 시각
  * `prev_close` (FLOAT64): 전일 종가
  * `prev_adj_close` (FLOAT64): 전일 조정종가
  * `ret_1d_close` (FLOAT64): 전일 대비 수익률(종가 기준)
  * `ret_1d_adj` (FLOAT64): 전일 대비 수익률(조정종가 기준)

---

#### 2. 가격 타임시리즈 확장 (`vw_price_timeseries_enriched`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_price_timeseries_enriched`
* **윈도우(기본)**: 최근 3년
* **설명**: 차트/리서치에 자주 쓰는 이동평균·변동성·드로우다운을 추가한 확장 타임시리즈.
* **스키마**:

  * (베이스와 동일 필드) `date`, `ticker`, `open`, `high`, `low`, `close`, `adj_close`, `volume`, `ingest_at`, `prev_close`, `prev_adj_close`, `ret_1d_close`, `ret_1d_adj`
  * `ma_7` (FLOAT64): 7일 이동평균(종가)
  * `ma_30` (FLOAT64): 30일 이동평균(종가)
  * `ma_120` (FLOAT64): 120일 이동평균(종가)
  * `vol_30d` (FLOAT64): 30일 이동 변동성(일간 수익률 표준편차)
  * `rolling_peak` (FLOAT64): 과거부터 현재까지의 조정종가 최고치
  * `drawdown` (FLOAT64): 현재 조정종가의 피크 대비 낙폭 비율

---

#### 3. 월간 수익률 (`vw_returns_monthly`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_returns_monthly`
* **윈도우(기본)**: 최근 5년
* **설명**: 월초/월말 가격으로 계산한 월간 수익률. 막대/히트맵 차트에 적합.
* **스키마**:

  * `month` (DATE): 월(1일로 절단된 날짜)
  * `ticker` (STRING): 종목 코드
  * `month_open` (FLOAT64): 월의 첫 거래일 종가
  * `month_close` (FLOAT64): 월의 마지막 거래일 종가
  * `ret_1m` (FLOAT64): 월간 수익률 = (month_close - month_open) / month_open

---

#### 4. 분기 수익률 (`vw_returns_quarterly`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_returns_quarterly`
* **윈도우(기본)**: 최근 3년
* **설명**: 분기초/분기말 가격으로 계산한 QoQ 수익률.
* **스키마**:

  * `qtr` (DATE): 분기(해당 분기의 첫 달 1일)
  * `ticker` (STRING): 종목 코드
  * `qtr_open` (FLOAT64): 분기의 첫 거래일 종가
  * `qtr_close` (FLOAT64): 분기의 마지막 거래일 종가
  * `ret_qoq` (FLOAT64): 분기 수익률 = (qtr_close - qtr_open) / qtr_open

---

#### 5. 최신 가격 스냅샷 (`vw_price_latest_snapshot`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_price_latest_snapshot`
* **윈도우(기본)**: 최신 1건(기간 제한 없음)
* **설명**: 각 티커의 **가장 최신** 거래일(+최신 ingest)의 한 행만 제공. 대시보드 카드/랭킹 소스.
* **스키마**:

  * `date` (DATE): 최신 거래일
  * `ticker` (STRING): 종목 코드
  * `open`, `high`, `low`, `close`, `adj_close` (FLOAT64)
  * `volume` (INT64)
  * `ingest_at` (TIMESTAMP)

---

#### 6. 배당 지표 최신 스냅샷 (`vw_dividend_metrics_latest`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_dividend_metrics_latest`
* **윈도우(기본)**: 최신 1건(메트릭/가격 모두)
* **설명**: `silver_dividend_metrics_daily` 최신 행에 **최신 가격 스냅샷**을 조인한 요약 뷰.
* **스키마**:

  * `ticker` (STRING)
  * `metrics_date` (DATE): 메트릭 기준일
  * `metrics_last_price` (FLOAT64): 메트릭 산출 시점의 최근 주가
  * `market_cap` (INT64): 시가총액(현재 0일 수 있음)
  * `dividend_ttm` (FLOAT64): 최근 12개월 배당 총액
  * `dividend_yield_ttm` (FLOAT64): TTM 배당수익률(%)
  * `div_count_1y` (INT64): 연간 배당 횟수
  * `last_div_date` (DATE): 최근 배당일
  * `updated_at` (TIMESTAMP): 메트릭 업데이트 시각
  * `price_date` (DATE): 최신 가격 기준일
  * `latest_close` (FLOAT64): 최신 종가
  * `latest_adj_close` (FLOAT64): 최신 조정종가

---

#### 7. 배당 캘린더(2년) (`vw_dividend_calendar_2y`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_dividend_calendar_2y`
* **윈도우(기본)**: 최근 2년
* **설명**: 최근 2년의 배당 이벤트 일정(테이블/캘린더 위젯용).
* **스키마**:

  * `ex_date` (DATE): 배당 지급일(실제 지급 기준일)
  * `ticker` (STRING)
  * `amount` (FLOAT64): 배당금액(주당)
  * `collect_date` (DATE): 수집일(원본의 `date`)
  * `ingest_at` (TIMESTAMP): 수집 시각

---

#### 8. 배당수익률 랭킹(일자별, 2년) (`vw_dividend_yield_rank_daily`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_dividend_yield_rank_daily`
* **윈도우(기본)**: 최근 2년
* **설명**: 날짜별 배당수익률 내림차순 랭킹. Top-N 필터/랭킹 테이블에 바로 사용.
* **스키마**:

  * `date` (DATE)
  * `ticker` (STRING)
  * `last_price` (FLOAT64)
  * `dividend_ttm` (FLOAT64)
  * `dividend_yield_ttm` (FLOAT64)
  * `div_count_1y` (INT64)
  * `last_div_date` (DATE)
  * `updated_at` (TIMESTAMP)
  * `yield_rank` (INT64): 일자 내 배당수익률 순위(1=최상)

---

#### 9. 시장 요약(2년) (`vw_market_daily_summary`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_market_daily_summary`
* **윈도우(기본)**: 최근 2년
* **설명**: 시장의 광의적 브레드스(상승/하락/보합 수, 평균 수익률, 총 거래량) 일자 요약.
* **스키마**:

  * `date` (DATE)
  * `up_cnt` (INT64): 상승 종목 수(일간 수익률 > 0)
  * `down_cnt` (INT64): 하락 종목 수(일간 수익률 < 0)
  * `flat_cnt` (INT64): 보합 종목 수(일간 수익률 = 0)
  * `avg_ret_1d` (FLOAT64): 평균 일간 수익률
  * `total_volume` (INT64): 총 거래량

---

#### 10. 일자별 Top Movers(180일) (`vw_top_movers_daily`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_top_movers_daily`
* **윈도우(기본)**: 최근 180일
* **설명**: 날짜별 수익률 상/하위 종목을 빠르게 선별하기 위한 랭킹 전처리.
* **스키마**:

  * `date` (DATE)
  * `ticker` (STRING)
  * `close` (FLOAT64)
  * `adj_close` (FLOAT64)
  * `ret_1d_adj` (FLOAT64): 일간 수익률(조정종가 기준)
  * `rn_desc` (INT64): 일자 내 **상승** 순위(1=최상승)
  * `rn_asc` (INT64): 일자 내 **하락** 순위(1=최하락)

---

#### 11. 팩터: 배당수익률 분위수 vs 전방 1M 수익률(3년) (`vw_factor_dividend_vs_fwd_return`)

* **경로**: `stock-dashboard-472700.bronze_dividend_events.vw_factor_dividend_vs_fwd_return`
* **윈도우(기본)**: 최근 3년
* **설명**: 배당수익률을 일자별 5분위로 나누고(1=고배당), 21영업일 전방 수익률 평균을 비교하는 팩터 스터디용 집계.
* **스키마**:

  * `date` (DATE)
  * `dy_quintile` (INT64): 배당수익률 분위수(1~5, 1이 높은 그룹)
  * `avg_fwd_ret_21` (FLOAT64): 전방 21영업일 평균 수익률
  * `n` (INT64): 각 분위수의 표본 종목 수


## 🚀 설치 및 실행

### 1. uv 설치
```bash
# uv 설치 (Linux/macOS)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 또는 pip를 통해
pip install uv
```

### 2. 프로젝트 설정
```bash
# 가상환경 생성 및 의존성 설치
uv sync

# 개발 의존성 포함하여 설치
uv sync --dev
```

### 3. 환경 변수 설정
```bash
# Google Cloud 인증 설정
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

### 4. 실행 명령어

#### Bronze Layer (원시 데이터 수집)
```bash
# Bronze Layer 전체 수집 (가격 + 배당)
uv run python -m src.app.main --mode bronze-full --date 2025-09-29

# 가격 데이터만 수집
uv run python -m src.app.main --mode bronze-price --date 2025-09-29

# 배당 데이터만 수집
uv run python -m src.app.main --mode bronze-dividend --date 2025-09-29
```

#### Silver Layer (정제된 데이터 생성)
```bash
# Silver Layer 실행 (배당 지표 계산)
uv run python -m src.app.main --mode silver --date 2025-09-29
```

## 🧪 개발 도구

### 코드 품질 관리
```bash
# 코드 포맷팅
uv run black .

# import 정렬
uv run isort .

# 린팅
uv run flake8 .

# 타입 체킹
uv run mypy .
```

### 테스트
```bash
# 전체 테스트 실행
uv run pytest

# 특정 테스트 파일 실행
uv run pytest tests/test_stock_dashboard.py -v

# 테스트 커버리지 확인
uv run pytest --cov=src
```

## 📈 데이터 수집 현황

### 현재 수집 데이터
- **S&P 500 종목**: 503개 종목
- **가격 데이터**: 일별 OHLCV 데이터
- **배당 데이터**: 배당 지급일, 배당금액 정보
- **수집 주기**: 수동 실행 (향후 스케줄링 예정)

### 배당 지표 계산
- **TTM 배당수익률**: 최근 12개월 배당 총액 기준
- **배당 횟수**: 연간 배당 지급 횟수
- **최근 배당일**: 가장 최근 배당 지급일

## 🐳 Docker 배포

### Docker 이미지 빌드
```bash
# Docker 이미지 빌드
docker build -t stock-dashboard .

# 컨테이너 실행
docker run -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json stock-dashboard
```

### Cloud Run 배포
```bash
# Cloud Run에 배포
gcloud run deploy stock-dashboard \
  --source . \
  --platform managed \
  --region asia-northeast1 \
  --allow-unauthenticated
```

## 🔧 설정 파일

### pyproject.toml
- 프로젝트 메타데이터 및 의존성 정의
- 개발 도구 설정 (black, isort, flake8, mypy, pytest)

### dockerfile
- Python 3.12 기반 Docker 이미지
- uv를 사용한 의존성 관리
- Cloud Run 최적화

## 📝 개발 가이드라인

### 코딩 스타일
- PEP 8 스타일 가이드 준수
- 함수와 클래스에는 명확한 docstring 작성
- 변수명은 snake_case 사용
- 클래스명은 PascalCase 사용

### 테스트 작성
- 각 모듈별 단위 테스트 작성
- 통합 테스트로 전체 파이프라인 검증
- Mock을 활용한 외부 API 테스트
- 테스트 커버리지 80% 이상 유지

### 에러 처리
- 모든 외부 API 호출에 try-catch 적용
- 로깅을 통한 에러 추적
- 재시도 로직 구현
- 데이터 검증 실패 시 명확한 에러 메시지

## 🚧 향후 계획

### Gold Layer 구현
- BigQuery View로 분석용 뷰 생성
- 대시보드용 집계 데이터 준비

### 프론트엔드 개발
- React/Next.js 기반 대시보드
- 배당수익률 차트 및 분석 도구

### 자동화
- Cloud Scheduler를 통한 정기적 데이터 수집
- 데이터 품질 모니터링
- 알림 시스템 구축

## 📞 문의

프로젝트 관련 문의사항이 있으시면 이슈를 생성해 주세요.

---

**Stock Dashboard** - Medallion Architecture 기반 배당주 데이터 파이프라인