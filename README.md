# stock_dashboard

미국 배당주 데이터와 S&P 500 데이터를 처리하는 데이터 파이프라인 프로젝트입니다.

## 프로젝트 구조

```
stock_dashboard/
├── src/
│   ├── app/                    # 애플리케이션 레이어
│   │   ├── main.py            # 메인 실행 파일
│   │   ├── bronze/            # Bronze Layer 실행 파일들
│   │   │   ├── bronze_layer.py
│   │   │   ├── bronze_layer_delta.py
│   │   │   └── bronze_layer_orchestrator.py
│   │   └── silver/            # Silver Layer 실행 파일들
│   │       └── silver_layer_delta.py
│   └── utils/                 # 공통 기능 모듈
│       ├── data_collectors.py # 데이터 수집 함수들
│       ├── data_storage.py    # 데이터 저장 함수들
│       └── data_validators.py # 데이터 검증 함수들
├── tests/                     # 테스트 파일들
├── pyproject.toml            # 프로젝트 설정
└── README.md
```

## 아키텍처

- **Bronze Layer**: 원시 데이터 수집 (Delta Lake)
- **Silver Layer**: 데이터 정제 및 변환
- **Gold Layer**: 분석용 뷰 생성 (BigQuery + DuckDB)

## 설치 및 실행

### uv 설치
```bash
# uv 설치 (macOS)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 또는 pip를 통해
pip install uv
```

### 프로젝트 설정
```bash
# 가상환경 생성 및 의존성 설치
uv sync

# 개발 의존성 포함하여 설치
uv sync --dev
```

### 실행
```bash
# 메인 애플리케이션 실행 (Bronze Layer 전체 수집)
uv run python -m src.app.main --mode bronze-full

# 가격 데이터만 수집
uv run python -m src.app.main --mode bronze-price

# 배당 데이터만 수집
uv run python -m src.app.main --mode bronze-dividend

# Silver Layer 실행
uv run python -m src.app.main --mode silver

# 특정 날짜 데이터 처리
uv run python -m src.app.main --mode bronze-full --date 2024-01-15

# GCS 연결 테스트
uv run python test_gcs_connection.py
```

### 개발 도구
```bash
# 코드 포맷팅
uv run black .

# import 정렬
uv run isort .

# 린팅
uv run flake8 .

# 타입 체킹
uv run mypy .

# 테스트 실행
uv run pytest
```

## Finance Reader 테스트

### 기능
- S&P 500 종목 리스트 자동 수집
- 개별 종목의 가격, 배당, 분할 데이터 수집
- 배당주 분석 및 필터링
- CSV 형태로 데이터 저장

### 의존성 관리
- `pyproject.toml`: 프로젝트 메타데이터 및 의존성 정의
- `uv.lock`: 정확한 버전 잠금 파일 (자동 생성)
- `uv.toml`: uv 설정 파일
