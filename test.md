# Stock Dashboard 테스트 가이드

## 📋 테스트 개요

이 프로젝트는 S&P 500 주식 데이터를 수집하고 처리하는 시스템으로, Bronze Layer와 Silver Layer로 구성된 데이터 파이프라인을 테스트합니다.

## ��️ 테스트 구조

**tests/** 폴더 구조:
- `__init__.py` - 테스트 패키지 초기화
- `conftest.py` - pytest 설정 및 공통 픽스처
- `test_bronze_layer_delta.py` - Bronze Layer 테스트
- `test_silver_layer_delta.py` - Silver Layer 테스트
- `test_integration.py` - 통합 테스트
- `test_utils.py` - 유틸리티 함수 테스트

## 🧪 테스트 파일별 상세 내용

### 1. conftest.py - 공통 픽스처
**주요 픽스처:**
- `sample_sp500_data`: S&P 500 샘플 데이터
- `sample_price_data`: 주가 데이터 픽스처
- `sample_dividend_data`: 배당 데이터 픽스처
- `mock_gcs_bucket`: GCS 버킷 모킹
- `mock_yfinance_ticker`: yfinance Ticker 모킹
- `mock_requests_get`: requests.get 모킹
- `mock_delta_table`: Delta Table 모킹

### 2. test_bronze_layer_delta.py - Bronze Layer 테스트
**주요 테스트 케이스:**
- ✅ **초기화 테스트**: BronzeLayerDelta 클래스 초기화
- ✅ **심볼 변환 테스트**: Yahoo Finance 형식으로 심볼 변환
- ✅ **Wikipedia 데이터 수집**: S&P 500 종목 리스트 수집
- ✅ **일일 데이터 수집**: 주가 데이터 수집 (성공/실패)
- ✅ **배당 정보 수집**: 배당 관련 정보 수집
- ✅ **Delta Table 저장**: 가격/배당 데이터 저장
- ✅ **일일 수집 실행**: 전체 파이프라인 실행

**테스트된 기능:**
- 심볼 변환 예시: BRK.B → BRK-B, BRK.A → BRK-A, AAPL → AAPL
- 데이터 수집 플로우: Wikipedia → S&P 500 리스트 → yfinance → Delta Table

### 3. test_silver_layer_delta.py - Silver Layer 테스트
**주요 테스트 케이스:**
- ✅ **초기화 테스트**: SilverLayerDelta 클래스 초기화
- ✅ **Bronze 데이터 로드**: Delta Table에서 데이터 로드
- ✅ **통합 테이블 생성**: 가격 + 배당 데이터 병합
- ✅ **데이터 저장**: 통합/배당주 테이블 저장
- ✅ **배당주 분석**: 섹터별 분포, 배당수익률 분석
- ✅ **Silver 처리 실행**: 전체 처리 파이프라인

**데이터 품질 검증:**
- 전체 레코드 수 확인
- 배당주 레코드 수 확인
- 결측값 현황 분석
- 데이터 타입 검증

### 4. test_integration.py - 통합 테스트
**주요 테스트 케이스:**
- ✅ **Bronze → Silver 데이터 흐름**: 전체 파이프라인 연동
- ✅ **데이터 품질 검증**: 결측값, 데이터 일관성
- ✅ **에러 처리 및 복구**: 네트워크 오류, API 실패
- ✅ **심볼 정규화 일관성**: 다양한 심볼 형식 처리
- ✅ **대용량 데이터 성능**: 100개 종목 처리 성능

**통합 테스트 시나리오:**
Wikipedia → Bronze Layer → Delta Table → Silver Layer → 분석 결과

### 5. test_utils.py - 유틸리티 테스트
**주요 테스트 케이스:**
- ✅ **데이터 구조 검증**: 필수 컬럼, 데이터 타입
- ✅ **데이터 품질 검증**: 가격 범위, 배당수익률
- ✅ **심볼 정규화 엣지 케이스**: 특수 문자, 빈 문자열
- ✅ **데이터 병합 로직**: LEFT JOIN, 결측값 처리

## �� 테스트 실행 방법

### 1. 의존성 설치
```bash
pip install -r requirements.txt
pip install pytest pytest-mock pytest-cov
```

### 2. 전체 테스트 실행
```bash
# 모든 테스트 실행
pytest

# 상세 출력과 함께 실행
pytest -v

# 커버리지 리포트와 함께 실행
pytest --cov=. --cov-report=html
```

### 3. 특정 테스트 실행
```bash
# 특정 파일 테스트
pytest tests/test_bronze_layer_delta.py

# 특정 클래스 테스트
pytest tests/test_bronze_layer_delta.py::TestBronzeLayerDelta

# 특정 메서드 테스트
pytest tests/test_bronze_layer_delta.py::TestBronzeLayerDelta::test_init
```

### 4. 마커를 사용한 테스트 실행
```bash
# 느린 테스트 제외
pytest -m "not slow"

# 통합 테스트만 실행
pytest -m "integration"
```

## 📊 테스트 커버리지

**주요 테스트 영역:**
- **데이터 수집**: Wikipedia, yfinance API
- **데이터 변환**: 심볼 정규화, 데이터 병합
- **데이터 저장**: Delta Table 저장/로드
- **에러 처리**: 네트워크 오류, API 실패
- **데이터 품질**: 결측값, 데이터 일관성

**모킹된 외부 의존성:**
- Google Cloud Storage
- yfinance API
- Wikipedia API
- Delta Lake

## 🔧 테스트 설정

**pytest.ini 설정:**
```
```

## 해결 방법

### 1. pytest 설치
```bash
# uv를 사용하여 pytest 설치
uv add pytest pytest-mock pytest-cov

# 또는 pip를 사용하여 설치
pip install pytest pytest-mock pytest-cov
```

### 2. 테스트 실행 명령어 수정
```bash
# 올바른 테스트 실행 명령어
uv run pytest tests/

# 또는
uv run python -m pytest tests/

# 또는 직접 pytest 실행
pytest tests/
```

### 3. requirements.txt에 테스트 의존성 추가
```txt
# 기존 의존성에 추가
pytest>=7.0.0
pytest-mock>=3.10.0
pytest-cov>=4.0.0
```

### 4. pyproject.toml에 테스트 의존성 추가 (uv 사용 시)
```toml
<code_block_to_apply_changes_from>
[project]
dependencies = [
    "yfinance>=0.2.18",
    "pandas>=1.5.0",
    "numpy>=1.24.0",
    "requests>=2.28.0",
    "lxml>=4.9.0",
    "html5lib>=1.1",
    "beautifulsoup4>=4.11.0",
    "finance-datareader>=0.9.50",
    "pyarrow>=10.0.0",
    "google-cloud-storage>=2.0.0",
    "deltalake>=0.15.0",
    "pytest>=7.0.0",
    "pytest-mock>=3.10.0",
    "pytest-cov>=4.0.0"
]

[project.optional-dependencies]
test = [
    "pytest>=7.0.0",
    "pytest-mock>=3.10.0",
    "pytest-cov>=4.0.0"
]
```

### 5. 테스트 실행 스크립트 생성
```bash
# test.sh 파일 생성
#!/bin/bash
echo "테스트 실행 중..."
uv run pytest tests/ -v --tb=short
```

### 6. 환경 확인
```bash
# uv 환경에서 pytest 확인
uv run which pytest

# pytest 버전 확인
uv run pytest --version

# 설치된 패키지 확인
uv pip list | grep pytest
```

이렇게 수정하면 `uv run pytest tests/` 명령어가 정상적으로 작동할 것입니다.
