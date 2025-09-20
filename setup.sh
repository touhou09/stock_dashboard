#!/bin/bash

# 프로젝트 설정 스크립트

echo "=== Stock Dashboard 프로젝트 설정 ==="

# uv 설치 확인 및 설치
if ! command -v uv &> /dev/null; then
    echo "uv 설치 중..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.cargo/env
    echo "uv 설치 완료!"
else
    echo "uv가 이미 설치되어 있습니다."
fi

# 의존성 설치
echo "의존성 설치 중..."
uv sync --dev

# 실행 권한 부여
chmod +x run_tests.sh

echo
echo "설정 완료!"
echo
echo "사용 가능한 명령어:"
echo "  ./run_tests.sh          - 모든 테스트 실행"
echo "  uv run python simple_test.py  - 간단한 테스트"
echo "  uv run python test_finance_reader.py  - 전체 테스트"
echo "  uv run pytest           - 단위 테스트"
echo "  uv run black .          - 코드 포맷팅" 