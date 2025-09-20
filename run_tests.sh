#!/bin/bash

# Finance Reader 테스트 실행 스크립트

echo "=== Stock Dashboard Finance Reader Tests ==="
echo

# uv가 설치되어 있는지 확인
if ! command -v uv &> /dev/null; then
    echo "uv가 설치되어 있지 않습니다. 설치 중..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.cargo/env
fi

# 의존성 설치
echo "의존성 설치 중..."
uv sync

echo
echo "1. 간단한 테스트 실행..."
uv run python simple_test.py

echo
echo "2. 전체 S&P 500 데이터 테스트 실행..."
uv run python test_finance_reader.py

echo
echo "테스트 완료!" 