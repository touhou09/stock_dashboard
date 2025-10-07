FROM python:3.12-slim

# [수정] uv가 만드는 .venv가 /opt/app/.venv에 생기도록, 의존성 설치 전에 작업 디렉터리 고정
WORKDIR /opt/app

# uv 설치(변경 없음)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# [수정] 의존성 레이어 캐시 최적화: 의존성 정의만 먼저 복사
COPY pyproject.toml uv.lock ./

# [수정] 잠금 고정 설치: .venv가 /opt/app/.venv에 생성됨
RUN uv sync --frozen --no-dev

# 소스 코드 복사(변경 없음)
COPY src /opt/app/src
COPY *.py /opt/app/

# [수정] 이후 모든 RUN/CMD에서 venv 파이썬을 쓰도록 PATH를 먼저 설정
ENV PATH="/opt/app/.venv/bin:$PATH"

# [수정] 바이트코드 컴파일하여 성능 최적화
RUN uv run python -m compileall -b .
RUN find . -name "*.py" -exec rm {} \;

# [수정] venv의 python을 확실히 사용하도록 "python"으로 지정
CMD ["python", "src/app/main.pyc"]
