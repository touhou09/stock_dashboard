FROM python:3.12-slim

# uv 설치
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# uv 설정 파일 복사
COPY pyproject.toml uv.lock ./

# 의존성 설치
RUN uv sync --frozen --no-dev

# 소스 코드 복사
COPY src /opt/app/src
COPY *.py /opt/app/

WORKDIR /opt/app

# Python 파일들을 .pyc로 컴파일
RUN uv run python -m compileall -b .
RUN find . -name "*.py" -exec rm {} \;

# 환경변수 설정
ENV PATH="/opt/app/.venv/bin:$PATH"

CMD ["python3", "src/app/main.pyc"]