# Define the build argument with default value
ARG PYTHON_VERSION=3.12

FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

ENV UV_PYTHON_DOWNLOADS=0
ENV UV_COMPILE_BYTECODE=1
ENV UV_CACHE_DIR=/app/.cache

WORKDIR /app

RUN apt update && apt install git -y

# Copy entire source code first to ensure .git is available for version detection
COPY . /app

# Install dependencies with version detection (works if .git exists, falls back gracefully if not)
RUN uv sync --frozen --no-dev --no-editable

FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim

# Create app user and group
RUN groupadd -r app && useradd -r -g app app

COPY --from=builder --chown=app:app /app /app
WORKDIR /app

ENV UV_CACHE_DIR=/app/.cache
ENV PATH="/app/.venv/bin:$PATH"

USER app

CMD ["spark-mcp"]
