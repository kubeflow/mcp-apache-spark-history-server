# Refer to images by hashes
FROM ghcr.io/astral-sh/uv:0.11.23-python3.12-alpine@sha256:af42a377a30c429320016ce58aaa017cb8f3b2e67999d63b8a39a0cb8d25402d AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

ENV UV_PYTHON_DOWNLOADS=0
ENV UV_COMPILE_BYTECODE=1
ENV UV_CACHE_DIR=/app/.cache

WORKDIR /app

RUN apk add --no-cache git

# Just in case for future external caching mechanisms
RUN --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=.git,target=.git \
    uv sync --frozen --no-install-project --no-dev --no-editable

COPY . /app

# Install project
RUN uv sync --frozen --no-dev --no-editable

FROM ghcr.io/astral-sh/uv:0.11.23-python3.12-alpine@sha256:af42a377a30c429320016ce58aaa017cb8f3b2e67999d63b8a39a0cb8d25402d

# Create app user and group
RUN addgroup app && adduser -G app -D app

COPY --from=builder --chown=app:app /app /app
WORKDIR /app

ENV UV_CACHE_DIR=/app/.cache
ENV PATH="/app/.venv/bin:$PATH"

USER app

CMD ["spark-mcp"]
