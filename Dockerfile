# syntax=docker/dockerfile:1.6

# =============================================================================
# Stage 1: Builder — copy uv from official image, then install deps
# =============================================================================
ARG UV_VERSION=0.6.6
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:3.14-slim-bookworm AS builder

WORKDIR /app

# Copy uv binary from official image (no curl/wget needed)
COPY --from=uv /uv /usr/local/bin/uv

# Version injected from git tag at build time (hatch-vcs fallback for no .git)
ARG APP_VERSION=""

# Copy dependency definitions and source
COPY pyproject.toml uv.lock ./
COPY src/ ./src/

# Create venv, install deps AND local project
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${APP_VERSION:-0.0.0.dev0}
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv /app/.venv && \
    uv sync --frozen

# =============================================================================
# Stage 2: Runtime — minimal python, no uv needed
# =============================================================================
FROM python:3.14-slim-bookworm

WORKDIR /app

# Create non-root user
RUN useradd --create-home --uid 1000 appuser

# Environment
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

# Copy venv + source from builder
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appuser /app/src /app/src

# Pre-create data dir so Docker named-volume copy-up inherits appuser ownership
RUN mkdir -p /app/data && chown appuser:appuser /app/data

USER appuser

ENTRYPOINT ["python", "-m", "main"]
