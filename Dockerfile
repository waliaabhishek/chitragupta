# syntax=docker/dockerfile:1.6

# =============================================================================
# Stage 1: Builder — install dependencies into venv
# =============================================================================
FROM python:3.14-slim AS builder

ENV UV_LINK_MODE=copy

WORKDIR /app

ARG UV_VERSION=0.10.6

# Install uv with cached wheels
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install "uv==${UV_VERSION}"

# Copy dependency definitions
COPY pyproject.toml uv.lock ./

# Create venv and install dependencies (not local project yet)
RUN uv venv
RUN cp $(command -v uv) .venv/bin/uv
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-install-project --frozen

# =============================================================================
# Stage 2: Runtime — copy venv, add code, install local project
# =============================================================================
FROM python:3.14-slim

ENV UV_LINK_MODE=copy

WORKDIR /app

# Create non-root user
RUN useradd --create-home --uid 1000 appuser

# Environment variables
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app

# Copy venv from builder
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv

# Copy application code
COPY --chown=appuser:appuser pyproject.toml uv.lock ./
COPY --chown=appuser:appuser src/ ./src/

# Prepare cache directories
RUN mkdir -p /home/appuser/.cache/uv /home/appuser/.cache/pip && \
    chown -R appuser:appuser /home/appuser/.cache

USER appuser

# Install local project (fast — deps already installed)
RUN --mount=type=cache,target=/home/appuser/.cache/uv,uid=1000,gid=1000 \
    --mount=type=cache,target=/home/appuser/.cache/pip,uid=1000,gid=1000 \
    uv sync --frozen

ENTRYPOINT ["python", "-m", "src.main"]
