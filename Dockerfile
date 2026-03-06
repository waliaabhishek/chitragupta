# syntax=docker/dockerfile:1.6

# =============================================================================
# Stage 1: Builder — install uv via standalone installer, then deps
# =============================================================================
FROM python:3.14-slim-bookworm AS builder

WORKDIR /app

# Install uv via standalone installer (faster than pip, no pip needed)
ARG UV_VERSION=0.6.6
ADD https://astral.sh/uv/${UV_VERSION}/install.sh /uv-install.sh
RUN chmod +x /uv-install.sh && /uv-install.sh && rm /uv-install.sh

ENV PATH="/root/.local/bin:$PATH"

# Copy dependency definitions and source
COPY pyproject.toml uv.lock ./
COPY src/ ./src/

# Create venv, install deps AND local project
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
    PYTHONPATH=/app

# Copy venv + source from builder
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appuser /app/src /app/src

USER appuser

ENTRYPOINT ["python", "-m", "src.main"]
