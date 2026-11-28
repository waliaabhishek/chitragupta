FROM golang:1.24-alpine AS go-tools
ENV CGO_ENABLED=0 GOFLAGS=-p=3
RUN go install github.com/minio/mc@RELEASE.2025-08-13T08-35-41Z

FROM go-tools AS minio-build
RUN go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z

FROM alpine:3.22.8 AS minio
COPY --from=minio-build /go/bin/minio /usr/local/bin/
ENTRYPOINT ["minio", "server", "/data", "--console-address", ":9001"]

FROM python:3.14-slim-bookworm AS checks
WORKDIR /app
COPY --from=go-tools /go/bin/mc /usr/local/bin/
RUN python -m pip install --no-cache-dir uv==0.13.9
COPY pyproject.toml uv.lock ./
RUN uv sync --no-cache --frozen --extra iceberg --no-default-groups --no-install-project \
    && uv pip install --no-cache --no-deps duckdb==2.0.1 duckdb-core-ext-avro==2.0.1 \
        duckdb-core-ext-httpfs==2.0.1 duckdb-core-ext-iceberg==2.0.1 \
    && uv pip install --no-cache pytest==9.0.3
COPY src ./src
ENV PATH="/app/.venv/bin:$PATH" PYTHONPATH=/app/src PYTHONDONTWRITEBYTECODE=1
CMD ["timeout", "-k", "5s", "180s", "python", "-u", "/checks/check.py"]
