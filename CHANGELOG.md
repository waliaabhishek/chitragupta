# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- Run `uv run git-cliff --config cliff.toml --tag vX.Y.Z --output CHANGELOG.md` locally before tagging a release. -->

## [Unreleased]

### Fixed

- Metrics prefetch failures now produce a distinct `METRICS_FETCH_FAILED` allocation detail instead of being silently conflated with empty data (`NO_USAGE_FOR_ACTIVE_IDENTITIES`). Chargeback rows produced during Prometheus outages are now identifiable and filterable in the database. New `metrics_fetch_failed` field on `AllocationContext` propagates failure state from `_prefetch_metrics` through allocators. (TASK-135)

### Enhanced

- `--validate` CLI flag now validates plugin-specific configs (e.g., CCloud CKU ratios, self-managed Kafka cost model) for each configured tenant, not just top-level `AppSettings`. Previously, plugin config errors were only caught at first pipeline run. (TASK-132)

### Changed

- Restructured deployment examples into self-contained directories under `examples/`. Each example includes a `docker-compose.yml`, `config.yaml`, `.env.example`, and `README.md`. Three examples provided: `ccloud-grafana` (pipeline worker + Grafana, no API server), `ccloud-full` (full stack: pipeline + REST API + Grafana + UI), and `self-managed-full` (full stack for on-prem/self-managed Kafka). Shared Grafana provisioning assets moved to `examples/shared/grafana/provisioning/`. Makefile updated with per-example targets (`example-ccloud-grafana-up/down`, `example-ccloud-full-up/down`, `example-self-managed-up/down`); legacy `docker-up`/`docker-down`/`docker-dev` aliases retained pointing at `ccloud-full`. Stale configs in `deployables/config/examples/` (18 files) removed. (TASK-139)
