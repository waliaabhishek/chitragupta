## Unreleased

### Added

- Add bounded self-managed Kafka historical Prometheus acquisition with a default
  five-day response/chunk limit, configurable `1..30` day range, exact scope reuse,
  recovery preflight, and documented logical-family versus HTTP-attempt bounds.
- Add opt-in quota-backed principal attribution for self-managed Kafka network
  pools, including fail-closed evidence handling, self-managed-plugin-owned
  scope state and historical team snapshots, and operator configuration
  guidance.
- Add optional self-managed Kafka Prometheus metric and label aliases plus an
  explicit telemetry checker with deterministic JSONL diagnostics and
  warning-only historical gaps.

--8<-- "CHANGELOG.md"
