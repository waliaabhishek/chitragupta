# Self-managed Kafka telemetry lab

This local-only lab proves the raw Kafka JMX to JMX Exporter to Prometheus telemetry path under authenticated traffic. It runs two independent single-node Kafka 4.3.1 KRaft clusters with deliberately overlapping names and explicit cluster identity labels. Its opt-in principal demonstrator validates a bounded contract; it is not a production deployment template.

## Prerequisites

- Docker with an accessible daemon
- standalone `docker-compose`
- `uv`
- `curl`
- at least 4 GiB of free memory and 4 GiB of free disk space for images and bounded writable layers

No JMX/RMI port is published to the host. The exporter endpoints bind to loopback ports 7071 and 7072; Prometheus binds to loopback port 9090. Generated credentials, cluster IDs, runtime configuration, and evidence stay below this directory and are ignored by Git.

## Lifecycle

Run commands from this directory:

```bash
./scripts/lab.sh prereq
./scripts/lab.sh start
./scripts/lab.sh ready
./scripts/lab.sh workload status
./scripts/lab.sh validate --window 5m
./scripts/lab.sh validate --window 5m --principal-contract
./scripts/lab.sh evidence --window 5m
./scripts/lab.sh stop
./scripts/lab.sh cleanup
```

Workloads can be controlled independently:

```bash
./scripts/lab.sh workload start
./scripts/lab.sh workload stop
./scripts/lab.sh workload status
```

`start` is idempotent for a running generation: it renders local configuration, starts both clusters and telemetry services, creates topics and quotas with idempotent Kafka commands, and starts continuous producers and consumers. The workload includes distinct user, client-only, and combined user/client quota scopes at low and high steady rates. Both clusters deliberately overlap their generated fixture names to prove that explicit cluster target labels, not names, define scope.

Wait at least five minutes after `ready` before validating a five-minute rate window. Validation is fail-closed: missing metrics or labels, wrong metric types, absent quota scopes, non-positive Produce or Fetch throttling, insufficient low/high rate separation, unhealthy targets, missing cluster selectors, unexpected high-cardinality families, or stale evidence produce exit code 7 and a JSON result path.

Ordinary telemetry `validate` is the canonical complete evidence-producing command. It captures the live telemetry artifacts, validates that same bundle, writes `validator-result.json` into it, and leaves `evidence/latest` pointing to the validated bundle.

## Principal-allocation demonstration

`validate --principal-contract` adds an opt-in, telemetry-only demonstration of the principal-allocation contract. It does not configure or enable production attribution.

The demonstrator uses authenticated Kafka user identity as `User:` plus the exported case-sensitive user label. Produce quota rate supplies ingress evidence and Fetch quota rate supplies egress evidence. Client-only evidence remains an explicit residual; it is never assigned to a user. Principal and topic results are independent marginals, so this command does not infer principal-by-topic or topic-owner allocations.

The logical billing interval remains `[start,end)`. Raw quota inputs use `(start,end]` membership with an actual leading guard bounded by the configured maximum gap. The lab declares its actual 5-second scrape cadence and 10-second maximum gap; production cadence and gaps remain operator/configuration/evidence inputs. Sparse but complete production samples are monitoring-resolution estimates, not byte-exact totals. The capture first proves the exact Prometheus target scope with bounded actual `up` samples. If scope cannot be proven, it writes only `principal-scope-evidence.json` and suppresses quota queries. After a scope pass, it captures only raw Produce and Fetch quota matrices; no lookback value, synthetic timestamp, interpolation, or unbounded hold contributes evidence.

An affected principal direction/cluster-day terminalizes independently; later principal days and the independent topic lane continue. An explicit reprocess replaces that cluster-day from retained corrected evidence using the current mapping.

For each direction, `q_i` is a valid user or user-client quota weight, `c` is a valid client-only weight, and `W=sum(q_i)+c`. The configured monetary pool `M` is allocated independently as `M*q_i/W`; the client-only share `M*c/W` remains unallocated. Invalid, incomplete, or absent evidence is unavailable. Complete `W=0` is zero usage; complete positive `W` is ready without client-only weight and degraded with it. Amounts are Decimal values rounded down to four decimal places, with the remaining fractional currency retained as an explicit rounding residual so users plus unallocated amount balance exactly.

The versioned contract includes one mapped user, one unmapped user, and one
client-only identity in each direction. A successful live demonstration requires
complete ingress and egress evidence for all three. Retries, repeat consumption,
Kafka-recorded failed Produce requests, non-throttled empty/error Fetch responses,
and protocol envelope contribute when Kafka records them in the applicable quota
rate. For production configuration, see the
[Self-Managed Kafka Configuration Reference](../../docs/configuration/self-managed-reference.md#quota-backed-principal-attribution).
After a scope pass, the complete validated and finalized principal bundle contains:

- `principal-window.json`
- `principal-scope-evidence.json`
- `principal-raw-query-results.json`
- `principal-allocation-demonstration.json`
- `validator-result.json`
- `cleanup-result.json` after `cleanup` finalizes it

If contract or scope capture is blocked, the capture writes only `principal-scope-evidence.json` and suppresses the principal window, raw query, and demonstration artifacts; `validate` still records its `validator-result.json`.

Prometheus must retain the requested window, leading guard, calculation delay, and any intended explicit reprocessing horizon; the lab baseline is 14 days. Principal validation failure returns exit 7 and leaves ordinary telemetry validation behavior unchanged.

## Clean restart proof

The cleanup command records the current generation and cleanup result before removing containers, volumes, generated credentials, and runtime configuration. A new start generates different cluster IDs and credentials. Use this complete sequence:

```bash
./scripts/lab.sh cleanup
./scripts/lab.sh start
./scripts/lab.sh ready
./scripts/lab.sh validate --window 5m --require-recreated-state
./scripts/lab.sh cleanup
```

The recreated-state validation requires a prior cleanup marker plus live proof that users, quotas, topics, and traffic exist in the new generation.

## Storage and log bounds

- Prometheus retains at most 14 days or 1 GiB of TSDB blocks and runs in a 1.5 GiB tmpfs hard cap.
- Each topic has one partition, 30-minute time retention, and 32 MiB retention per partition.
- Each Kafka log directory runs in a 512 MiB tmpfs.
- Kafka, exporter, Prometheus, setup, workload, and helper containers use rotating 10 MiB JSON logs with three files.

The evidence footprint is measured from running containers. Image-layer storage is outside the Prometheus and Kafka data caps and is reported separately by Docker inspection.

## Evidence bundle

Each ordinary telemetry validation creates a timestamped directory under `evidence/` and updates the ignored `evidence/latest` symlink. A complete validated bundle contains:

- `raw-jmx-cluster-a.jsonl`
- `raw-jmx-cluster-b.jsonl`
- `exporter-cluster-a.metrics`
- `exporter-cluster-b.metrics`
- `prometheus-targets.json`
- `prometheus-metadata.json`
- `prometheus-query-results.json`
- `quota-descriptions.json`
- `topic-descriptions.json`
- `cluster-id-comparison.json`
- `clean-restart-manifest.json`
- `footprint.json`
- `cleanup-result.json`
- `validator-result.json`

Raw JMX is collected from each broker over the isolated lab network. Exporter and Prometheus artifacts are captured from live endpoints after authenticated producer and consumer traffic. `cleanup-result.json` is initially pending and is finalized by `cleanup`.

The standalone `evidence` subcommand is capture-only. It updates `evidence/latest` and writes the capture artifacts, but it does not create `validator-result.json`. If it is used after a validation, run `validate` again before `cleanup` when a complete validated and finalized bundle is required.

## Error contract

| Exit | Prefix | Meaning |
| --- | --- | --- |
| 2 | `prereq_failed:` | A required local tool or daemon is unavailable. |
| 3 | `config_generation_failed:` | Local credentials or rendered configuration could not be created. |
| 4 | `kafka_setup_failed:` | Brokers, telemetry services, topics, users, or quotas are not ready. |
| 5 | `workload_failed:` | Continuous workload control failed. |
| 6 | `evidence_capture_failed:` | Live evidence could not be captured. |
| 7 | `validation_failed:` | Evidence failed the metric contract; the message includes the JSON details path. |
| 8 | `cleanup_failed:` | Stop, container removal, secret removal, or cleanup recording failed. |
