# Architecture Overview

The engine is a multi-tenant cost allocation pipeline. Each tenant maps to one ecosystem plugin.

## Component hierarchy

```mermaid
graph TD
    A[AppSettings] --> B[WorkflowRunner]
    B --> C[ChargebackOrchestrator]
    C --> GP[GatherPhase]
    C --> CP[CalculatePhase]
    C --> TOP[TopicOverlay — optional]
    C --> EP[EmitPhase]
    C --> D[EcosystemPlugin]
    D --> E[ServiceHandler×N]
    D --> F[CostInput]
    D --> G[MetricsSource]
    D --> SM[StorageModule]
    E --> H[CostAllocator]
    C --> I[StorageBackend]
    C --> J[Emitter×N]
```

The orchestrator delegates to three internal phase classes: `GatherPhase` (billing + resources + identities + deletion detection), `CalculatePhase` (metrics + identity resolution + allocation), and `EmitPhase` (commit + emitters). When topic attribution is enabled (CCloud + Prometheus), an optional **TopicOverlay** stage runs between Calculate and Emit to attribute Kafka cluster costs to individual topics.

## Layers

| Layer | Package | Responsibility |
|---|---|---|
| Entry point | `src/main.py` | Arg parsing, mode selection, signal handling |
| Runner | `src/workflow_runner.py` | Periodic execution, tenant lifecycle, concurrency |
| Orchestrator | `src/core/engine/orchestrator.py` | Pipeline steps per tenant per date |
| Plugin | `src/plugins/*/plugin.py` | Ecosystem-specific initialization and wiring |
| Handler | `src/plugins/*/handlers/` | Resource/identity gather and cost allocation |
| Storage | `src/core/storage/` | SQLModel + Alembic, per-tenant isolation |
| API | `src/core/api/` | FastAPI REST, reads from storage |
| Emitters | `src/emitters/` | Output sinks (CSV, etc.) |

## Detailed documentation

| Page | Purpose |
|---|---|
| [Plugin System](plugin-system.md) | Protocol hierarchy and plugin loading |
| [Data Flow](data-flow.md) | Step-by-step pipeline execution |
| [Identity Resolution](identity-resolution.md) | How principals map to cost allocations |
