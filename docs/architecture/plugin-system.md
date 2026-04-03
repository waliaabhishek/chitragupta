# Plugin System

## Protocols

File: `src/core/plugin/protocols.py`

Runtime-checkable protocols:

| Protocol | Responsibility |
|---|---|
| `EcosystemPlugin` | Entry point per ecosystem. Initialize, return handlers/cost-input/metrics |
| `ServiceHandler` | Per product type: gather resources, identities, resolve allocations |
| `CostInput` | Yield `BillingLineItem` objects for a date range |
| `CostAllocator` | Callable: `AllocationContext → AllocationResult` |
| `Emitter` | Callable: `(tenant_id, date, rows) → None` |
| `StorageModule` | Plugin-supplied table schemas: custom billing/resource/identity repositories |
| `IdentityResolver` | Standalone callable override for identity resolution per product type |
| `TopicDiscoveryPlugin` | Gather topic resources from Prometheus for topic attribution (CCloud only) |
| `OverlayPlugin` | Provide overlay-specific config (e.g., topic attribution) to core code |

## Plugin discovery

File: `src/core/plugin/loader.py`

Plugins are discovered by scanning the plugins directory for packages exporting
a `register()` function. The built-in plugins are in `src/plugins/`.

Custom plugins can be placed at `plugins_path` (configured in `AppSettings`).
Two import strategies are used transparently:

- **Package import** (`importlib.import_module`): used when the plugins directory's
  parent is on `sys.path`. This is the path taken for built-in `src/plugins/`.
- **File-based import** (`importlib.util.spec_from_file_location`): used for external
  directories not on `sys.path`. Each plugin package must contain `__init__.py`.

## Plugin initialization

```python
# src/core/plugin/protocols.py, line 95
def initialize(self, config: dict[str, Any]) -> None: ...
```

Called once per tenant runtime with the raw `plugin_settings` dict. The plugin
validates config (Pydantic), creates clients, and wires internal components.

## Handler dispatch

Each handler declares which product types it handles:

```python
@property
def handles_product_types(self) -> Sequence[str]: ...
```

The orchestrator routes each `BillingLineItem.product_type` to the matching handler.
When no handler matches, `plugin.get_fallback_allocator()` is called. CCloud's fallback
logs a warning and allocates the cost to UNALLOCATED.

## ResolveContext

`ServiceHandler.resolve_identities()` accepts an optional `context: ResolveContext | None`
parameter for caching optimization. `ResolveContext` is a TypedDict containing
`cached_identities` (IdentitySet) and `cached_resources` (dict of Resource objects).

## Emitter registry

Emitters are registered at application startup via `core.emitters.registry.register(name, factory)`. The factory callable receives `**params` from the YAML config's `params` dict.

**Storage injection:** If a factory needs access to the storage backend (to query billing/resource/identity data at emit time), set the attribute `factory.needs_storage_backend = True`. The orchestrator detects this flag and injects `storage_backend` as a keyword argument alongside the configured params. Factories without this attribute receive only their configured params.

```python
def make_my_emitter(port: int, storage_backend: StorageBackend) -> MyEmitter:
    return MyEmitter(port=port, storage_backend=storage_backend)

make_my_emitter.needs_storage_backend = True
register("my_emitter", make_my_emitter)
```

The built-in emitters are `csv` (no storage needed) and `prometheus` (`needs_storage_backend = True`).

## Topic attribution protocols

Two additional runtime-checkable protocols support the topic attribution overlay:

**`TopicDiscoveryPlugin`** (`src/core/plugin/protocols.py:160-171`)

```python
class TopicDiscoveryPlugin(Protocol):
    def gather_topic_resources(
        self, tenant_id: str, cluster_ids: list[str],
    ) -> Iterable[Resource]: ...
```

Implement alongside `EcosystemPlugin` to enable topic discovery via Prometheus metrics.

**`OverlayPlugin`** (`src/core/plugin/protocols.py:181-188`)

```python
class OverlayPlugin(Protocol):
    def get_overlay_config(self, name: str) -> OverlayConfig | None: ...
```

Provides overlay-specific configuration (e.g., `TopicAttributionConfig`) to core code without `getattr` probing.

| Plugin | TopicDiscoveryPlugin | OverlayPlugin |
|---|---|---|
| `confluent_cloud` | Yes | Yes |
| `self_managed_kafka` | No | No |
| `generic_metrics_only` | No | No |

## Lifecycle

1. `plugin.initialize(config)` — validate config, create clients
2. `plugin.get_service_handlers()` → dict of handlers by service_type
3. `plugin.get_cost_input()` → CostInput
4. `plugin.get_metrics_source()` → MetricsSource or None
5. `plugin.get_storage_module()` → StorageModule (custom table schemas, e.g. CCloud billing with `env_id` in PK)
6. `plugin.get_fallback_allocator()` → CostAllocator or None (handles unknown product types)
7. `plugin.build_shared_context(tenant_id)` → shared state accessible to all handlers
8. Per billing date: gather → detect deletions → allocate → commit → emit
9. `plugin.close()` — clean up connections
