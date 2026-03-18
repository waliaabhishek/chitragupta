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

## Plugin discovery

File: `src/core/plugin/loader.py`

Plugins are discovered by scanning the plugins directory for packages exporting
a class implementing `EcosystemPlugin`. The built-in plugins are in `src/plugins/`.

Custom plugins can be placed at `plugins_path` (configured in `AppSettings`).

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
