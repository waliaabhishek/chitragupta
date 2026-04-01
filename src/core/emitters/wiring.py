from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from core.emitters.sources import (
    BillingRowFetcher,
    IdentityRowFetcher,
    RegistryEmitterBuilder,
    ResourceRowFetcher,
)

if TYPE_CHECKING:
    from core.config.models import EmitterSpec
    from core.emitters.protocols import PipelineDateSource
    from core.emitters.runner import EmitterRunner
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)


def create_auxiliary_prometheus_runners(
    ecosystem: str,
    storage_backend: StorageBackend,
    prometheus_specs: list[EmitterSpec],
    date_source: PipelineDateSource,
) -> list[EmitterRunner]:
    """Create billing/resource/identity EmitterRunner instances for Prometheus-only streams.

    Only call when prometheus_specs is non-empty — these row types are Prometheus-only
    (empty __csv_fields__). Receives only prometheus-filtered specs to prevent spurious
    emission tracking records for CSV or other emitter types.

    All three streams share the same date_source (ChargebackDateSource) — billing,
    resource, and identity data is indexed by chargeback billing dates.
    """
    from core.emitters.runner import EmitterRunner  # avoid circular import at module level

    return [
        EmitterRunner(
            ecosystem=ecosystem,
            storage_backend=storage_backend,
            emitter_specs=prometheus_specs,
            date_source=date_source,
            row_fetcher=BillingRowFetcher(storage_backend),
            emitter_builder=RegistryEmitterBuilder(),
            pipeline="billing",
        ),
        EmitterRunner(
            ecosystem=ecosystem,
            storage_backend=storage_backend,
            emitter_specs=prometheus_specs,
            date_source=date_source,
            row_fetcher=ResourceRowFetcher(storage_backend),
            emitter_builder=RegistryEmitterBuilder(),
            pipeline="resource",
        ),
        EmitterRunner(
            ecosystem=ecosystem,
            storage_backend=storage_backend,
            emitter_specs=prometheus_specs,
            date_source=date_source,
            row_fetcher=IdentityRowFetcher(storage_backend),
            emitter_builder=RegistryEmitterBuilder(),
            pipeline="identity",
        ),
    ]
