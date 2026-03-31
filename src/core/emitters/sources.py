from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Any, Literal

from core.emitters.registry import get as registry_get
from core.emitters.registry import get_factory

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.config.models import EmitterSpec
    from core.models.chargeback import ChargebackRow
    from core.storage.interface import StorageBackend


class ChargebackDateSource:
    """PipelineDateSource backed by ChargebackRepository."""

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date]:
        with self._storage_backend.create_unit_of_work() as uow:
            return uow.chargebacks.get_distinct_dates(ecosystem, tenant_id)


class ChargebackRowFetcher:
    """PipelineAggregatedRowFetcher backed by ChargebackRepository."""

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def fetch_by_date(self, ecosystem: str, tenant_id: str, dt: date) -> list[ChargebackRow]:
        with self._storage_backend.create_unit_of_work() as uow:
            return uow.chargebacks.find_by_date(ecosystem, tenant_id, dt)

    def fetch_aggregated(
        self,
        ecosystem: str,
        tenant_id: str,
        start: date,
        end: date,
        granularity: Literal["daily", "monthly"],
    ) -> list[ChargebackRow]:
        with self._storage_backend.create_unit_of_work() as uow:
            return uow.chargebacks.find_aggregated_for_emit(ecosystem, tenant_id, start, end, granularity)


class RegistryEmitterBuilder:
    """PipelineEmitterBuilder that uses the emitter registry (chargeback pipeline)."""

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def build(self, spec: EmitterSpec) -> Any:
        factory = get_factory(spec.type)
        extra: dict[str, Any] = {}
        if factory is not None and getattr(factory, "needs_storage_backend", False):
            extra["storage_backend"] = self._storage_backend
        return registry_get(spec.type, spec.params, extra=extra)
