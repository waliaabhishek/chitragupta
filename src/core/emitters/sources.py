from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, Literal

from core.emitters.emit_rows import BillingEmitRow, IdentityEmitRow, ResourceEmitRow
from core.emitters.registry import get as registry_get

if TYPE_CHECKING:
    from core.config.models import EmitterSpec
    from core.models.chargeback import ChargebackRow
    from core.models.topic_attribution import TopicAttributionRow
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)


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


class TopicAttributionDateSource:
    """PipelineDateSource backed by TopicAttributionRepository (read-only UoW)."""

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def get_distinct_dates(self, ecosystem: str, tenant_id: str) -> list[date]:
        with self._storage_backend.create_read_only_unit_of_work() as uow:
            return uow.topic_attributions.get_distinct_dates(ecosystem, tenant_id)


class TopicAttributionRowFetcher:
    """PipelineRowFetcher backed by TopicAttributionRepository (read-only UoW)."""

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def fetch_by_date(self, ecosystem: str, tenant_id: str, dt: date) -> list[TopicAttributionRow]:
        with self._storage_backend.create_read_only_unit_of_work() as uow:
            return uow.topic_attributions.find_by_date(ecosystem, tenant_id, dt)


class BillingRowFetcher:
    """PipelineRowFetcher for billing data — wraps BillingLineItem into BillingEmitRow."""

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def fetch_by_date(self, ecosystem: str, tenant_id: str, dt: date) -> list[BillingEmitRow]:
        billing_ts = datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=UTC)
        with self._storage_backend.create_unit_of_work() as uow:
            lines = uow.billing.find_by_date(ecosystem, tenant_id, dt)
            return [BillingEmitRow.from_line(line, tenant_id, billing_ts) for line in lines]


class ResourceRowFetcher:
    """PipelineRowFetcher for active resources — wraps Resource into ResourceEmitRow."""

    def __init__(self, storage_backend: StorageBackend, resource_types: Sequence[str]) -> None:
        self._storage_backend = storage_backend
        self._resource_types = resource_types

    def fetch_by_date(self, ecosystem: str, tenant_id: str, dt: date) -> list[ResourceEmitRow]:
        billing_ts = datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=UTC)
        with self._storage_backend.create_unit_of_work() as uow:
            resources, _ = uow.resources.find_active_at(
                ecosystem,
                tenant_id,
                billing_ts,
                resource_type=self._resource_types,
                count=False,
            )
            return [ResourceEmitRow.from_resource(r, tenant_id, billing_ts) for r in resources]


class IdentityRowFetcher:
    """PipelineRowFetcher for active identities — wraps Identity into IdentityEmitRow."""

    def __init__(self, storage_backend: StorageBackend) -> None:
        self._storage_backend = storage_backend

    def fetch_by_date(self, ecosystem: str, tenant_id: str, dt: date) -> list[IdentityEmitRow]:
        billing_ts = datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=UTC)
        with self._storage_backend.create_unit_of_work() as uow:
            identities, _ = uow.identities.find_active_at(ecosystem, tenant_id, billing_ts, count=False)
            return [IdentityEmitRow.from_identity(i, tenant_id, billing_ts) for i in identities]


class RegistryEmitterBuilder:
    """PipelineEmitterBuilder that uses the emitter registry for all pipelines."""

    def build(self, spec: EmitterSpec) -> Any:
        return registry_get(spec.type, spec.params)
