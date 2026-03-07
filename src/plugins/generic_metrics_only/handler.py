from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from core.engine.helpers import allocate_by_usage_ratio, allocate_evenly_with_fallback
from core.models import CoreIdentity, Identity, IdentityResolution, IdentitySet, MetricQuery, Resource
from plugins.generic_metrics_only.shared_context import GenericSharedContext

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext, AllocationResult
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from core.plugin.protocols import CostAllocator
    from core.storage.interface import UnitOfWork
    from plugins.generic_metrics_only.config import GenericMetricsOnlyConfig

logger = logging.getLogger(__name__)


def _make_usage_ratio_allocator(label: str, metric_key: str) -> CostAllocator:
    def allocator(ctx: AllocationContext) -> AllocationResult:
        if not ctx.metrics_data:
            return allocate_evenly_with_fallback(ctx)
        identity_values: dict[str, float] = {}
        for row in ctx.metrics_data.get(metric_key, []):
            identity_id = row.labels.get(label)
            if identity_id and row.value > 0:
                identity_values[identity_id] = identity_values.get(identity_id, 0.0) + row.value
        if not identity_values:
            return allocate_evenly_with_fallback(ctx)
        return allocate_by_usage_ratio(ctx, identity_values)

    return allocator  # type: ignore[return-value]  # closure satisfies CostAllocator protocol at runtime


class GenericMetricsOnlyHandler:
    """ServiceHandler for any metrics-only ecosystem.

    All cost types are handled by one handler (single cluster resource = single
    billable unit). Allocators and metrics map are built from config at __init__ time.
    """

    def __init__(
        self,
        config: GenericMetricsOnlyConfig,
        metrics_source: MetricsSource,
    ) -> None:
        self._config = config
        self._metrics_source = metrics_source
        self._ecosystem = config.ecosystem_name
        self._handles_product_types: tuple[str, ...] = tuple(ct.name for ct in config.cost_types)
        # Build allocators and metrics map once from config
        self._allocator_map: dict[str, CostAllocator] = {}
        self._metrics_map: dict[str, list[MetricQuery]] = {}
        cfg = config.identity_source
        for ct in config.cost_types:
            if ct.allocation_strategy == "even_split":
                self._allocator_map[ct.name] = allocate_evenly_with_fallback
                if cfg.source in ("prometheus", "both"):
                    self._metrics_map[ct.name] = [
                        MetricQuery(
                            key="discovery",
                            query_expression=cfg.discovery_query,  # type: ignore[arg-type]  # validated non-None by GenericIdentitySourceConfig.validate_discovery_query
                            label_keys=(cfg.label,),
                            resource_label=cfg.label,
                        )
                    ]
                else:
                    self._metrics_map[ct.name] = []
            else:
                # metric_key must match what _metrics_map returns
                self._allocator_map[ct.name] = _make_usage_ratio_allocator(
                    ct.allocation_label,  # type: ignore[arg-type]  # validated non-None in CostTypeConfig
                    f"alloc_{ct.name}",
                )
                self._metrics_map[ct.name] = [
                    MetricQuery(
                        key=f"alloc_{ct.name}",
                        query_expression=ct.allocation_query,  # type: ignore[arg-type]  # validated non-None by CostTypeConfig.validate_usage_ratio_fields
                        label_keys=(ct.allocation_label,),  # type: ignore[arg-type]  # validated non-None by CostTypeConfig.validate_usage_ratio_fields
                        resource_label=ct.allocation_label,  # type: ignore[arg-type]  # validated non-None by CostTypeConfig.validate_usage_ratio_fields
                    )
                ]

    @property
    def service_type(self) -> str:
        return "generic"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return self._handles_product_types

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
        logger.debug("Gathering %s resources for tenant %s", self.service_type, tenant_id)
        if isinstance(shared_ctx, GenericSharedContext):
            yield shared_ctx.cluster_resource

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
        logger.debug("Gathering %s identities for tenant %s", self.service_type, tenant_id)
        source = self._config.identity_source.source
        if source in ("prometheus", "both"):
            yield from self._gather_from_prometheus(tenant_id)
        if source in ("static", "both"):
            yield from self._gather_static(tenant_id)

    def _gather_from_prometheus(self, tenant_id: str) -> Iterable[Identity]:
        cfg = self._config.identity_source
        query = MetricQuery(
            key="discovery",
            query_expression=cfg.discovery_query,  # type: ignore[arg-type]  # validated non-None
            label_keys=(cfg.label,),
            resource_label=cfg.label,
        )
        now = datetime.now(UTC)
        step = timedelta(seconds=self._config.metrics_step_seconds)
        results = self._metrics_source.query(queries=[query], start=now - timedelta(hours=1), end=now, step=step)
        seen: set[str] = set()
        for row in results.get("discovery", []):
            identity_id = row.labels.get(cfg.label)
            if identity_id and identity_id not in seen:
                seen.add(identity_id)
                yield self._make_identity(identity_id, tenant_id, now)

    def _gather_static(self, tenant_id: str) -> Iterable[Identity]:
        now = datetime.now(UTC)
        for static in self._config.identity_source.static_identities:
            yield CoreIdentity(
                ecosystem=self._ecosystem,
                tenant_id=tenant_id,
                identity_id=static.identity_id,
                identity_type=static.identity_type,
                display_name=static.display_name or static.identity_id,
                created_at=None,
                deleted_at=None,
                last_seen_at=now,
                metadata={"team": static.team} if static.team else {},
            )

    def _make_identity(self, identity_id: str, tenant_id: str, now: datetime) -> Identity:
        cfg = self._config.identity_source
        team = cfg.principal_to_team.get(identity_id, cfg.default_team)
        return CoreIdentity(
            ecosystem=self._ecosystem,
            tenant_id=tenant_id,
            identity_id=identity_id,
            identity_type="principal",
            display_name=team if team != cfg.default_team else identity_id,
            created_at=None,
            deleted_at=None,
            last_seen_at=now,
            metadata={"team": team},
        )

    def resolve_identities(
        self,
        tenant_id: str,
        resource_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
        metrics_data: dict[str, list[MetricRow]] | None,
        uow: UnitOfWork,
    ) -> IdentityResolution:
        logger.debug(
            "Resolving %s identities resource=%s timestamp=%s", self.service_type, resource_id, billing_timestamp
        )
        resource_active = IdentitySet()
        metrics_derived = IdentitySet()
        cfg = self._config.identity_source
        source = cfg.source

        if source in ("prometheus", "both") and metrics_data:
            now = datetime.now(UTC)
            seen: set[str] = set()
            for rows in metrics_data.values():
                for row in rows:
                    identity_id = row.labels.get(cfg.label)
                    if identity_id and identity_id not in seen:
                        seen.add(identity_id)
                        metrics_derived.add(self._make_identity(identity_id, tenant_id, now))

        if source in ("static", "both"):
            for identity in self._gather_static(tenant_id):
                resource_active.add(identity)

        return IdentityResolution(
            resource_active=resource_active,
            metrics_derived=metrics_derived,
            tenant_period=IdentitySet(),
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        return self._metrics_map.get(product_type, [])

    def get_allocator(self, product_type: str) -> CostAllocator:
        try:
            return self._allocator_map[product_type]
        except KeyError:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg) from None
