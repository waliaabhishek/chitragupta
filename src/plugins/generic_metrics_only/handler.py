from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from core.engine.allocation_models import ChainModel, EvenSplitModel, TerminalModel, UsageRatioModel
from core.models import CoreIdentity, CostType, Identity, IdentityResolution, IdentitySet, MetricQuery, Resource
from core.models.chargeback import AllocationDetail
from plugins.generic_metrics_only.shared_context import GenericSharedContext

if TYPE_CHECKING:
    from core.engine.allocation import AllocationContext
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from core.plugin.protocols import CostAllocator, ResolveContext
    from core.storage.interface import UnitOfWork
    from plugins.generic_metrics_only.config import CostTypeConfig, GenericMetricsOnlyConfig

logger = logging.getLogger(__name__)


def make_model_from_config(ct: CostTypeConfig) -> ChainModel:
    """Build a ChainModel for the given cost type configuration.

    even_split strategy (2-tier):
        Tier 0: EvenSplitModel(merged_active)  — metrics + static combined
        Tier 1: TerminalModel(UNALLOCATED)      — no identities at all
        (No resource_active tier: merged_active ⊇ resource_active, so a separate
        resource_active tier is dead code.)

    usage_ratio strategy (3-tier):
        Tier 0: UsageRatioModel(usage_source)  — allocate by label metric values
        Tier 1: EvenSplitModel(merged_active)  — fallback when no metric data
        Tier 2: TerminalModel(UNALLOCATED)      — fallback when no identities
    """
    if ct.allocation_strategy == "even_split":
        return ChainModel(
            models=[
                EvenSplitModel(
                    source=lambda ctx: sorted(ctx.identities.merged_active.ids()),
                    cost_type=CostType.SHARED,
                    detail=AllocationDetail.EVEN_SPLIT_ALLOCATION,
                ),
                TerminalModel(
                    identity_id="UNALLOCATED",
                    cost_type=CostType.SHARED,
                    detail=AllocationDetail.NO_IDENTITIES_LOCATED,
                ),
            ],
            log_fallbacks=True,
        )
    else:
        # usage_ratio — ct.allocation_label validated non-None by CostTypeConfig.validate_usage_ratio_fields
        label = ct.allocation_label
        assert label is not None  # validated by CostTypeConfig.validate_usage_ratio_fields
        metric_key = f"alloc_{ct.name}"

        def usage_source(ctx: AllocationContext) -> dict[str, float]:
            if not ctx.metrics_data:
                return {}
            result: dict[str, float] = {}
            for row in ctx.metrics_data.get(metric_key, []):
                identity_id = row.labels.get(label)
                if identity_id and row.value > 0:
                    result[identity_id] = result.get(identity_id, 0.0) + row.value
            return result

        return ChainModel(
            models=[
                UsageRatioModel(
                    usage_source=usage_source,
                    detail=AllocationDetail.USAGE_RATIO_ALLOCATION,
                ),
                EvenSplitModel(
                    source=lambda ctx: sorted(ctx.identities.merged_active.ids()),
                    cost_type=CostType.SHARED,
                    detail=AllocationDetail.NO_METRICS_LOCATED,
                ),
                TerminalModel(
                    identity_id="UNALLOCATED",
                    cost_type=CostType.SHARED,
                    detail=AllocationDetail.NO_IDENTITIES_LOCATED,
                ),
            ],
            log_fallbacks=True,
        )


class GenericMetricsOnlyHandler:
    """ServiceHandler for any metrics-only ecosystem.

    All cost types are handled by one handler (single cluster resource = single
    billable unit). Models and metrics map are built from config at __init__ time.
    """

    def __init__(
        self,
        config: GenericMetricsOnlyConfig,
        metrics_source: MetricsSource,
    ) -> None:
        self._config = config
        self._metrics_source = metrics_source
        self._ecosystem = "generic_metrics_only"
        self._handles_product_types: tuple[str, ...] = tuple(ct.name for ct in config.cost_types)
        # Build discovery query once (reused in _metrics_map and _gather_from_prometheus)
        cfg = config.identity_source
        self._discovery_query: MetricQuery | None = (
            MetricQuery(
                key="discovery",
                query_expression=cfg.discovery_query,  # type: ignore[arg-type]  # validated non-None by GenericIdentitySourceConfig.validate_discovery_query
                label_keys=(cfg.label,),
                resource_label=cfg.label,
            )
            if cfg.source in ("prometheus", "both")
            else None
        )
        # Build models and metrics map once from config
        self._model_map: dict[str, ChainModel] = {}
        self._metrics_map: dict[str, list[MetricQuery]] = {}
        for ct in config.cost_types:
            self._model_map[ct.name] = make_model_from_config(ct)
            if ct.allocation_strategy == "even_split":
                if self._discovery_query is not None:
                    self._metrics_map[ct.name] = [self._discovery_query]
                else:
                    self._metrics_map[ct.name] = []
            else:
                self._metrics_map[ct.name] = [
                    MetricQuery(
                        key=f"alloc_{ct.name}",
                        query_expression=ct.allocation_query,  # type: ignore[arg-type]  # validated non-None by CostTypeConfig.validate_usage_ratio_fields
                        label_keys=(ct.allocation_label,),  # type: ignore[arg-type]  # validated non-None by CostTypeConfig.validate_usage_ratio_fields
                        resource_label=ct.allocation_label,
                    )
                ]

    @property
    def service_type(self) -> str:
        return "generic"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return self._handles_product_types

    @property
    def gathered_resource_types(self) -> Sequence[str]:
        return ["cluster"]

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
        query = self._discovery_query
        if query is None:
            return
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
        context: ResolveContext | None = None,
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
            return self._model_map[product_type]
        except KeyError:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg) from None
