"""Self-managed Kafka service handler.

Single handler covering all product types:
- SELF_KAFKA_COMPUTE: fixed compute costs (even split)
- SELF_KAFKA_STORAGE: storage costs (even split)
- SELF_KAFKA_NETWORK_INGRESS: ingress costs (usage ratio)
- SELF_KAFKA_NETWORK_EGRESS: egress costs (usage ratio)
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from math import isfinite
from typing import TYPE_CHECKING, Any, Literal

from core.metrics.protocol import MetricsQueryError
from core.models import Identity, IdentityResolution, IdentitySet, MetricQuery, Resource
from plugins.self_managed_kafka.allocation_models import (
    SMK_EGRESS_MODEL,
    SMK_INFRA_MODEL,
    SMK_INGRESS_MODEL,
    FixedPrincipalPolicyAllocationModel,
    QuotaPrincipalAllocationModel,
)
from plugins.self_managed_kafka.principal_attribution import (
    PrincipalAttributionState,
    PrincipalDirectionEvaluation,
    evaluate_quota_direction,
)
from plugins.self_managed_kafka.telemetry_contract import (
    SMK_DETAIL_NO_FINITE_POSITIVE_THROTTLE,
    SMK_DETAIL_PRINCIPAL_TELEMETRY_INVALID,
    SMK_DETAIL_PRINCIPAL_TELEMETRY_NOT_OBSERVED,
    MetricsScopeEvidence,
    MetricsScopeStatus,
    PrincipalTelemetryEvidence,
    PrincipalTelemetryStatus,
)

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.models import MetricRow
    from core.plugin.protocols import CostAllocator, ResolveContext
    from core.storage.interface import UnitOfWork
    from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
    from plugins.self_managed_kafka.shared_context import SMKSharedContext

logger = logging.getLogger(__name__)


_SELF_KAFKA_PRODUCT_TYPES: tuple[str, ...] = (
    "SELF_KAFKA_COMPUTE",
    "SELF_KAFKA_STORAGE",
    "SELF_KAFKA_NETWORK_INGRESS",
    "SELF_KAFKA_NETWORK_EGRESS",
)

_ALLOCATOR_MAP: dict[str, CostAllocator] = {
    "SELF_KAFKA_COMPUTE": SMK_INFRA_MODEL,
    "SELF_KAFKA_STORAGE": SMK_INFRA_MODEL,
    "SELF_KAFKA_NETWORK_INGRESS": SMK_INGRESS_MODEL,
    "SELF_KAFKA_NETWORK_EGRESS": SMK_EGRESS_MODEL,
}


class SelfManagedKafkaHandler:
    """Service handler for self-managed Kafka clusters.

    Implements the ServiceHandler protocol. Single handler covering all product
    types since all costs flow through one cluster resource.

    Dispatches resource/identity gathering based on resource_source.source
    and identity_source.source configuration.
    """

    def __init__(
        self,
        config: SelfManagedKafkaConfig,
        metrics_source: MetricsSource,
        admin_client: Any = None,
        metrics_scope_evidence: Callable[[str, datetime, timedelta], MetricsScopeEvidence | None] | None = None,
    ) -> None:
        """Initialize handler with config and discovery clients.

        Args:
            config: Plugin configuration.
            metrics_source: Prometheus client for resource/identity discovery.
            admin_client: Kafka AdminClient for resource discovery (optional).
        """
        self._config = config
        self._metrics_source = metrics_source
        self._admin_client = admin_client
        self._metrics_scope_evidence = metrics_scope_evidence
        self._ecosystem = "self_managed_kafka"
        self._current_gather_ctx: SMKSharedContext | None = None
        self._admin_inventory_complete = False
        self._admin_inventory_is_partitionless = False
        self._principal_evidence_cache: dict[tuple[str, str, datetime, datetime], PrincipalTelemetryEvidence] = {}
        self._allocator_map = self._build_allocator_map()

    @property
    def service_type(self) -> str:
        return "kafka"

    @property
    def handles_product_types(self) -> Sequence[str]:
        return _SELF_KAFKA_PRODUCT_TYPES

    def _build_allocator_map(self) -> dict[str, CostAllocator]:
        """Build handlers' allocation policy from the validated plugin configuration."""
        if not self._config.principal_attribution.enabled:
            return _ALLOCATOR_MAP
        attribution = self._config.principal_attribution
        static_identities = tuple(
            sorted(identity.identity_id for identity in self._config.identity_source.static_identities)
        )
        return {
            "SELF_KAFKA_COMPUTE": FixedPrincipalPolicyAllocationModel(attribution.compute_policy, static_identities),
            "SELF_KAFKA_STORAGE": FixedPrincipalPolicyAllocationModel(attribution.storage_policy, static_identities),
            "SELF_KAFKA_NETWORK_INGRESS": QuotaPrincipalAllocationModel("ingress"),
            "SELF_KAFKA_NETWORK_EGRESS": QuotaPrincipalAllocationModel("egress"),
        }

    @property
    def gathered_resource_types(self) -> Sequence[str]:
        return ["cluster", "broker", "topic"]

    def gather_resources(self, tenant_id: str, uow: UnitOfWork, shared_ctx: object | None = None) -> Iterable[Resource]:
        """Gather cluster, brokers, and topics.

        Cluster resource comes from shared_ctx (pre-built in Phase 1).
        Broker and topic gathering proceeds as before via admin_api or Prometheus.
        """
        logger.debug("Gathering %s resources for tenant %s", self.service_type, tenant_id)
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        if not isinstance(shared_ctx, SMKSharedContext):
            return

        self._current_gather_ctx = shared_ctx
        yield shared_ctx.cluster_resource

        if self._config.resource_source.source == "admin_api":
            self.clear_admin_inventory_proof()
            yield from self._gather_resources_from_admin(tenant_id)
        else:
            yield from self._gather_resources_from_prometheus(tenant_id)

    @property
    def admin_inventory_complete(self) -> bool:
        """Whether the current gather completed authoritative Admin API discovery."""
        return self._admin_inventory_complete

    def clear_admin_inventory_proof(self) -> None:
        """Discard the prior gather's Admin API inventory proof."""
        self._admin_inventory_complete = False
        self._admin_inventory_is_partitionless = False

    @property
    def admin_inventory_is_partitionless(self) -> bool:
        """Whether current authoritative Admin API discovery found no topics."""
        return self._admin_inventory_complete and self._admin_inventory_is_partitionless

    def _gather_resources_from_prometheus(self, tenant_id: str) -> Iterable[Resource]:
        """Gather brokers and topics from cached discovery sets in shared context."""
        ctx = self._current_gather_ctx
        if ctx is None or ctx.discovered_brokers is None or ctx.discovered_topics is None:
            return

        from plugins.self_managed_kafka.gathering.prometheus import brokers_to_resources, topics_to_resources

        yield from brokers_to_resources(ctx.discovered_brokers, self._ecosystem, tenant_id, self._config.cluster_id)
        yield from topics_to_resources(ctx.discovered_topics, self._ecosystem, tenant_id, self._config.cluster_id)

    def _gather_resources_from_admin(self, tenant_id: str) -> Iterable[Resource]:
        """Gather brokers and topics from Kafka Admin API."""
        from plugins.self_managed_kafka.gathering.admin_api import (
            gather_brokers_from_admin,
            gather_topics_from_admin,
        )

        if self._admin_client is None:
            return

        yield from gather_brokers_from_admin(self._admin_client, self._ecosystem, tenant_id, self._config.cluster_id)
        topics = tuple(
            gather_topics_from_admin(self._admin_client, self._ecosystem, tenant_id, self._config.cluster_id)
        )
        self._admin_inventory_complete = True
        self._admin_inventory_is_partitionless = not topics
        yield from topics

    def gather_identities(self, tenant_id: str, uow: UnitOfWork) -> Iterable[Identity]:
        """Gather configured static policy identities only."""
        logger.debug("Gathering %s identities for tenant %s", self.service_type, tenant_id)
        source = self._config.identity_source.source
        if source in ("static", "both"):
            yield from self._gather_static_identities(tenant_id)

    def _gather_static_identities(self, tenant_id: str) -> Iterable[Identity]:
        """Load static identities from config."""
        from plugins.self_managed_kafka.gathering.prometheus import load_static_identities

        yield from load_static_identities(self._config.identity_source, self._ecosystem, tenant_id)

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
        """Resolve static policy identities and quota telemetry evidence per window."""
        logger.debug(
            "Resolving %s identities resource=%s timestamp=%s", self.service_type, resource_id, billing_timestamp
        )
        resource_active = IdentitySet()
        metrics_derived = IdentitySet()
        tenant_period = IdentitySet()

        source = self._config.identity_source.source
        if source in ("static", "both"):
            from plugins.self_managed_kafka.gathering.prometheus import load_static_identities

            for identity in load_static_identities(self._config.identity_source, self._ecosystem, tenant_id):
                resource_active.add(identity)

        scope_evidence: MetricsScopeEvidence | None = None
        if self._config.principal_attribution.enabled:
            scope_evidence = (
                self._metrics_scope_evidence(tenant_id, billing_timestamp, billing_duration)
                if self._metrics_scope_evidence is not None
                else None
            )
            evidence = self._measured_principal_telemetry_evidence(
                tenant_id,
                resource_id,
                billing_timestamp,
                billing_duration,
                scope_evidence,
            )
        else:
            evidence = self._principal_telemetry_evidence(tenant_id, resource_id, billing_timestamp, billing_duration)
            scope_evidence = (
                self._metrics_scope_evidence(tenant_id, billing_timestamp, billing_duration)
                if self._metrics_scope_evidence is not None
                else None
            )

        if self._config.principal_attribution.enabled:
            from core.models import CoreIdentity

            for direction in (evidence.ingress, evidence.egress):
                if direction is None:
                    continue
                for weight in direction.user_weights:
                    metrics_derived.add(
                        CoreIdentity(
                            ecosystem=self._ecosystem,
                            tenant_id=tenant_id,
                            identity_id=weight.identity_id,
                            identity_type="principal",
                            metadata={"team": weight.team},
                        )
                    )
        resolution_context: dict[str, object] = {
            "principal_attribution_status": evidence.status.value,
            "principal_attribution_detail": evidence.detail,
            "principal_telemetry_evidence": evidence,
            "measured_usage": self._config.principal_attribution.enabled,
        }
        if scope_evidence is not None:
            resolution_context.update(
                {
                    "metrics_scope_status": scope_evidence.status.value,
                    "metrics_scope_detail": scope_evidence.detail,
                    "metrics_scope_evidence": scope_evidence,
                }
            )

        return IdentityResolution(
            resource_active=resource_active,
            metrics_derived=metrics_derived,
            tenant_period=tenant_period,
            context=resolution_context,
        )

    def _measured_principal_telemetry_evidence(
        self,
        tenant_id: str,
        resource_id: str,
        start: datetime,
        duration: timedelta,
        scope_evidence: MetricsScopeEvidence | None,
    ) -> PrincipalTelemetryEvidence:
        """Acquire quota evidence only after the existing target-scope gate."""
        end = start + duration
        attribution = self._config.principal_attribution
        if scope_evidence is None or scope_evidence.status is not MetricsScopeStatus.VALID:
            unavailable = self._unavailable_direction(
                "ingress",
                "target_scope_blocked",
                timedelta(seconds=attribution.scrape_interval_seconds or 1),
            )
            unavailable_egress = self._unavailable_direction(
                "egress",
                "target_scope_blocked",
                timedelta(seconds=attribution.scrape_interval_seconds or 1),
            )
            return PrincipalTelemetryEvidence(
                window_start=start,
                window_end=end,
                status=PrincipalTelemetryStatus.UNAVAILABLE,
                detail="target_scope_blocked",
                ingress=unavailable,
                egress=unavailable_egress,
            )
        cache_key = (tenant_id, resource_id, start, end)
        cached = self._principal_evidence_cache.get(cache_key)
        if cached is not None:
            return cached
        scrape_interval = timedelta(seconds=attribution.scrape_interval_seconds or 1)
        max_gap = timedelta(seconds=attribution.max_gap_seconds or 1)
        evaluations: dict[Literal["ingress", "egress"], PrincipalDirectionEvaluation] = {}
        direction_quota_types: tuple[tuple[Literal["ingress", "egress"], Literal["Produce", "Fetch"]], ...] = (
            ("ingress", "Produce"),
            ("egress", "Fetch"),
        )
        for direction, quota_type in direction_quota_types:
            query = self._quota_query(direction, quota_type, duration + max_gap)
            try:
                result = self._metrics_source.query(
                    queries=[query],
                    start=start,
                    end=end,
                    step=duration,
                    resource_id_filter=self._config.metrics_identifier,
                )
            except MetricsQueryError:
                evaluations[direction] = self._unavailable_direction(direction, "metrics_fetch_failed", scrape_interval)
                continue
            evaluations[direction] = evaluate_quota_direction(
                result.get(query.key, []),
                direction=direction,
                start=start,
                end=end,
                scrape_interval=scrape_interval,
                max_gap=max_gap,
                principal_to_team=self._config.identity_source.principal_to_team,
                default_team=self._config.identity_source.default_team,
            )
        ingress = evaluations["ingress"]
        egress = evaluations["egress"]
        status = (
            PrincipalTelemetryStatus.UNAVAILABLE
            if PrincipalAttributionState.UNAVAILABLE in {ingress.state, egress.state}
            else PrincipalTelemetryStatus.OBSERVED
        )
        detail = (
            "principal_telemetry_unavailable"
            if status is PrincipalTelemetryStatus.UNAVAILABLE
            else "quota_identity_observed"
        )
        evidence = PrincipalTelemetryEvidence(
            window_start=start,
            window_end=end,
            status=status,
            detail=detail,
            ingress=ingress,
            egress=egress,
        )
        self._principal_evidence_cache[cache_key] = evidence
        return evidence

    def _quota_query(
        self,
        direction: Literal["ingress", "egress"],
        quota_type: Literal["Produce", "Fetch"],
        source_window: timedelta,
    ) -> MetricQuery:
        """Build one exact raw quota range selector for a measured direction."""
        del direction
        selector_label = self._promql_quote(self._config.metrics_identifier_label)
        selector_value = self._promql_quote(self._config.metrics_identifier)
        seconds = int(source_window.total_seconds())
        return MetricQuery(
            key=f"principal_quota_{quota_type.lower()}",
            query_expression=(
                f'kafka_server_quota_byte_rate{{{selector_label}="{selector_value}",quota_type="{quota_type}"}}[{seconds}s]'
            ),
            label_keys=(
                "broker",
                self._config.metrics_identifier_label,
                "quota_type",
                "quota_scope",
                "user",
                "client_id",
            ),
            resource_label=self._config.metrics_identifier_label,
            query_mode="instant",
        )

    @staticmethod
    def _promql_quote(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    @staticmethod
    def _unavailable_direction(
        direction: Literal["ingress", "egress"],
        detail: str,
        scrape_interval: timedelta,
    ) -> PrincipalDirectionEvaluation:
        quota_type: Literal["Produce", "Fetch"] = "Produce" if direction == "ingress" else "Fetch"
        return PrincipalDirectionEvaluation(
            direction=direction,
            quota_type=quota_type,
            state=PrincipalAttributionState.UNAVAILABLE,
            detail=detail,
            user_weights=(),
            client_only_weight=Decimal("0"),
            total_weight=Decimal("0"),
            coverage_complete=False,
            declared_scrape_interval=scrape_interval,
            observed_deltas=(),
        )

    def get_metrics_for_product_type(self, product_type: str) -> list[MetricQuery]:
        """No BrokerTopicMetrics row is a principal allocation input."""
        if product_type in _SELF_KAFKA_PRODUCT_TYPES:
            return []
        return []

    def clear_principal_telemetry_evidence(self) -> None:
        """Discard evidence cached for a prior orchestration cycle."""
        self._principal_evidence_cache.clear()

    def _principal_telemetry_evidence(
        self,
        tenant_id: str,
        resource_id: str,
        start: datetime,
        duration: timedelta,
    ) -> PrincipalTelemetryEvidence:
        end = start + duration
        if self._config.identity_source.source == "static":
            return PrincipalTelemetryEvidence(
                window_start=start,
                window_end=end,
                status=PrincipalTelemetryStatus.POLICY_ONLY_CONFIGURED,
                detail="policy_only_configured",
            )
        cache_key = (tenant_id, resource_id, start, end)
        if cache_key in self._principal_evidence_cache:
            return self._principal_evidence_cache[cache_key]
        queries = [
            MetricQuery(
                key="quota_byte_rate",
                query_expression="sum by (quota_type, quota_scope, user, client_id) (kafka_server_quota_byte_rate{})",
                label_keys=("quota_type", "quota_scope", "user", "client_id"),
                resource_label=self._config.metrics_identifier_label,
            ),
            MetricQuery(
                key="quota_throttle_time_ms",
                query_expression=(
                    "avg by (quota_type, quota_scope, user, client_id) (kafka_server_quota_throttle_time_ms{})"
                ),
                label_keys=("quota_type", "quota_scope", "user", "client_id"),
                resource_label=self._config.metrics_identifier_label,
            ),
        ]
        try:
            result = self._metrics_source.query(
                queries=queries,
                start=start,
                end=end,
                step=duration,
                resource_id_filter=self._config.metrics_identifier,
            )
        except MetricsQueryError:
            return PrincipalTelemetryEvidence(
                window_start=start,
                window_end=end,
                status=PrincipalTelemetryStatus.TRANSIENT_FAILURE,
                detail="metrics_fetch_failed",
            )

        byte_rate_rows = result.get("quota_byte_rate", [])
        if not byte_rate_rows:
            evidence = PrincipalTelemetryEvidence(
                window_start=start,
                window_end=end,
                status=PrincipalTelemetryStatus.NOT_OBSERVED,
                detail=SMK_DETAIL_PRINCIPAL_TELEMETRY_NOT_OBSERVED,
            )
            self._principal_evidence_cache[cache_key] = evidence
            return evidence
        quota_scopes = frozenset(row.labels.get("quota_scope", "") for row in byte_rate_rows)
        if any(not self._is_valid_quota_row(row, require_finite_value=True) for row in byte_rate_rows):
            evidence = PrincipalTelemetryEvidence(
                window_start=start,
                window_end=end,
                status=PrincipalTelemetryStatus.INVALID,
                detail=SMK_DETAIL_PRINCIPAL_TELEMETRY_INVALID,
                quota_scopes=quota_scopes,
            )
            self._principal_evidence_cache[cache_key] = evidence
            return evidence
        throttle_rows = result.get("quota_throttle_time_ms", [])
        if any(not self._is_valid_quota_row(row, require_finite_value=False) for row in throttle_rows):
            evidence = PrincipalTelemetryEvidence(
                window_start=start,
                window_end=end,
                status=PrincipalTelemetryStatus.INVALID,
                detail=SMK_DETAIL_PRINCIPAL_TELEMETRY_INVALID,
                quota_scopes=quota_scopes,
            )
        elif any(not isfinite(row.value) for row in throttle_rows):
            evidence = PrincipalTelemetryEvidence(
                window_start=start,
                window_end=end,
                status=PrincipalTelemetryStatus.OBSERVED,
                detail=SMK_DETAIL_NO_FINITE_POSITIVE_THROTTLE,
                quota_scopes=quota_scopes,
            )
        else:
            evidence = PrincipalTelemetryEvidence(
                window_start=start,
                window_end=end,
                status=PrincipalTelemetryStatus.OBSERVED,
                detail="quota_identity_observed",
                quota_scopes=quota_scopes,
            )
        self._principal_evidence_cache[cache_key] = evidence
        return evidence

    @staticmethod
    def _is_valid_quota_row(row: MetricRow, *, require_finite_value: bool) -> bool:
        labels = row.labels
        if not {"quota_type", "quota_scope", "user", "client_id"}.issubset(labels):
            return False
        if labels.get("quota_type") not in {"Produce", "Fetch"}:
            return False
        scope = labels.get("quota_scope")
        if scope not in {"user", "client-id", "user-client"}:
            return False
        if scope in {"user", "user-client"} and not SelfManagedKafkaHandler._is_identity_label(labels.get("user")):
            return False
        if scope in {"client-id", "user-client"} and not SelfManagedKafkaHandler._is_identity_label(
            labels.get("client_id")
        ):
            return False
        return not require_finite_value or isfinite(row.value)

    @staticmethod
    def _is_identity_label(value: str | None) -> bool:
        return bool(value and value != "not_applicable")

    def get_allocator(self, product_type: str) -> CostAllocator:
        """Return allocator function for this product type."""
        allocator = self._allocator_map.get(product_type)
        if allocator is None:
            msg = f"Unknown product type: {product_type}"
            raise ValueError(msg)
        return allocator
