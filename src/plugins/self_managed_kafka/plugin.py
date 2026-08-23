"""Self-managed Kafka ecosystem plugin."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from math import isfinite
from typing import TYPE_CHECKING, Any, cast

from core.logging_context import safe_exception_context, safe_log_context
from core.metrics.config import create_metrics_source
from core.metrics.protocol import MetricsQueryError
from core.models import MetricQuery
from core.plugin.protocols import (
    ScopeBlockedError,
    ScopeGateDecision,
    ScopeGateResult,
)
from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
from plugins.self_managed_kafka.cost_input import ConstructedCostInput
from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
from plugins.self_managed_kafka.telemetry_contract import (
    MetricsScopeEvidence,
    MetricsScopeRequest,
    MetricsScopeStatus,
)

if TYPE_CHECKING:
    from core.engine.topic_attribution_provider import TopicAttributionProvider
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostAllocator, CostInput, OverlayConfig, ServiceHandler
    from core.storage.interface import UnitOfWork
    from plugins.self_managed_kafka.shared_context import SMKSharedContext
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule
    from plugins.self_managed_kafka.storage.repositories import SelfManagedKafkaScopeStateRepository

logger = logging.getLogger(__name__)


class SelfManagedKafkaPlugin:
    """Self-managed Kafka ecosystem plugin.

    Creates and owns:
    - MetricsSource (Prometheus client) — shared by CostInput and Handler
    - KafkaAdminClient (if resource_source="admin_api") — owned by plugin, passed to handler
    - ConstructedCostInput — receives metrics_source
    - SelfManagedKafkaHandler — receives metrics_source + admin_client
    """

    def __init__(self) -> None:
        self._config: SelfManagedKafkaConfig | None = None
        self._metrics_source: MetricsSource | None = None
        self._admin_client: Any = None
        self._handler: SelfManagedKafkaHandler | None = None
        self._topic_attribution_provider: TopicAttributionProvider | None = None
        self._scope_evidence_by_window: dict[tuple[str, datetime, datetime], MetricsScopeEvidence] = {}
        self._scope_evidence_by_request: dict[MetricsScopeRequest, MetricsScopeEvidence] = {}
        self._scope_query_evidence: dict[MetricsScopeRequest, MetricsScopeEvidence] = {}

    @property
    def ecosystem(self) -> str:
        return "self_managed_kafka"

    def initialize(self, config: dict[str, Any]) -> None:
        """Initialize plugin with validated config.

        Creates:
        1. MetricsSource from config.metrics (always required)
        2. KafkaAdminClient if resource_source.source="admin_api"
        3. Handler with clients
        """
        logger.info(
            "plugin_initialize_started provider=self_managed_kafka%s",
            safe_log_context(stage="plugin_initialize", operation="initialize", outcome="started"),
        )
        self._topic_attribution_provider = None
        self._config = SelfManagedKafkaConfig.from_plugin_settings(config)
        self._scope_evidence_by_window.clear()
        self._scope_evidence_by_request.clear()
        self._scope_query_evidence.clear()
        # Always create MetricsSource (required for cost construction)
        self._metrics_source = create_metrics_source(self._config.metrics)

        # Create AdminClient if using admin_api for resource discovery
        if self._config.resource_source.source == "admin_api":
            from plugins.self_managed_kafka.gathering.admin_api import create_admin_client

            self._admin_client = create_admin_client(self._config.resource_source)

        # Principal evidence is per billing window; startup does not infer it from topic counters.
        self._handler = SelfManagedKafkaHandler(
            config=self._config,
            metrics_source=self._metrics_source,
            admin_client=self._admin_client,
            metrics_scope_evidence=self._scope_evidence_for_window,
        )
        if self._config.topic_attribution.enabled:
            from plugins.self_managed_kafka.overlays.topic_attribution import SelfManagedKafkaTopicAttributionProvider

            self._topic_attribution_provider = SelfManagedKafkaTopicAttributionProvider(
                config=self._config,
                metrics_source=self._metrics_source,
                inventory_is_partitionless=lambda: (
                    self._handler is not None and self._handler.admin_inventory_is_partitionless
                ),
            )
        logger.info(
            "plugin_initialize_completed provider=self_managed_kafka%s",
            safe_log_context(stage="plugin_initialize", operation="initialize", outcome="completed"),
        )

    def get_service_handlers(self) -> dict[str, ServiceHandler]:
        """Return service handlers keyed by service type."""
        if self._handler is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        return {"kafka": self._handler}

    def get_cost_input(self) -> CostInput:
        """Return ConstructedCostInput backed by Prometheus metrics."""
        if self._config is None or self._metrics_source is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        return ConstructedCostInput(
            self._config,
            self._metrics_source,
            inventory_is_partitionless=lambda: (
                self._handler is not None and self._handler.admin_inventory_is_partitionless
            ),
        )

    def get_overlay_config(self, name: str) -> OverlayConfig | None:
        """Return the typed configuration for supported optional overlays."""
        if name == "topic_attribution" and self._config is not None:
            return self._config.topic_attribution
        return None

    def get_topic_attribution_provider(self) -> TopicAttributionProvider | None:
        """Return the enabled self-managed topic-attribution provider."""
        return self._topic_attribution_provider

    def reset_topic_attribution_inventory_proof(self) -> None:
        """Require current Admin API discovery before absent storage is treated as zero."""
        if self._handler is not None:
            self._handler.clear_admin_inventory_proof()

    def topic_attribution_inventory_ready(self, shared_context: object | None) -> bool:
        """Report current-gather topic inventory readiness to the overlay lifecycle."""
        if self._config is None or not self._config.topic_attribution.enabled:
            return False
        if self._config.resource_source.source == "admin_api":
            return self._handler is not None and self._handler.admin_inventory_complete

        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        return (
            isinstance(shared_context, SMKSharedContext)
            and shared_context.discovered_brokers is not None
            and shared_context.discovered_topics is not None
        )

    def get_metrics_source(self) -> MetricsSource | None:
        """Return metrics source (always set after initialize)."""
        return self._metrics_source

    def get_fallback_allocator(self) -> CostAllocator | None:
        return None

    def build_shared_context(self, tenant_id: str) -> SMKSharedContext:
        """Build the cluster resource once for the gather cycle.

        When Prometheus is the resource source, discovers broker/topic labels in one round-trip.
        """
        if self._config is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "shared_context_started provider=self_managed_kafka%s",
                safe_log_context(
                    tenant_id=tenant_id,
                    stage="shared_context",
                    operation="build_shared_context",
                    outcome="started",
                ),
            )

        from plugins.self_managed_kafka.gathering.prometheus import gather_cluster_resource, run_broker_topic_discovery
        from plugins.self_managed_kafka.shared_context import SMKSharedContext

        cluster = gather_cluster_resource(
            ecosystem=self.ecosystem,
            tenant_id=tenant_id,
            cluster_id=self._config.cluster_id,
            broker_count=self._config.broker_count,
            region=self._config.region,
        )

        needs_prometheus = self._config.resource_source.source == "prometheus"

        if needs_prometheus and self._metrics_source is not None:
            step = timedelta(seconds=self._config.metrics_step_seconds)
            try:
                brokers, topics = run_broker_topic_discovery(
                    self._metrics_source,
                    metrics_identifier_label=self._config.metrics_identifier_label,
                    metrics_identifier=self._config.metrics_identifier,
                    step=step,
                    discovery_window_hours=self._config.discovery_window_hours,
                    include_topic_evidence=self._config.topic_attribution.enabled,
                )
                context = SMKSharedContext(
                    cluster_resource=cluster,
                    discovered_brokers=brokers,
                    discovered_topics=topics,
                )
                logger.info(
                    "shared_context_completed provider=self_managed_kafka brokers=%d topics=%d%s",
                    len(brokers),
                    len(topics),
                    safe_log_context(
                        tenant_id=tenant_id,
                        stage="shared_context",
                        operation="build_shared_context",
                        outcome="completed",
                    ),
                )
                return context
            except MetricsQueryError:
                logger.warning("self_managed_kafka: Combined discovery query failed. Discovery sets will be None.")

        logger.info(
            "shared_context_completed provider=self_managed_kafka%s",
            safe_log_context(
                tenant_id=tenant_id,
                stage="shared_context",
                operation="build_shared_context",
                outcome="completed",
            ),
        )
        return SMKSharedContext(cluster_resource=cluster)

    def prepare_gather_scope(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
    ) -> ScopeGateResult:
        return self._prepare_scope(tenant_id, start, end, uow)

    def begin_scope_gate_run(self) -> None:
        """Reset ephemeral scope and workload evidence for one pipeline run."""
        self._scope_evidence_by_window.clear()
        self._scope_evidence_by_request.clear()
        self._scope_query_evidence.clear()
        if self._handler is not None:
            self._handler.clear_principal_telemetry_evidence()
        provider = self._topic_attribution_provider
        if provider is not None and hasattr(provider, "clear_evidence_chunk"):
            provider.clear_evidence_chunk()

    def prepare_post_recovery_gather_scope(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
    ) -> ScopeGateResult:
        """Validate the complete gather scope after durable recovery state."""
        return self._prepare_scope(tenant_id, start, end, uow, force_full=True)

    def prepare_calculation_scope(
        self,
        tenant_id: str,
        windows: Sequence[tuple[datetime, datetime]],
        uow: UnitOfWork,
    ) -> ScopeGateResult:
        if not windows:
            return ScopeGateResult(ScopeGateDecision.ALLOW, self._require_config().cluster_id, "no billing windows")
        config = self._require_config()
        state = self._scope_state_repository(uow).get(self.ecosystem, tenant_id, config.cluster_id)
        if getattr(state, "status", None) == "open":
            return self._prepare_open_calculation_probe(tenant_id, windows, uow, state)
        recovery_result: ScopeGateResult | None = None
        for start, end in windows:
            result = self._prepare_scope(tenant_id, start, end, uow)
            if result.decision is not ScopeGateDecision.ALLOW:
                return result
            if result.recovery_start is not None and result.recovery_end is not None:
                recovery_result = result
        if recovery_result is not None:
            return recovery_result
        return ScopeGateResult(ScopeGateDecision.ALLOW, self._require_config().cluster_id, "target scope valid")

    def _prepare_open_calculation_probe(
        self,
        tenant_id: str,
        windows: Sequence[tuple[datetime, datetime]],
        uow: UnitOfWork,
        state: object,
    ) -> ScopeGateResult:
        """Probe the newest required calculation point once for an open scope."""
        config = self._require_config()
        metrics_source = self._require_metrics_source()
        step = timedelta(seconds=config.metrics_step_seconds)
        available_start = min(start for start, _ in windows)
        available_end = max(end for _, end in windows)
        newest = max(self._latest_scope_timestamp(start, end, step) for start, end in windows)
        detail_prefix = (
            f"expected Prometheus target label {config.metrics_identifier_label}={config.metrics_identifier}"
        )
        request = MetricsScopeRequest(
            tenant_id,
            config.metrics_identifier,
            config.metrics_identifier_label,
            step,
            newest,
            newest,
        )
        evidence = self._scope_query_evidence.get(request)
        if evidence is None:
            evidence = self._query_scope_request(
                request,
                metrics_source,
                detail_prefix,
                max_points=1,
            )
            self._scope_query_evidence[request] = evidence
        if evidence.status is not MetricsScopeStatus.VALID:
            return self._blocked_scope_result(evidence, state)
        recovery_start, retention_gap_start, retention_gap_end = self._recovery_window(
            state,
            available_start,
            available_end,
        )
        return ScopeGateResult(
            ScopeGateDecision.RECOVERY_READY,
            config.cluster_id,
            "target scope recovered",
            recovery_start=recovery_start,
            recovery_end=available_end,
            retention_gap_start=retention_gap_start,
            retention_gap_end=retention_gap_end,
            status=evidence.status.value,
            reason="target_scope_recovered",
            evidence=evidence,
        )

    def persist_scope_blocked(self, tenant_id: str, result: ScopeGateResult, uow: UnitOfWork) -> None:
        config = self._require_config()
        repository = self._scope_state_repository(uow)
        now = datetime.now(UTC)
        repository.open(
            ecosystem=self.ecosystem,
            tenant_id=tenant_id,
            cluster_id=result.scope_id,
            metrics_identifier_label=config.metrics_identifier_label,
            metrics_identifier=config.metrics_identifier,
            window_start=result.blocked_window_start or now,
            window_end=result.blocked_window_end or now,
            reason=result.reason or "target_scope_validation",
            status=result.status or MetricsScopeStatus.NOT_OBSERVED.value,
            detail=result.detail,
            opened_at=now,
        )

    def persist_scope_probe(self, tenant_id: str, result: ScopeGateResult, uow: UnitOfWork) -> None:
        self._scope_state_repository(uow).record_probe(
            self.ecosystem,
            tenant_id,
            result.scope_id,
            probed_at=datetime.now(UTC),
            status=result.status
            or (MetricsScopeStatus.VALID.value if result.decision is ScopeGateDecision.RECOVERY_READY else "blocked"),
        )

    def persist_scope_recovery(self, tenant_id: str, result: ScopeGateResult, uow: UnitOfWork) -> None:
        recovered_at = datetime.now(UTC)
        recovery_start = result.recovery_start or recovered_at
        repository = self._scope_state_repository(uow)
        repository.mark_recovering(
            self.ecosystem,
            tenant_id,
            result.scope_id,
            recovered_at=recovered_at,
            recovery_cursor_date=recovery_start.date(),
        )
        if result.retention_gap_start is not None and result.retention_gap_end is not None:
            repository.mark_retention_gap(
                self.ecosystem,
                tenant_id,
                result.scope_id,
                gap_start=result.retention_gap_start,
                gap_end=result.retention_gap_end,
            )

    def persist_scope_closed(self, tenant_id: str, result: ScopeGateResult, uow: UnitOfWork) -> None:
        self._scope_state_repository(uow).close(
            self.ecosystem,
            tenant_id,
            result.scope_id,
            recovery_cursor_date=result.recovery_end.date() if result.recovery_end is not None else None,
        )

    def _prepare_scope(
        self,
        tenant_id: str,
        start: datetime,
        end: datetime,
        uow: UnitOfWork,
        *,
        force_full: bool = False,
    ) -> ScopeGateResult:
        config = self._require_config()
        metrics_source = self._require_metrics_source()
        state = self._scope_state_repository(uow).get(self.ecosystem, tenant_id, config.cluster_id)
        step = timedelta(seconds=config.metrics_step_seconds)
        state_status = getattr(state, "status", None)
        detail_prefix = (
            f"expected Prometheus target label {config.metrics_identifier_label}={config.metrics_identifier}"
        )

        # An open breaker gets one newest-point probe.  The probe is deliberately
        # not promoted to aggregate evidence for the requested historical range.
        if state_status == "open" and not force_full:
            newest = self._latest_scope_timestamp(start, end, step)
            request = MetricsScopeRequest(
                tenant_id,
                config.metrics_identifier,
                config.metrics_identifier_label,
                step,
                newest,
                newest,
            )
            evidence = self._scope_query_evidence.get(request)
            if evidence is None:
                evidence = self._query_scope_request(
                    request,
                    metrics_source,
                    detail_prefix,
                    max_points=1,
                )
                self._scope_query_evidence[request] = evidence
            if evidence.status is not MetricsScopeStatus.VALID:
                return self._blocked_scope_result(evidence, state)
            recovery_start, retention_gap_start, retention_gap_end = self._recovery_window(state, start, end)
            return ScopeGateResult(
                ScopeGateDecision.RECOVERY_READY,
                config.cluster_id,
                "target scope recovered",
                recovery_start=recovery_start,
                recovery_end=end,
                retention_gap_start=retention_gap_start,
                retention_gap_end=retention_gap_end,
                status=evidence.status.value,
                reason="target_scope_recovered",
                evidence=evidence,
            )

        aggregate_request = MetricsScopeRequest(
            tenant_id,
            config.metrics_identifier,
            config.metrics_identifier_label,
            step,
            start,
            end,
        )
        cached_evidence = self._scope_evidence_by_request.get(aggregate_request)
        if cached_evidence is None:
            evidence = self._query_bounded_scope(
                aggregate_request,
                metrics_source,
                detail_prefix,
                config.historical_acquisition_chunk_days,
            )
            self._scope_evidence_by_request[aggregate_request] = evidence
        else:
            evidence = cached_evidence
        self._record_scope_evidence(tenant_id, evidence)
        if evidence.status is not MetricsScopeStatus.VALID:
            return self._blocked_scope_result(evidence, state)
        if state_status in {"recovering", "retention_gap"}:
            recovery_start, _, _ = self._recovery_window(state, start, end)
            return ScopeGateResult(
                ScopeGateDecision.ALLOW,
                config.cluster_id,
                "target scope recovery continuing",
                recovery_start=recovery_start,
                recovery_end=end,
                status=evidence.status.value,
                reason="target_scope_recovery_continuing",
                evidence=evidence,
            )
        return ScopeGateResult(
            ScopeGateDecision.ALLOW,
            config.cluster_id,
            "target scope valid",
            status=evidence.status.value,
            reason="target_scope_valid",
            evidence=evidence,
        )

    def _query_bounded_scope(
        self,
        request: MetricsScopeRequest,
        metrics_source: MetricsSource,
        detail_prefix: str,
        chunk_days: int,
    ) -> MetricsScopeEvidence:
        total_points = self._scope_point_count(request.start, request.end, request.step)
        if total_points <= 0:
            return MetricsScopeEvidence(
                request.metrics_identifier_label,
                request.metrics_identifier,
                request.start,
                request.end,
                MetricsScopeStatus.NOT_OBSERVED,
                f"{detail_prefix}: target coverage incomplete",
            )
        max_points = max(1, int(timedelta(days=chunk_days) // request.step) + 1)
        observed_points = 0
        saw_rows = False
        for offset in range(0, total_points, max_points):
            point_count = min(max_points, total_points - offset)
            group_start = request.start + offset * request.step
            group_end = group_start + (point_count - 1) * request.step
            physical = MetricsScopeRequest(
                request.tenant_id,
                request.metrics_identifier,
                request.metrics_identifier_label,
                request.step,
                group_start,
                group_end,
            )
            physical_evidence = self._scope_query_evidence.get(physical)
            if physical_evidence is None:
                physical_evidence = self._query_scope_request(
                    physical,
                    metrics_source,
                    detail_prefix,
                    max_points=max_points,
                )
                self._scope_query_evidence[physical] = physical_evidence
            if physical_evidence.status is MetricsScopeStatus.TRANSIENT_FAILURE:
                return MetricsScopeEvidence(
                    request.metrics_identifier_label,
                    request.metrics_identifier,
                    request.start,
                    request.end,
                    MetricsScopeStatus.TRANSIENT_FAILURE,
                    physical_evidence.detail,
                )
            if physical_evidence.status is MetricsScopeStatus.MISMATCH:
                return MetricsScopeEvidence(
                    request.metrics_identifier_label,
                    request.metrics_identifier,
                    request.start,
                    request.end,
                    MetricsScopeStatus.MISMATCH,
                    physical_evidence.detail,
                )
            if physical_evidence.status is MetricsScopeStatus.TARGET_DOWN:
                return MetricsScopeEvidence(
                    request.metrics_identifier_label,
                    request.metrics_identifier,
                    request.start,
                    request.end,
                    MetricsScopeStatus.TARGET_DOWN,
                    physical_evidence.detail,
                )
            if physical_evidence.status is MetricsScopeStatus.NOT_OBSERVED:
                return MetricsScopeEvidence(
                    request.metrics_identifier_label,
                    request.metrics_identifier,
                    request.start,
                    request.end,
                    MetricsScopeStatus.NOT_OBSERVED,
                    physical_evidence.detail,
                )
            saw_rows = saw_rows or physical_evidence.observed_target_count > 0
            observed_points += point_count

        if not saw_rows or observed_points != total_points:
            return MetricsScopeEvidence(
                request.metrics_identifier_label,
                request.metrics_identifier,
                request.start,
                request.end,
                MetricsScopeStatus.NOT_OBSERVED,
                f"{detail_prefix}: target coverage incomplete",
                observed_target_count=observed_points,
            )
        return MetricsScopeEvidence(
            request.metrics_identifier_label,
            request.metrics_identifier,
            request.start,
            request.end,
            MetricsScopeStatus.VALID,
            f"{detail_prefix}: target healthy",
            observed_target_count=observed_points,
        )

    def _query_scope_request(
        self,
        request: MetricsScopeRequest,
        metrics_source: MetricsSource,
        detail_prefix: str,
        *,
        max_points: int,
    ) -> MetricsScopeEvidence:
        query = MetricQuery(
            key="target_up",
            query_expression="up{}",
            label_keys=(request.metrics_identifier_label,),
            resource_label=request.metrics_identifier_label,
        )
        try:
            rows = metrics_source.query(
                queries=[query],
                start=request.start,
                end=request.end,
                step=request.step,
                resource_id_filter=request.metrics_identifier,
            ).get(query.key, [])
        except MetricsQueryError as exc:
            return MetricsScopeEvidence(
                request.metrics_identifier_label,
                request.metrics_identifier,
                request.start,
                request.end,
                MetricsScopeStatus.TRANSIENT_FAILURE,
                f"{detail_prefix}: {type(exc).__name__}",
            )
        return self._scope_evidence(
            rows=rows,
            label=request.metrics_identifier_label,
            identifier=request.metrics_identifier,
            start=request.start,
            end=request.end,
            step=request.step,
            detail_prefix=detail_prefix,
        )

    def _blocked_scope_result(
        self,
        evidence: MetricsScopeEvidence,
        state: object | None,
    ) -> ScopeGateResult:
        result = ScopeGateResult(
            ScopeGateDecision.BLOCKED,
            self._require_config().cluster_id,
            evidence.detail,
            blocked_window_start=evidence.window_start,
            blocked_window_end=evidence.window_end,
            status=evidence.status.value,
            reason="target_scope_validation",
            evidence=evidence,
            probe_only=getattr(state, "status", None) == "open",
        )
        if result.probe_only:
            return result
        raise ScopeBlockedError(result)

    @staticmethod
    def _scope_evidence(
        *,
        rows: Sequence[object],
        label: str,
        identifier: str,
        start: datetime,
        end: datetime,
        step: timedelta,
        detail_prefix: str,
    ) -> MetricsScopeEvidence:
        matching = [row for row in rows if getattr(row, "labels", {}).get(label) == identifier]
        if not matching:
            status = MetricsScopeStatus.MISMATCH if rows else MetricsScopeStatus.NOT_OBSERVED
            detail = f"{detail_prefix}: target label not observed" if rows else f"{detail_prefix}: target not observed"
            return MetricsScopeEvidence(label, identifier, start, end, status, detail)

        observed: dict[datetime, list[float]] = {}
        for row in matching:
            timestamp = getattr(row, "timestamp", None)
            value = getattr(row, "value", None)
            if isinstance(timestamp, datetime) and isinstance(value, (int, float)):
                observed.setdefault(timestamp, []).append(float(value))
        expected_count = SelfManagedKafkaPlugin._scope_point_count(start, end, step)
        if expected_count <= 0:
            return MetricsScopeEvidence(
                label,
                identifier,
                start,
                end,
                MetricsScopeStatus.NOT_OBSERVED,
                f"{detail_prefix}: target coverage incomplete",
            )
        expected_values: list[float] = []
        for offset in range(expected_count):
            timestamp = start + offset * step
            values_at_timestamp = observed.get(timestamp)
            if values_at_timestamp is None:
                return MetricsScopeEvidence(
                    label,
                    identifier,
                    start,
                    end,
                    MetricsScopeStatus.NOT_OBSERVED,
                    f"{detail_prefix}: target coverage incomplete",
                    observed_target_count=len(observed),
                )
            expected_values.extend(values_at_timestamp)
        values = expected_values
        if any(not isfinite(value) or value <= 0 for value in values):
            return MetricsScopeEvidence(
                label,
                identifier,
                start,
                end,
                MetricsScopeStatus.TARGET_DOWN,
                f"{detail_prefix}: target down or unhealthy",
                observed_target_count=len(observed),
            )
        return MetricsScopeEvidence(
            label,
            identifier,
            start,
            end,
            MetricsScopeStatus.VALID,
            f"{detail_prefix}: target healthy",
            observed_target_count=len(observed),
        )

    @staticmethod
    def _expected_scope_timestamps(start: datetime, end: datetime, step: timedelta) -> tuple[datetime, ...]:
        count = SelfManagedKafkaPlugin._scope_point_count(start, end, step)
        return tuple(start + offset * step for offset in range(count))

    @staticmethod
    def _scope_point_count(start: datetime, end: datetime, step: timedelta) -> int:
        if step <= timedelta(0) or end < start:
            return 0
        return int((end - start) // step) + 1

    @staticmethod
    def _latest_scope_timestamp(start: datetime, end: datetime, step: timedelta) -> datetime:
        count = SelfManagedKafkaPlugin._scope_point_count(start, end, step)
        if count <= 0:
            raise ValueError("scope window must contain at least one positive-step timestamp")
        return start + (count - 1) * step

    def _record_scope_evidence(self, tenant_id: str, evidence: MetricsScopeEvidence) -> None:
        self._scope_evidence_by_window[(tenant_id, evidence.window_start, evidence.window_end)] = evidence

    def _scope_evidence_for_window(
        self,
        tenant_id: str,
        billing_timestamp: datetime,
        billing_duration: timedelta,
    ) -> MetricsScopeEvidence | None:
        return self._scope_evidence_by_window.get((tenant_id, billing_timestamp, billing_timestamp + billing_duration))

    @staticmethod
    def _recovery_window(
        state: object,
        available_start: datetime,
        available_end: datetime,
    ) -> tuple[datetime, datetime | None, datetime | None]:
        first_blocked = getattr(state, "first_blocked_window_start", None) or available_start
        recovery_start = max(first_blocked, available_start)
        recovery_cursor_date = getattr(state, "recovery_cursor_date", None)
        if recovery_cursor_date is not None:
            recovery_start = max(
                recovery_start,
                datetime.combine(recovery_cursor_date, datetime.min.time(), tzinfo=available_start.tzinfo),
            )
        recovery_start = min(recovery_start, available_end)
        if first_blocked < available_start:
            return recovery_start, first_blocked, available_start
        return recovery_start, None, None

    def _require_config(self) -> SelfManagedKafkaConfig:
        if self._config is None or self._metrics_source is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        return self._config

    def _require_metrics_source(self) -> MetricsSource:
        if self._metrics_source is None:
            raise RuntimeError("Plugin not initialized. Call initialize() first.")
        return self._metrics_source

    @staticmethod
    def _scope_state_repository(uow: UnitOfWork) -> SelfManagedKafkaScopeStateRepository:
        repository = getattr(uow, "self_managed_kafka_scope_state", None)
        if repository is None:
            raise RuntimeError("Self-managed Kafka scope-state repository is unavailable")
        return cast("SelfManagedKafkaScopeStateRepository", repository)

    def get_storage_module(self) -> SelfManagedKafkaStorageModule:
        from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

        return SelfManagedKafkaStorageModule()

    def validate_plugin_settings(self, config: dict[str, Any]) -> None:
        """Validate plugin-specific config without creating live connections."""
        SelfManagedKafkaConfig.from_plugin_settings(config)

    def close(self) -> None:
        """Clean up resources (AdminClient connection, metrics source)."""
        if self._admin_client is not None:
            try:
                self._admin_client.close()
            except Exception as exc:
                logger.warning(
                    "plugin_cleanup_failed provider=self_managed_kafka%s",
                    safe_log_context(
                        stage="plugin_close",
                        operation="close_admin_client",
                        outcome="degraded",
                        retryable=False,
                        **safe_exception_context(exc),
                    ),
                )
            self._admin_client = None
        if self._metrics_source is not None:
            self._metrics_source.close()
            self._metrics_source = None
        self._topic_attribution_provider = None
