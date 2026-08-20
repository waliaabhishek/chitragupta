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
from core.plugin.protocols import ScopeBlockedError, ScopeGateDecision, ScopeGateResult
from plugins.self_managed_kafka.config import SelfManagedKafkaConfig
from plugins.self_managed_kafka.cost_input import ConstructedCostInput
from plugins.self_managed_kafka.handlers.kafka import SelfManagedKafkaHandler
from plugins.self_managed_kafka.telemetry_contract import MetricsScopeEvidence, MetricsScopeStatus

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
    from core.plugin.protocols import CostAllocator, CostInput, ServiceHandler
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
        self._scope_evidence_by_window: dict[tuple[str, datetime, datetime], MetricsScopeEvidence] = {}

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
        self._config = SelfManagedKafkaConfig.from_plugin_settings(config)
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
        return ConstructedCostInput(self._config, self._metrics_source)

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
        self._scope_evidence_by_window.clear()
        return self._prepare_scope(tenant_id, start, end, uow)

    def prepare_calculation_scope(
        self,
        tenant_id: str,
        windows: Sequence[tuple[datetime, datetime]],
        uow: UnitOfWork,
    ) -> ScopeGateResult:
        self._scope_evidence_by_window.clear()
        if self._handler is not None:
            self._handler.clear_principal_telemetry_evidence()
        if not windows:
            return ScopeGateResult(ScopeGateDecision.ALLOW, self._require_config().cluster_id, "no billing windows")
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

    def _prepare_scope(self, tenant_id: str, start: datetime, end: datetime, uow: UnitOfWork) -> ScopeGateResult:
        config = self._require_config()
        metrics_source = self._require_metrics_source()
        state = self._scope_state_repository(uow).get(self.ecosystem, tenant_id, config.cluster_id)
        query = MetricQuery(
            key="target_up",
            query_expression="up{}",
            label_keys=(config.metrics_identifier_label,),
            resource_label=config.metrics_identifier_label,
        )
        detail_prefix = (
            f"expected Prometheus target label {config.metrics_identifier_label}={config.metrics_identifier}"
        )
        try:
            rows = metrics_source.query(
                queries=[query],
                start=start,
                end=end,
                step=timedelta(seconds=config.metrics_step_seconds),
                resource_id_filter=config.metrics_identifier,
            ).get(query.key, [])
        except MetricsQueryError as exc:
            evidence = MetricsScopeEvidence(
                label=config.metrics_identifier_label,
                identifier=config.metrics_identifier,
                window_start=start,
                window_end=end,
                status=MetricsScopeStatus.TRANSIENT_FAILURE,
                detail=f"{detail_prefix}: {type(exc).__name__}",
            )
            self._record_scope_evidence(tenant_id, evidence)
            return self._blocked_scope_result(evidence, state)

        evidence = self._scope_evidence(
            rows=rows,
            label=config.metrics_identifier_label,
            identifier=config.metrics_identifier,
            start=start,
            end=end,
            step=timedelta(seconds=config.metrics_step_seconds),
            detail_prefix=detail_prefix,
        )
        self._record_scope_evidence(tenant_id, evidence)
        if evidence.status is not MetricsScopeStatus.VALID:
            return self._blocked_scope_result(evidence, state)
        state_status = getattr(state, "status", None)
        if state_status == "open":
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

        expected = SelfManagedKafkaPlugin._expected_scope_timestamps(start, end, step)
        observed: dict[datetime, list[float]] = {}
        for row in matching:
            timestamp = getattr(row, "timestamp", None)
            value = getattr(row, "value", None)
            if isinstance(timestamp, datetime) and isinstance(value, (int, float)):
                observed.setdefault(timestamp, []).append(float(value))
        if any(timestamp not in observed for timestamp in expected):
            return MetricsScopeEvidence(
                label,
                identifier,
                start,
                end,
                MetricsScopeStatus.NOT_OBSERVED,
                f"{detail_prefix}: target coverage incomplete",
                observed_target_count=len(observed),
            )
        values = [value for timestamp in expected for value in observed[timestamp]]
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
        timestamps: list[datetime] = []
        current = start
        while current <= end:
            timestamps.append(current)
            current += step
        return tuple(timestamps)

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
