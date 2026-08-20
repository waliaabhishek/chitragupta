from __future__ import annotations

import logging
from datetime import date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlmodel import Session

from core.storage.backends.sqlmodel.repositories import (
    SQLModelBillingRepository as SMKBillingRepository,
)
from core.storage.backends.sqlmodel.repositories import (
    SQLModelIdentityRepository as SMKIdentityRepository,
)
from core.storage.backends.sqlmodel.repositories import (
    SQLModelResourceRepository as SMKResourceRepository,
)
from plugins.self_managed_kafka.storage.tables import SelfManagedKafkaScopeStateTable

logger = logging.getLogger(__name__)


class SelfManagedKafkaScopeStateRepository:
    """Repository for target-scope breaker state."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get(self, ecosystem: str, tenant_id: str, cluster_id: str) -> SelfManagedKafkaScopeStateTable | None:
        return self._session.get(SelfManagedKafkaScopeStateTable, (ecosystem, tenant_id, cluster_id))

    def open(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        metrics_identifier_label: str,
        metrics_identifier: str,
        window_start: datetime,
        window_end: datetime,
        reason: str,
        status: str,
        detail: str,
        opened_at: datetime,
    ) -> None:
        state = self.get(ecosystem, tenant_id, cluster_id)
        if state is None:
            state = SelfManagedKafkaScopeStateTable(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                cluster_id=cluster_id,
                metrics_identifier_label=metrics_identifier_label,
                metrics_identifier=metrics_identifier,
                status="open",
                opened_at=opened_at,
                first_blocked_window_start=window_start,
                first_blocked_window_end=window_end,
                last_failure_reason=reason,
                last_failure_status=status,
                last_failure_detail=detail,
            )
            self._session.add(state)
            return
        state.status = "open"
        state.metrics_identifier_label = metrics_identifier_label
        state.metrics_identifier = metrics_identifier
        state.opened_at = opened_at
        state.first_blocked_window_start = window_start
        state.first_blocked_window_end = window_end
        state.last_failure_reason = reason
        state.last_failure_status = status
        state.last_failure_detail = detail
        state.last_probe_at = None
        state.last_probe_status = None
        state.recovered_at = None
        state.recovery_cursor_date = None
        state.retention_gap_start = None
        state.retention_gap_end = None

    def record_probe(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        *,
        probed_at: datetime,
        status: str,
    ) -> None:
        state = self._require(ecosystem, tenant_id, cluster_id)
        state.last_probe_at = probed_at
        state.last_probe_status = status

    def mark_recovering(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        *,
        recovered_at: datetime,
        recovery_cursor_date: date,
    ) -> None:
        state = self._require(ecosystem, tenant_id, cluster_id)
        state.status = "recovering"
        state.recovered_at = recovered_at
        state.recovery_cursor_date = recovery_cursor_date

    def mark_retention_gap(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        *,
        gap_start: datetime,
        gap_end: datetime,
    ) -> None:
        state = self._require(ecosystem, tenant_id, cluster_id)
        state.status = "retention_gap"
        state.retention_gap_start = gap_start
        state.retention_gap_end = gap_end

    def close(
        self,
        ecosystem: str,
        tenant_id: str,
        cluster_id: str,
        *,
        recovery_cursor_date: date | None = None,
    ) -> None:
        state = self._require(ecosystem, tenant_id, cluster_id)
        if recovery_cursor_date is not None:
            state.recovery_cursor_date = recovery_cursor_date
        state.status = "closed"

    def _require(self, ecosystem: str, tenant_id: str, cluster_id: str) -> SelfManagedKafkaScopeStateTable:
        state = self.get(ecosystem, tenant_id, cluster_id)
        if state is None:
            raise ValueError("self-managed Kafka scope state does not exist")
        return state


__all__ = [
    "SMKBillingRepository",
    "SMKIdentityRepository",
    "SMKResourceRepository",
    "SelfManagedKafkaScopeStateRepository",
]
