"""Persistence tests for self-managed Kafka scope state."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule


@pytest.fixture
def backend(tmp_path: Path) -> SQLModelBackend:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'scope-state.db'}",
        SelfManagedKafkaStorageModule(),
        use_migrations=True,
    )
    backend.create_tables()
    yield backend
    backend.dispose()


def _open_state(repository: Any, *, start: datetime, end: datetime, status: str = "mismatch") -> None:
    repository.open(
        ecosystem="self_managed_kafka",
        tenant_id="tenant-1",
        cluster_id="billing-cluster-a",
        metrics_identifier_label="kafka_cluster_id",
        metrics_identifier="kraft-a-001",
        window_start=start,
        window_end=end,
        reason="mismatch",
        status=status,
        detail="expected Prometheus target label kafka_cluster_id=kraft-a-001",
        opened_at=end,
    )


class TestScopeStateRepository:
    def test_open_state_survives_commit_close_and_reopen(self, backend: SQLModelBackend) -> None:
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = datetime(2026, 8, 2, tzinfo=UTC)

        with backend.create_unit_of_work() as uow:
            _open_state(uow.self_managed_kafka_scope_state, start=start, end=end)
            uow.commit()

        with backend.create_unit_of_work() as reopened_uow:
            state = reopened_uow.self_managed_kafka_scope_state.get(
                "self_managed_kafka", "tenant-1", "billing-cluster-a"
            )
            assert state is not None
            assert state.status == "open"
            assert state.metrics_identifier_label == "kafka_cluster_id"
            assert state.metrics_identifier == "kraft-a-001"
            assert state.first_blocked_window_start == start
            assert state.first_blocked_window_end == end
            assert state.last_failure_reason == "mismatch"
            assert state.last_failure_status == "mismatch"
            assert state.last_failure_detail == "expected Prometheus target label kafka_cluster_id=kraft-a-001"

    def test_uncommitted_scope_state_is_rolled_back_from_a_separate_unit_of_work(
        self, backend: SQLModelBackend
    ) -> None:
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = datetime(2026, 8, 2, tzinfo=UTC)

        with backend.create_unit_of_work() as uow:
            _open_state(uow.self_managed_kafka_scope_state, start=start, end=end)

        with backend.create_unit_of_work() as separate_uow:
            state = separate_uow.self_managed_kafka_scope_state.get(
                "self_managed_kafka", "tenant-1", "billing-cluster-a"
            )

        assert state is None

    def test_probe_recovery_retention_and_close_survive_a_separate_commit(self, backend: SQLModelBackend) -> None:
        blocked_start = datetime(2026, 7, 1, tzinfo=UTC)
        blocked_end = datetime(2026, 7, 2, tzinfo=UTC)
        recovered_at = datetime(2026, 8, 1, tzinfo=UTC)
        retained_start = datetime(2026, 7, 15, tzinfo=UTC)

        with backend.create_unit_of_work() as uow:
            repository = uow.self_managed_kafka_scope_state
            _open_state(repository, start=blocked_start, end=blocked_end, status="target_down")
            repository.record_probe(
                "self_managed_kafka",
                "tenant-1",
                "billing-cluster-a",
                probed_at=recovered_at,
                status="valid",
            )
            repository.mark_recovering(
                "self_managed_kafka",
                "tenant-1",
                "billing-cluster-a",
                recovered_at=recovered_at,
                recovery_cursor_date=retained_start.date(),
            )
            repository.mark_retention_gap(
                "self_managed_kafka",
                "tenant-1",
                "billing-cluster-a",
                gap_start=blocked_start,
                gap_end=retained_start,
            )
            repository.close(
                "self_managed_kafka",
                "tenant-1",
                "billing-cluster-a",
                recovery_cursor_date=recovered_at.date(),
            )
            uow.commit()

        with backend.create_unit_of_work() as reopened_uow:
            state = reopened_uow.self_managed_kafka_scope_state.get(
                "self_managed_kafka", "tenant-1", "billing-cluster-a"
            )
            assert state is not None
            assert state.last_probe_at == recovered_at
            assert state.last_probe_status == "valid"
            assert state.status == "closed"
            assert state.recovered_at == recovered_at
            assert state.recovery_cursor_date == recovered_at.date()
            assert state.retention_gap_start == blocked_start
            assert state.retention_gap_end == retained_start

    def test_reopen_clears_previous_recovery_and_retention_interval(self, backend: SQLModelBackend) -> None:
        blocked_start = datetime(2026, 7, 1, tzinfo=UTC)
        blocked_end = datetime(2026, 7, 2, tzinfo=UTC)
        reopened_at = datetime(2026, 8, 1, tzinfo=UTC)

        with backend.create_unit_of_work() as uow:
            repository = uow.self_managed_kafka_scope_state
            _open_state(repository, start=blocked_start, end=blocked_end, status="target_down")
            repository.mark_recovering(
                "self_managed_kafka",
                "tenant-1",
                "billing-cluster-a",
                recovered_at=reopened_at,
                recovery_cursor_date=reopened_at.date(),
            )
            repository.mark_retention_gap(
                "self_managed_kafka",
                "tenant-1",
                "billing-cluster-a",
                gap_start=blocked_start,
                gap_end=reopened_at,
            )
            _open_state(repository, start=reopened_at, end=reopened_at, status="not_observed")
            uow.commit()

        with backend.create_unit_of_work() as reopened_uow:
            state = reopened_uow.self_managed_kafka_scope_state.get(
                "self_managed_kafka", "tenant-1", "billing-cluster-a"
            )
            assert state is not None
            assert state.status == "open"
            assert state.first_blocked_window_start == reopened_at
            assert state.recovered_at is None
            assert state.recovery_cursor_date is None
            assert state.retention_gap_start is None
            assert state.retention_gap_end is None
