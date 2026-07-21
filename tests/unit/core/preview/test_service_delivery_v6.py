from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Never

import pytest

from core.preview.artifacts import PreviewArtifactStore
from core.preview.persistence import (
    PreviewInterruptionRecoveryResult,
    PreviewRequestRepository,
    PreviewStaleLeaseRecoveryResult,
    PreviewStorageBackend,
)
from core.preview.service import PreviewExecutor, PreviewRuntime
from tests.unit.core.preview.conftest import preview_module

if TYPE_CHECKING:
    from core.preview.models import (
        PreviewArtifactMetadata,
        PreviewArtifactPayload,
        PreviewDiagnostic,
        PreviewRequest,
        PreviewSourceSnapshot,
        PreviewStoredPackage,
    )
    from core.preview.persistence import PreviewExpiredArtifact, PreviewWriteUnitOfWork


class ImmediateExecutor:
    def submit(self, task: Callable[[], None]) -> Future[None]:
        future: Future[None] = Future()
        future.set_result(None)
        del task
        return future

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        del wait, cancel_futures


@dataclass
class RecordingStore:
    cleanup_calls: int = 0
    fail_cleanup: bool = False

    def stage_data_files(
        self,
        *,
        request_id: str,
        data_files: tuple[PreviewArtifactPayload, ...],
    ) -> Never:
        del request_id, data_files
        raise AssertionError("not used")

    def read_manifest(self, storage_key: str, metadata: PreviewArtifactMetadata) -> Never:
        del storage_key, metadata
        raise AssertionError("not used")

    def read_file(self, storage_key: str, metadata: PreviewArtifactMetadata) -> Never:
        del storage_key, metadata
        raise AssertionError("not used")

    def open_archive(
        self,
        *,
        storage_key: str,
        manifest: PreviewArtifactMetadata,
        files: tuple[PreviewArtifactMetadata, ...],
    ) -> Never:
        del storage_key, manifest, files
        raise AssertionError("not used")

    def delete_package(self, *, storage_key: str) -> bool:
        del storage_key
        return False

    def cleanup_staging(self) -> int:
        self.cleanup_calls += 1
        if self.fail_cleanup:
            raise OSError("staging unavailable")
        return 0

    def close(self) -> None:
        return None


@dataclass
class RecordingRequests:
    owner: str
    fail_recovery: bool = False
    recovery_calls: list[tuple[str, str, datetime, datetime]] = field(default_factory=list)

    def create_queued(
        self,
        request: PreviewRequest,
        *,
        worker_id: str | None = None,
        lease_expires_at: datetime | None = None,
    ) -> PreviewRequest:
        del worker_id, lease_expires_at
        return request

    def get_for_owner(self, request_id: str, ecosystem: str, tenant_id: str) -> PreviewRequest | None:
        del request_id, ecosystem, tenant_id
        return None

    def list_recent_for_owner(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        limit: int,
        cursor_request_id: str | None,
    ) -> Never:
        del ecosystem, tenant_id, limit, cursor_request_id
        raise AssertionError("not used")

    def mark_running(
        self,
        request_id: str,
        started_at: datetime,
        *,
        worker_id: str | None = None,
        lease_expires_at: datetime | None = None,
    ) -> PreviewRequest | None:
        del request_id, started_at, worker_id, lease_expires_at
        return None

    def renew_lease(self, request_id: str, worker_id: str, lease_expires_at: datetime) -> bool:
        del request_id, worker_id, lease_expires_at
        return False

    def mark_ready(
        self,
        request_id: str,
        completed_at: datetime,
        expires_at: datetime,
        source_snapshot: PreviewSourceSnapshot,
        stored_package: PreviewStoredPackage,
        *,
        worker_id: str | None = None,
    ) -> bool:
        del request_id, completed_at, expires_at, source_snapshot, stored_package, worker_id
        return False

    def mark_failed(
        self,
        request_id: str,
        completed_at: datetime,
        diagnostic: PreviewDiagnostic,
        *,
        worker_id: str | None = None,
    ) -> bool:
        del request_id, completed_at, diagnostic, worker_id
        return False

    def expire_ready_due(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        now: datetime,
        limit: int,
    ) -> tuple[PreviewExpiredArtifact, ...]:
        del ecosystem, tenant_id, now, limit
        return ()

    def expire_ready_request(
        self,
        *,
        request_id: str,
        ecosystem: str,
        tenant_id: str,
        now: datetime,
    ) -> PreviewExpiredArtifact | None:
        del request_id, ecosystem, tenant_id, now
        return None

    def list_expired_artifacts(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        limit: int,
    ) -> tuple[PreviewExpiredArtifact, ...]:
        del ecosystem, tenant_id, limit
        return ()

    def clear_expired_storage_key(self, request_id: str, storage_key: str) -> bool:
        del request_id, storage_key
        return False

    def fail_interrupted_before(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        startup_at: datetime,
        lease_stale_at: datetime,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewInterruptionRecoveryResult:
        del diagnostic
        self.recovery_calls.append((ecosystem, tenant_id, startup_at, lease_stale_at))
        if self.fail_recovery:
            raise OSError(f"{self.owner} unavailable")
        return PreviewInterruptionRecoveryResult(failed_count=0, protected_count=0)

    def fail_stale_foreign_leases(
        self,
        *,
        ecosystem: str,
        tenant_id: str,
        current_worker_id: str,
        lease_stale_at: datetime,
        limit: int,
        diagnostic: PreviewDiagnostic,
    ) -> PreviewStaleLeaseRecoveryResult:
        del ecosystem, tenant_id, current_worker_id, lease_stale_at, limit, diagnostic
        if self.fail_recovery:
            raise OSError(f"{self.owner} unavailable")
        return PreviewStaleLeaseRecoveryResult(failed_count=0, has_more=False)


@dataclass
class RecordingWriteUow:
    requests: PreviewRequestRepository
    commits: int = 0

    def __enter__(self) -> RecordingWriteUow:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        return None


@dataclass
class RecordingBackend:
    requests: RecordingRequests

    def create_preview_write_unit_of_work(self) -> PreviewWriteUnitOfWork:
        return RecordingWriteUow(self.requests)

    def create_preview_read_unit_of_work(self) -> Never:
        raise AssertionError("not used")


def _runtime(store: RecordingStore) -> PreviewRuntime:
    return PreviewRuntime(
        artifact_store=store,
        max_workers=1,
        max_csv_file_bytes=None,
        startup_at=datetime(2026, 7, 3, 1, 2, 3, 900000, tzinfo=UTC),
        clock=lambda: datetime(2026, 7, 3, 1, 2, 3, tzinfo=UTC),
        configured_owner_keys=(
            ("tenant-a", "confluent_cloud", "shared-provider-tenant"),
            ("tenant-b", "confluent_cloud", "shared-provider-tenant"),
        ),
        executor=ImmediateExecutor(),
    )


def test_delivery_recovery_doubles_satisfy_full_production_protocols() -> None:
    store = RecordingStore()
    requests = RecordingRequests("tenant-a")
    backend = RecordingBackend(requests)

    assert isinstance(store, PreviewArtifactStore)
    assert isinstance(requests, PreviewRequestRepository)
    assert isinstance(backend, PreviewStorageBackend)
    assert isinstance(ImmediateExecutor(), PreviewExecutor)


def test_recovery_is_triple_keyed_and_failure_cannot_block_or_clear_same_provider_peer() -> None:
    service = preview_module("service")
    store = RecordingStore()
    runtime = _runtime(store)
    a_requests = RecordingRequests("tenant-a", fail_recovery=True)
    b_requests = RecordingRequests("tenant-b")
    backend_a = RecordingBackend(a_requests)
    backend_b = RecordingBackend(b_requests)
    try:
        runtime.ensure_staging_recovered()
        with pytest.raises(service.PreviewRecoveryUnavailable):
            runtime.ensure_owner_recovered(
                backend=backend_a,
                tenant_name="tenant-a",
                ecosystem="confluent_cloud",
                tenant_id="shared-provider-tenant",
            )
        runtime.ensure_owner_recovered(
            backend=backend_b,
            tenant_name="tenant-b",
            ecosystem="confluent_cloud",
            tenant_id="shared-provider-tenant",
        )
        runtime.ensure_owner_recovered(
            backend=backend_b,
            tenant_name="tenant-b",
            ecosystem="confluent_cloud",
            tenant_id="shared-provider-tenant",
        )
        a_requests.fail_recovery = False
        runtime.ensure_owner_recovered(
            backend=backend_a,
            tenant_name="tenant-a",
            ecosystem="confluent_cloud",
            tenant_id="shared-provider-tenant",
        )

        assert store.cleanup_calls == 1
        assert len(a_requests.recovery_calls) == 2
        assert len(b_requests.recovery_calls) == 1
        assert {call[2] for call in [*a_requests.recovery_calls, *b_requests.recovery_calls]} == {
            datetime(2026, 7, 3, 1, 2, 3, tzinfo=UTC)
        }
        assert {call[3] for call in [*a_requests.recovery_calls, *b_requests.recovery_calls]} == {
            datetime(2026, 7, 3, 1, 2, 3, tzinfo=UTC)
        }
    finally:
        runtime.close()


def test_global_staging_failure_keeps_every_owner_pending_until_later_success() -> None:
    service = preview_module("service")
    store = RecordingStore(fail_cleanup=True)
    runtime = _runtime(store)
    backend = RecordingBackend(RecordingRequests("tenant-a"))
    try:
        with pytest.raises(service.PreviewRecoveryUnavailable):
            runtime.ensure_owner_recovered(
                backend=backend,
                tenant_name="tenant-a",
                ecosystem="confluent_cloud",
                tenant_id="shared-provider-tenant",
            )
        assert backend.requests.recovery_calls == []

        store.fail_cleanup = False
        runtime.ensure_owner_recovered(
            backend=backend,
            tenant_name="tenant-a",
            ecosystem="confluent_cloud",
            tenant_id="shared-provider-tenant",
        )
        assert store.cleanup_calls == 2
        assert len(backend.requests.recovery_calls) == 1
    finally:
        runtime.close()
