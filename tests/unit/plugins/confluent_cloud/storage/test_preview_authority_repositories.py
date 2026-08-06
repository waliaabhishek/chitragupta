from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

import pytest

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

NOW = datetime(2026, 7, 22, 12, tzinfo=UTC)
START = datetime(2026, 7, 1, tzinfo=UTC)
END = datetime(2026, 7, 2, tzinfo=UTC)


class _ForeignSourceAttemptFinalStatus(StrEnum):
    COMPLETE = "complete"


class _ForeignSourceAttemptFailureReason(StrEnum):
    PERSISTENCE_FAILED = "persistence_failed"


@pytest.fixture
def evidence_backend(tmp_path: Path):
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'authority.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()
    yield backend
    backend.dispose()


def test_organization_attempt_is_durable_and_latest_sequence_wins(evidence_backend: object) -> None:
    from core.preview.organization_authority import (
        OrganizationAuthorityFailureReason,
        OrganizationAuthorityFinalStatus,
    )

    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
        first = uow.organization_authority.begin("confluent_cloud", "tenant-1", NOW)
        completed = uow.organization_authority.finalize(
            first.attempt_sequence,
            OrganizationAuthorityFinalStatus.AVAILABLE,
            completed_at=NOW + timedelta(seconds=1),
            organization_id="org-1",
            reason=None,
        )
        uow.commit()
    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
        second = uow.organization_authority.begin("confluent_cloud", "tenant-1", NOW + timedelta(minutes=1))
        failed = uow.organization_authority.finalize(
            second.attempt_sequence,
            OrganizationAuthorityFinalStatus.UNAVAILABLE,
            completed_at=NOW + timedelta(minutes=1, seconds=1),
            organization_id=None,
            reason=OrganizationAuthorityFailureReason.PROVIDER_ERROR,
        )
        uow.commit()
    with evidence_backend.create_preview_generation_read_unit_of_work() as uow:
        latest = uow.organization_authority.get_latest("confluent_cloud", "tenant-1")

    assert completed.organization_id == "org-1"
    assert failed.attempt_sequence > completed.attempt_sequence
    assert latest == failed


def test_pending_organization_attempt_survives_restart_and_fails_closed(evidence_backend: object) -> None:
    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
        pending = uow.organization_authority.begin("confluent_cloud", "tenant-1", NOW)
        uow.commit()
    with evidence_backend.create_preview_generation_read_unit_of_work() as uow:
        loaded = uow.organization_authority.get_latest("confluent_cloud", "tenant-1")

    assert loaded == pending
    assert loaded.status.value == "pending"
    assert loaded.completed_at is None


def test_organization_finalization_is_compare_and_set(evidence_backend: object) -> None:
    from core.preview.organization_authority import (
        OrganizationAuthorityFinalStatus,
        PreviewOrganizationAuthorityConflictError,
    )

    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
        pending = uow.organization_authority.begin("confluent_cloud", "tenant-1", NOW)
        uow.organization_authority.finalize(
            pending.attempt_sequence,
            OrganizationAuthorityFinalStatus.AVAILABLE,
            completed_at=NOW + timedelta(seconds=1),
            organization_id="org-1",
            reason=None,
        )
        with pytest.raises(PreviewOrganizationAuthorityConflictError):
            uow.organization_authority.finalize(
                pending.attempt_sequence,
                OrganizationAuthorityFinalStatus.AVAILABLE,
                completed_at=NOW + timedelta(seconds=2),
                organization_id="org-2",
                reason=None,
            )


def test_source_authority_ignores_aborted_attempt_but_not_newer_failed_attempt(
    evidence_backend: object,
) -> None:
    from core.preview.evidence import SourceAttemptFailureReason, SourceAttemptFinalStatus

    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
        complete = uow.source_readiness.begin_attempt(
            "confluent_cloud",
            "tenant-1",
            "token-complete",
            START,
            END,
            NOW,
        )
        complete = uow.source_readiness.finalize_attempt(
            complete.attempt_sequence,
            SourceAttemptFinalStatus.COMPLETE,
            completed_at=NOW + timedelta(seconds=1),
            reason=None,
        )
        aborted = uow.source_readiness.begin_attempt(
            "confluent_cloud",
            "tenant-1",
            "token-aborted",
            START,
            END,
            NOW + timedelta(minutes=1),
        )
        uow.source_readiness.finalize_attempt(
            aborted.attempt_sequence,
            SourceAttemptFinalStatus.ABORTED,
            completed_at=NOW + timedelta(minutes=1, seconds=1),
            reason=SourceAttemptFailureReason.GENERIC_GATHER_FAILED,
        )
        uow.commit()
    with evidence_backend.create_preview_generation_read_unit_of_work() as uow:
        assert uow.source_readiness.get_current_authority("confluent_cloud", "tenant-1") == complete

    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
        failed = uow.source_readiness.begin_attempt(
            "confluent_cloud",
            "tenant-1",
            "token-failed",
            START,
            END,
            NOW + timedelta(minutes=2),
        )
        failed = uow.source_readiness.finalize_attempt(
            failed.attempt_sequence,
            SourceAttemptFinalStatus.FAILED,
            completed_at=NOW + timedelta(minutes=2, seconds=1),
            reason=SourceAttemptFailureReason.ATTEMPT_BEGIN_FAILED,
        )
        uow.commit()
    with evidence_backend.create_preview_generation_read_unit_of_work() as uow:
        assert uow.source_readiness.get_current_authority("confluent_cloud", "tenant-1") == failed


def test_source_begin_is_idempotent_by_owner_and_refresh_token(evidence_backend: object) -> None:
    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
        first = uow.source_readiness.begin_attempt(
            "confluent_cloud",
            "tenant-1",
            "same-token",
            START,
            END,
            NOW,
        )
        second = uow.source_readiness.begin_attempt(
            "confluent_cloud",
            "tenant-1",
            "same-token",
            START,
            END,
            NOW,
        )
        uow.commit()

    assert second == first
    assert first.attempt_sequence > 0


@pytest.mark.parametrize(
    "status",
    ["complete", object(), _ForeignSourceAttemptFinalStatus.COMPLETE],
)
def test_source_repository_rejects_raw_arbitrary_and_foreign_final_status(
    evidence_backend: object,
    status: object,
) -> None:
    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
        pending = uow.source_readiness.begin_attempt(
            "confluent_cloud",
            "tenant-1",
            f"invalid-status-{type(status).__name__}",
            START,
            END,
            NOW,
        )

        with pytest.raises(ValueError, match="invalid source attempt final status"):
            uow.source_readiness.finalize_attempt(
                pending.attempt_sequence,
                status,
                completed_at=NOW + timedelta(seconds=1),
                reason=None,
            )


@pytest.mark.parametrize(
    "reason",
    ["persistence_failed", object(), _ForeignSourceAttemptFailureReason.PERSISTENCE_FAILED],
)
def test_source_repository_rejects_raw_arbitrary_and_foreign_failure_reason(
    evidence_backend: object,
    reason: object,
) -> None:
    from core.preview.evidence import SourceAttemptFinalStatus

    with evidence_backend.create_preview_evidence_unit_of_work() as uow:
        pending = uow.source_readiness.begin_attempt(
            "confluent_cloud",
            "tenant-1",
            f"invalid-reason-{type(reason).__name__}",
            START,
            END,
            NOW,
        )

        with pytest.raises(ValueError, match="invalid source attempt failure reason"):
            uow.source_readiness.finalize_attempt(
                pending.attempt_sequence,
                SourceAttemptFinalStatus.FAILED,
                completed_at=NOW + timedelta(seconds=1),
                reason=reason,
            )
