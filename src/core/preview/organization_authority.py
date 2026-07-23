from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


class OrganizationAuthorityAttemptStatus(StrEnum):
    PENDING = "pending"
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    CONFLICTING = "conflicting"


class OrganizationAuthorityFinalStatus(StrEnum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    CONFLICTING = "conflicting"


class OrganizationAuthorityFailureReason(StrEnum):
    PROVIDER_ERROR = "provider_error"
    CAPABILITY_UNAVAILABLE = "capability_unavailable"
    RESOURCE_PERSISTENCE_FAILED = "resource_persistence_failed"
    INVALID_CARDINALITY = "invalid_cardinality"
    BINDING_CONFLICT = "binding_conflict"


class PreviewOrganizationAuthorityDecodeError(ValueError):
    """Persisted organization authority does not satisfy the closed codec."""


class PreviewOrganizationAuthorityConflictError(RuntimeError):
    """An organization authority attempt was concurrently finalized."""


def _aware(value: object) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


@dataclass(frozen=True)
class OrganizationAuthorityAttempt:
    attempt_sequence: int
    ecosystem: str
    tenant_id: str
    status: OrganizationAuthorityAttemptStatus
    started_at: datetime
    completed_at: datetime | None
    organization_id: str | None
    failure_reason: OrganizationAuthorityFailureReason | None

    def __post_init__(self) -> None:
        if type(self.attempt_sequence) is not int or self.attempt_sequence <= 0:
            raise ValueError("attempt_sequence must be positive")
        if (
            not isinstance(self.ecosystem, str)
            or not isinstance(self.tenant_id, str)
            or not self.ecosystem.strip()
            or not self.tenant_id.strip()
        ):
            raise ValueError("organization authority owner must not be blank")
        if not isinstance(self.status, OrganizationAuthorityAttemptStatus):
            raise ValueError("organization authority status is invalid")
        if self.failure_reason is not None and not isinstance(self.failure_reason, OrganizationAuthorityFailureReason):
            raise ValueError("organization authority reason is invalid")
        if not _aware(self.started_at):
            raise ValueError("started_at must be timezone-aware")
        if self.completed_at is not None and not _aware(self.completed_at):
            raise ValueError("completed_at must be timezone-aware")
        if self.status is OrganizationAuthorityAttemptStatus.PENDING:
            if self.completed_at is not None or self.organization_id is not None or self.failure_reason is not None:
                raise ValueError("pending organization authority must not have completion fields")
            return
        if self.completed_at is None or self.completed_at < self.started_at:
            raise ValueError("terminal organization authority requires a valid completion time")
        if self.status is OrganizationAuthorityAttemptStatus.AVAILABLE:
            if (
                not isinstance(self.organization_id, str)
                or not self.organization_id.strip()
                or self.failure_reason is not None
            ):
                raise ValueError("available organization authority requires only an organization id")
            return
        if self.organization_id is not None or self.failure_reason is None:
            raise ValueError("failed organization authority requires only a failure reason")
        allowed = {
            OrganizationAuthorityAttemptStatus.UNAVAILABLE: {
                OrganizationAuthorityFailureReason.PROVIDER_ERROR,
                OrganizationAuthorityFailureReason.CAPABILITY_UNAVAILABLE,
                OrganizationAuthorityFailureReason.RESOURCE_PERSISTENCE_FAILED,
                OrganizationAuthorityFailureReason.INVALID_CARDINALITY,
            },
            OrganizationAuthorityAttemptStatus.CONFLICTING: {
                OrganizationAuthorityFailureReason.INVALID_CARDINALITY,
                OrganizationAuthorityFailureReason.BINDING_CONFLICT,
            },
        }
        if self.failure_reason not in allowed[self.status]:
            raise ValueError("organization authority status and reason do not match")


@runtime_checkable
class PreviewOrganizationAuthorityReader(Protocol):
    def get_latest(self, ecosystem: str, tenant_id: str) -> OrganizationAuthorityAttempt | None: ...


@runtime_checkable
class PreviewOrganizationAuthorityWriter(PreviewOrganizationAuthorityReader, Protocol):
    def begin(self, ecosystem: str, tenant_id: str, started_at: datetime) -> OrganizationAuthorityAttempt: ...

    def finalize(
        self,
        attempt_sequence: int,
        status: OrganizationAuthorityFinalStatus,
        *,
        completed_at: datetime,
        organization_id: str | None,
        reason: OrganizationAuthorityFailureReason | None,
    ) -> OrganizationAuthorityAttempt: ...

    def delete_superseded_before(self, ecosystem: str, tenant_id: str, before: datetime) -> int: ...
