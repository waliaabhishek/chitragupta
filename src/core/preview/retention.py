from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Protocol, runtime_checkable

from core.time_precision import canonical_utc_second

logger = logging.getLogger(__name__)

_ERROR_TYPE_PATTERN = re.compile(r"[^A-Za-z0-9_.-]")
_MAX_ERROR_TYPE_LENGTH = 80
_MAX_DIAGNOSTIC_CODE_LENGTH = 80
_MAX_DIAGNOSTIC_MESSAGE_LENGTH = 256


class PreviewRetentionCleanupKind(StrEnum):
    ORDINARY = "ordinary"
    PREVIEW_EVIDENCE = "preview_evidence"


class PreviewRetentionOutcomeStatus(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"


@dataclass(frozen=True)
class PreviewRetentionDiagnostic:
    code: str
    message: str
    error_type: str

    def __post_init__(self) -> None:
        if not self.code.strip() or len(self.code) > _MAX_DIAGNOSTIC_CODE_LENGTH:
            raise ValueError("retention diagnostic code must be nonblank and bounded")
        if not self.message.strip() or len(self.message) > _MAX_DIAGNOSTIC_MESSAGE_LENGTH:
            raise ValueError("retention diagnostic message must be nonblank and bounded")
        if (
            not self.error_type
            or len(self.error_type) > _MAX_ERROR_TYPE_LENGTH
            or _ERROR_TYPE_PATTERN.search(self.error_type)
        ):
            raise ValueError("retention diagnostic error_type must be redaction-safe and bounded to 80 characters")


@dataclass(frozen=True)
class PreviewRetentionOutcome:
    owner: str
    cleanup_kind: PreviewRetentionCleanupKind
    attempted_at: datetime
    status: PreviewRetentionOutcomeStatus
    diagnostic: PreviewRetentionDiagnostic | None

    def __post_init__(self) -> None:
        if not self.owner.strip():
            raise ValueError("retention outcome owner must not be blank")
        if not isinstance(self.cleanup_kind, PreviewRetentionCleanupKind):
            raise ValueError("invalid retention cleanup kind")
        if not isinstance(self.attempted_at, datetime):
            raise ValueError("retention attempted_at must be a datetime")
        if (
            self.attempted_at.tzinfo is None
            or self.attempted_at.utcoffset() is None
            or self.attempted_at.utcoffset() != timedelta(0)
            or self.attempted_at.microsecond != 0
        ):
            raise ValueError("retention attempted_at must be aware whole-second UTC")
        if not isinstance(self.status, PreviewRetentionOutcomeStatus):
            raise ValueError("invalid retention outcome status")
        if self.status is PreviewRetentionOutcomeStatus.SUCCESS and self.diagnostic is not None:
            raise ValueError("successful retention outcome must not have a diagnostic")
        if self.status is PreviewRetentionOutcomeStatus.FAILURE and self.diagnostic is None:
            raise ValueError("failed retention outcome requires a diagnostic")


@dataclass(frozen=True)
class PreviewRetentionOutcomeSet:
    ordinary: PreviewRetentionOutcome | None
    preview_evidence: PreviewRetentionOutcome | None

    def __post_init__(self) -> None:
        if self.ordinary is not None and self.ordinary.cleanup_kind is not PreviewRetentionCleanupKind.ORDINARY:
            raise ValueError("ordinary retention outcome has the wrong cleanup kind")
        if (
            self.preview_evidence is not None
            and self.preview_evidence.cleanup_kind is not PreviewRetentionCleanupKind.PREVIEW_EVIDENCE
        ):
            raise ValueError("Preview evidence retention outcome has the wrong cleanup kind")
        if (
            self.ordinary is not None
            and self.preview_evidence is not None
            and self.ordinary.owner != self.preview_evidence.owner
        ):
            raise ValueError("retention outcome owners do not match")


_FAILURE_DIAGNOSTICS = {
    PreviewRetentionCleanupKind.ORDINARY: (
        "focus_preview_ordinary_retention_failed",
        "Ordinary tenant retention cleanup failed. Review worker logs and restore tenant storage; "
        "existing valid Preview data remains available.",
    ),
    PreviewRetentionCleanupKind.PREVIEW_EVIDENCE: (
        "focus_preview_evidence_retention_failed",
        "FOCUS Preview evidence retention cleanup failed. Review worker logs and restore Preview "
        "evidence storage; existing valid Preview data remains available.",
    ),
}


def retention_failure_diagnostic(
    cleanup_kind: PreviewRetentionCleanupKind,
    error: BaseException,
) -> PreviewRetentionDiagnostic:
    code, message = _FAILURE_DIAGNOSTICS[cleanup_kind]
    error_type = _ERROR_TYPE_PATTERN.sub("", type(error).__name__)[:_MAX_ERROR_TYPE_LENGTH] or "Error"
    return PreviewRetentionDiagnostic(
        code=code,
        message=message,
        error_type=error_type,
    )


def canonical_retention_attempt(value: datetime) -> datetime:
    return canonical_utc_second(value, field="attempted_at")


@runtime_checkable
class PreviewRetentionOutcomeRepository(Protocol):
    def upsert_latest(
        self,
        ecosystem: str,
        tenant_id: str,
        outcome: PreviewRetentionOutcome,
    ) -> None: ...

    def get_latest_for_owner(
        self,
        ecosystem: str,
        tenant_id: str,
    ) -> PreviewRetentionOutcomeSet: ...
