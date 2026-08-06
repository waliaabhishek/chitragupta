from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum

logger = logging.getLogger(__name__)

CFG_PREVIEW_EVIDENCE_ENABLED = "chitragupta.preview_evidence_enabled"
CFG_PREVIEW_EVIDENCE_MODULE = "chitragupta.preview_evidence_module"
CFG_PREVIEW_EVIDENCE_ISSUES = "chitragupta.preview_evidence_issues"


class PreviewEvidenceAvailabilityState(StrEnum):
    READY = "ready"
    UNAVAILABLE = "unavailable"


class PreviewEvidenceIssueKind(StrEnum):
    CAPABILITY_MISSING = "capability_missing"
    SCHEMA_INCOMPATIBLE = "schema_incompatible"
    DDL_FAILED = "ddl_failed"
    BOOTSTRAP_FAILED = "bootstrap_failed"


@dataclass(frozen=True)
class PreviewEvidenceIssue:
    revision: str
    kind: PreviewEvidenceIssueKind
    error_type: str


@dataclass(frozen=True)
class PreviewEvidenceAvailability:
    state: PreviewEvidenceAvailabilityState
    issues: tuple[PreviewEvidenceIssue, ...] = ()


@dataclass(frozen=True)
class PreviewEvidenceBootstrapUnavailable:
    error_type: str

    def __post_init__(self) -> None:
        if not self.error_type.strip():
            raise ValueError("bootstrap unavailability error type must not be blank")


class PreviewEvidenceError(RuntimeError):
    """Base Preview evidence storage error."""


class PreviewEvidenceSchemaError(PreviewEvidenceError):
    """Preview evidence schema is incompatible."""


class PreviewEvidenceUnavailableError(PreviewEvidenceError):
    """Preview evidence storage is unavailable."""


class PreviewEvidenceOfflineMigrationError(PreviewEvidenceError):
    """Preview evidence migrations require an online database connection."""


class PreviewEvidenceIssueCollector:
    def __init__(self) -> None:
        self._issues: list[PreviewEvidenceIssue] = []

    def record(self, issue: PreviewEvidenceIssue) -> None:
        if issue not in self._issues:
            self._issues.append(issue)

    def snapshot(self) -> tuple[PreviewEvidenceIssue, ...]:
        return tuple(self._issues)
