from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import CheckConstraint, Column, DateTime, Index, Integer, PrimaryKeyConstraint
from sqlmodel import Field, SQLModel

from plugins.confluent_cloud.storage.tables import (
    CCloudAllocationLineagePortionTable,
    CCloudAllocationLineageRunTable,
    CCloudCostSourceTable,
)

logger = logging.getLogger(__name__)

# The public v26 name distinguishes the retained source record from the
# pre-opt-in module layout while preserving compatibility for existing imports.
CCloudCostSourceRecordTable = CCloudCostSourceTable


class CCloudSourceEvidenceAttemptTable(SQLModel, table=True):
    __tablename__ = "ccloud_source_evidence_attempts"
    __table_args__ = (
        Index(
            "uq_ccloud_source_attempt_owner_token",
            "ecosystem",
            "tenant_id",
            "refresh_token",
            unique=True,
        ),
        Index(
            "ix_ccloud_source_attempt_owner_latest",
            "ecosystem",
            "tenant_id",
            "attempt_sequence",
        ),
    )

    attempt_sequence: int | None = Field(
        default=None,
        sa_column=Column(Integer, primary_key=True, autoincrement=True),
    )
    ecosystem: str
    tenant_id: str
    refresh_token: str
    refresh_start: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    refresh_end: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    status: str
    started_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    completed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    failure_reason: str | None = Field(default=None)


class CCloudSourceCaptureReadinessTable(SQLModel, table=True):
    __tablename__ = "ccloud_source_capture_readiness"
    __table_args__ = (
        PrimaryKeyConstraint("ecosystem", "tenant_id", "window_start", "window_end"),
        Index(
            "uq_ccloud_source_capture_owner_id",
            "ecosystem",
            "tenant_id",
            "capture_id",
            unique=True,
        ),
        Index(
            "ix_ccloud_source_capture_attempt",
            "ecosystem",
            "tenant_id",
            "attempt_sequence",
        ),
        Index(
            "ix_ccloud_source_capture_retention",
            "ecosystem",
            "tenant_id",
            "window_end",
        ),
        CheckConstraint("window_start < window_end", name="ck_ccloud_source_capture_window"),
    )

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    window_start: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    window_end: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    capture_id: str
    captured_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    source_count: int
    attempt_sequence: int = Field(foreign_key="ccloud_source_evidence_attempts.attempt_sequence")


class CCloudOrganizationAuthorityAttemptTable(SQLModel, table=True):
    __tablename__ = "ccloud_organization_authority_attempts"
    __table_args__ = (
        Index(
            "ix_ccloud_org_authority_owner_latest",
            "ecosystem",
            "tenant_id",
            "attempt_sequence",
        ),
    )

    attempt_sequence: int | None = Field(
        default=None,
        sa_column=Column(Integer, primary_key=True, autoincrement=True),
    )
    ecosystem: str
    tenant_id: str
    status: str
    started_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    completed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    organization_id: str | None = Field(default=None)
    failure_reason: str | None = Field(default=None)


__all__ = [
    "CCloudAllocationLineagePortionTable",
    "CCloudAllocationLineageRunTable",
    "CCloudCostSourceRecordTable",
    "CCloudOrganizationAuthorityAttemptTable",
    "CCloudSourceCaptureReadinessTable",
    "CCloudSourceEvidenceAttemptTable",
]
