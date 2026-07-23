from __future__ import annotations

import logging
from datetime import date, datetime

from sqlalchemy import Boolean, CheckConstraint, Column, Date, DateTime, Index, Integer, PrimaryKeyConstraint
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


class CCloudSourceCaptureReadinessHistoryTable(SQLModel, table=True):
    __tablename__ = "ccloud_source_capture_readiness_history"
    __table_args__ = (
        PrimaryKeyConstraint(
            "ecosystem",
            "tenant_id",
            "attempt_sequence",
            "window_start",
            "window_end",
        ),
        Index(
            "ix_ccloud_source_capture_history_interval",
            "ecosystem",
            "tenant_id",
            "window_start",
            "window_end",
        ),
        Index(
            "ix_ccloud_source_capture_history_attempt",
            "ecosystem",
            "tenant_id",
            "attempt_sequence",
        ),
    )

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    attempt_sequence: int = Field(
        foreign_key="ccloud_source_evidence_attempts.attempt_sequence",
        primary_key=True,
    )
    window_start: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    window_end: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    capture_id: str
    captured_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    source_count: int


class CCloudFocusPreviewRepairTable(SQLModel, table=True):
    __tablename__ = "ccloud_focus_preview_repairs"
    __table_args__ = (
        Index("ix_ccloud_focus_preview_repair_owner_created", "ecosystem", "tenant_id", "created_at"),
        Index("ix_ccloud_focus_preview_repair_owner_status", "ecosystem", "tenant_id", "status"),
        CheckConstraint(
            "status IN ('queued','running','completed','completed_with_failures','failed')",
            name="ck_ccloud_focus_preview_repair_status",
        ),
    )

    repair_id: str = Field(primary_key=True)
    tenant_name: str
    ecosystem: str
    tenant_id: str
    start_date: date = Field(sa_column=Column(Date, nullable=False))
    end_date: date = Field(sa_column=Column(Date, nullable=False))
    status: str
    created_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    started_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    completed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    diagnostic_code: str | None = None
    diagnostic_message: str | None = None
    diagnostic_retryable: bool | None = Field(default=None, sa_column=Column(Boolean, nullable=True))


class CCloudFocusPreviewRepairDateTable(SQLModel, table=True):
    __tablename__ = "ccloud_focus_preview_repair_dates"
    __table_args__ = (
        PrimaryKeyConstraint("repair_id", "tracking_date"),
        Index("ix_ccloud_focus_preview_repair_date_status", "repair_id", "status", "tracking_date"),
        CheckConstraint(
            "status IN ('queued','running','daily_validated','succeeded','failed')",
            name="ck_ccloud_focus_preview_repair_date_status",
        ),
    )

    repair_id: str = Field(
        foreign_key="ccloud_focus_preview_repairs.repair_id",
        primary_key=True,
    )
    tracking_date: date = Field(sa_column=Column(Date, primary_key=True))
    status: str
    started_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    completed_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    calculation_id: str | None = None
    calculation_completed_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True),
    )
    rows_written: int | None = None
    failure_stage: str | None = None
    diagnostic_code: str | None = None
    diagnostic_message: str | None = None
    diagnostic_retryable: bool | None = Field(default=None, sa_column=Column(Boolean, nullable=True))
    source_correlation_ids_json: str | None = None


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
    "CCloudFocusPreviewRepairDateTable",
    "CCloudFocusPreviewRepairTable",
    "CCloudSourceCaptureReadinessTable",
    "CCloudSourceCaptureReadinessHistoryTable",
    "CCloudSourceEvidenceAttemptTable",
]
