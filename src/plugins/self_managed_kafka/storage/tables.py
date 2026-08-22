from __future__ import annotations

import logging
from datetime import date, datetime

from sqlalchemy import Column, ForeignKeyConstraint, PrimaryKeyConstraint
from sqlmodel import Field, SQLModel

from core.storage.backends.sqlmodel.base_tables import BillingTable, IdentityTable, ResourceTable
from core.storage.backends.sqlmodel.timestamps import UTCSecondDateTime

logger = logging.getLogger(__name__)

# Self-managed Kafka uses the same billing/resource/identity schema as core.
SMKBillingTable = BillingTable
SMKResourceTable = ResourceTable
SMKIdentityTable = IdentityTable


class SelfManagedKafkaScopeStateTable(SQLModel, table=True):
    """Durable target-scope breaker state owned by the self-managed plugin."""

    __tablename__ = "self_managed_kafka_scope_state"
    __table_args__ = (PrimaryKeyConstraint("ecosystem", "tenant_id", "cluster_id"),)

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    cluster_id: str = Field(primary_key=True)
    metrics_identifier_label: str
    metrics_identifier: str
    status: str
    opened_at: datetime | None = Field(default=None, sa_column=Column(UTCSecondDateTime(), nullable=True))
    first_blocked_window_start: datetime | None = Field(
        default=None,
        sa_column=Column(UTCSecondDateTime(), nullable=True),
    )
    first_blocked_window_end: datetime | None = Field(
        default=None,
        sa_column=Column(UTCSecondDateTime(), nullable=True),
    )
    last_failure_reason: str | None = None
    last_failure_status: str | None = None
    last_failure_detail: str | None = None
    last_probe_at: datetime | None = Field(default=None, sa_column=Column(UTCSecondDateTime(), nullable=True))
    last_probe_status: str | None = None
    recovered_at: datetime | None = Field(default=None, sa_column=Column(UTCSecondDateTime(), nullable=True))
    recovery_cursor_date: date | None = None
    retention_gap_start: datetime | None = Field(default=None, sa_column=Column(UTCSecondDateTime(), nullable=True))
    retention_gap_end: datetime | None = Field(default=None, sa_column=Column(UTCSecondDateTime(), nullable=True))


class SelfManagedKafkaPrincipalTeamSnapshotTable(SQLModel, table=True):
    """Calculation-time team snapshot owned by the self-managed plugin."""

    __tablename__ = "self_managed_kafka_principal_team_snapshots"
    __table_args__ = (
        ForeignKeyConstraint(
            ["timestamp", "dimension_id"],
            ["chargeback_facts.timestamp", "chargeback_facts.dimension_id"],
        ),
    )

    timestamp: datetime = Field(sa_column=Column(UTCSecondDateTime(), primary_key=True))
    dimension_id: int = Field(primary_key=True)
    team: str
