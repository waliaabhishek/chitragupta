from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import Column, DateTime, Index, PrimaryKeyConstraint
from sqlmodel import Field, SQLModel

logger = logging.getLogger(__name__)


class CCloudBillingTable(SQLModel, table=True):
    """Confluent Cloud billing table with 7-field composite PK including env_id.

    The env_id distinguishes billing rows for the same resource in different
    environments, preventing silent overwrite collisions on the 6-field core PK.

    Uses explicit PrimaryKeyConstraint to guarantee deterministic column order
    for session.get() lookups.
    """

    __tablename__ = "ccloud_billing"
    __table_args__ = (
        PrimaryKeyConstraint(
            "ecosystem",
            "tenant_id",
            "timestamp",
            "env_id",
            "resource_id",
            "product_type",
            "product_category",
        ),
    )

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    env_id: str = Field(primary_key=True)
    resource_id: str = Field(primary_key=True)
    product_type: str = Field(primary_key=True)
    product_category: str = Field(primary_key=True)
    # Value columns
    quantity: str = ""
    unit_price: str = ""
    total_cost: str = ""
    currency: str = "USD"
    granularity: str = "daily"
    allocation_attempts: int = Field(default=0)
    topic_attribution_attempts: int = Field(default=0)
    metadata_json: str | None = Field(default=None)


class CCloudCostSourceTable(SQLModel, table=True):
    """Lossless native Confluent Cost evidence beside allocation aggregates."""

    __tablename__ = "ccloud_cost_source_records"
    __table_args__ = (
        PrimaryKeyConstraint(
            "ecosystem",
            "tenant_id",
            "source_record_id",
            "evidence_scope_start",
            "evidence_scope_end",
        ),
        Index(
            "ix_ccloud_cost_source_allocation",
            "ecosystem",
            "tenant_id",
            "allocation_timestamp",
        ),
        Index(
            "ix_ccloud_cost_source_retention",
            "ecosystem",
            "tenant_id",
            "retention_timestamp",
        ),
        Index(
            "ix_ccloud_cost_source_undated_scope",
            "ecosystem",
            "tenant_id",
            "source_period_start",
            "evidence_scope_start",
            "evidence_scope_end",
        ),
    )

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    source_record_id: str = Field(primary_key=True)
    identity_scheme: str
    provider_cost_id: str | None = Field(default=None)
    source_period_start: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    source_period_end: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True))
    collection_window_start: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    collection_window_end: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    evidence_scope_start: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    evidence_scope_end: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    allocation_timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    retention_timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False))
    granularity: str | None = Field(default=None)
    product: str | None = Field(default=None)
    line_type: str | None = Field(default=None)
    amount: str | None = Field(default=None)
    original_amount: str | None = Field(default=None)
    discount_amount: str | None = Field(default=None)
    price: str | None = Field(default=None)
    quantity: str | None = Field(default=None)
    unit: str | None = Field(default=None)
    description: str | None = Field(default=None)
    network_access_type: str | None = Field(default=None)
    resource_id: str | None = Field(default=None)
    resource_name: str | None = Field(default=None)
    environment_id: str | None = Field(default=None)
    tier_dimensions_json: str
    malformed: bool = False
    diagnostics_json: str
    raw_payload_json: str
