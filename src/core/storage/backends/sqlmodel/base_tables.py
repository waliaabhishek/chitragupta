from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import Column, DateTime
from sqlmodel import Field, SQLModel

logger = logging.getLogger(__name__)


class ResourceTable(SQLModel, table=True):
    __tablename__ = "resources"

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    resource_id: str = Field(primary_key=True)
    resource_type: str = Field(index=True)
    display_name: str | None = None
    parent_id: str | None = Field(default=None, index=True)
    owner_id: str | None = Field(default=None, index=True)
    status: str = Field(default="active", index=True)
    cloud: str | None = Field(default=None, index=True)
    region: str | None = Field(default=None, index=True)
    created_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), index=True),
    )
    deleted_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), index=True),
    )
    last_seen_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    metadata_json: str | None = Field(default=None)


class IdentityTable(SQLModel, table=True):
    __tablename__ = "identities"

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    identity_id: str = Field(primary_key=True)
    identity_type: str = Field(index=True)
    display_name: str | None = None
    created_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), index=True),
    )
    deleted_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), index=True),
    )
    last_seen_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    metadata_json: str | None = Field(default=None)


class BillingTable(SQLModel, table=True):
    __tablename__ = "billing"

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    resource_id: str = Field(primary_key=True)
    product_type: str = Field(primary_key=True)
    product_category: str = Field(primary_key=True)
    quantity: str = ""
    unit_price: str = ""
    total_cost: str = ""
    currency: str = "USD"
    granularity: str = "daily"
    allocation_attempts: int = Field(default=0)
    topic_attribution_attempts: int = Field(default=0)
    metadata_json: str | None = Field(default=None)
