from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import Column, Date, DateTime, UniqueConstraint
from sqlmodel import Field, SQLModel


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
    created_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    deleted_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    last_seen_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    metadata_json: str | None = Field(default=None)


class IdentityTable(SQLModel, table=True):
    __tablename__ = "identities"

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    identity_id: str = Field(primary_key=True)
    identity_type: str = Field(index=True)
    display_name: str | None = None
    created_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    deleted_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    last_seen_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    metadata_json: str | None = Field(default=None)


class BillingTable(SQLModel, table=True):
    __tablename__ = "billing"

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    resource_id: str = Field(primary_key=True)
    product_type: str = Field(primary_key=True)
    product_category: str = Field(index=True)
    quantity: str = ""
    unit_price: str = ""
    total_cost: str = ""
    currency: str = "USD"
    granularity: str = "daily"
    metadata_json: str | None = Field(default=None)


class ChargebackDimensionTable(SQLModel, table=True):
    __tablename__ = "chargeback_dimensions"
    __table_args__ = (
        UniqueConstraint(
            "ecosystem",
            "tenant_id",
            "resource_id",
            "product_category",
            "product_type",
            "identity_id",
            "cost_type",
            "allocation_method",
            "allocation_detail",
            name="uq_chargeback_dimensions",
        ),
    )

    dimension_id: int | None = Field(default=None, primary_key=True)
    ecosystem: str = Field(index=True)
    tenant_id: str = Field(index=True)
    resource_id: str | None = Field(default=None, index=True)
    product_category: str = ""
    product_type: str = Field(default="", index=True)
    identity_id: str = Field(default="", index=True)
    cost_type: str = ""
    allocation_method: str | None = None
    allocation_detail: str | None = None


class ChargebackFactTable(SQLModel, table=True):
    __tablename__ = "chargeback_facts"

    timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    dimension_id: int = Field(primary_key=True, foreign_key="chargeback_dimensions.dimension_id")
    amount: str = ""
    tags_json: str = Field(default="[]")


class PipelineStateTable(SQLModel, table=True):
    __tablename__ = "pipeline_state"

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    tracking_date: date = Field(sa_column=Column(Date(), primary_key=True))
    billing_gathered: bool = False
    resources_gathered: bool = False
    chargeback_calculated: bool = False


class CustomTagTable(SQLModel, table=True):
    __tablename__ = "custom_tags"

    tag_id: int | None = Field(default=None, primary_key=True)
    dimension_id: int = Field(foreign_key="chargeback_dimensions.dimension_id", index=True)
    tag_key: str = ""
    tag_value: str = ""
    created_by: str = ""
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(DateTime(timezone=True)),
    )
