from __future__ import annotations

import logging
from datetime import UTC, date, datetime

from sqlalchemy import Column, Date, DateTime, UniqueConstraint
from sqlmodel import Field, SQLModel

logger = logging.getLogger(__name__)


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


class PipelineRunTable(SQLModel, table=True):
    __tablename__ = "pipeline_runs"

    id: int | None = Field(default=None, primary_key=True)
    tenant_name: str = Field(index=True)
    started_at: datetime = Field(sa_column=Column(DateTime(timezone=True)))
    ended_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True)))
    status: str  # "running" | "completed" | "failed"
    stage: str | None = None  # "gathering" | "calculating" | "emitting"
    current_date: date | None = None
    dates_gathered: int = 0
    dates_calculated: int = 0
    rows_written: int = 0
    error_message: str | None = None


class CustomTagTable(SQLModel, table=True):
    __tablename__ = "custom_tags"
    __table_args__ = (UniqueConstraint("dimension_id", "tag_key", name="uq_custom_tag_dimension_key"),)

    tag_id: int | None = Field(default=None, primary_key=True)
    dimension_id: int = Field(foreign_key="chargeback_dimensions.dimension_id", index=True)
    tag_key: str = ""
    tag_value: str = ""
    display_name: str = ""
    created_by: str = ""
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(DateTime(timezone=True)),
    )
