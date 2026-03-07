from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import Column, DateTime
from sqlmodel import Field, SQLModel

logger = logging.getLogger(__name__)


class CCloudBillingTable(SQLModel, table=True):
    """Confluent Cloud billing table with 7-field composite PK including env_id.

    The env_id distinguishes billing rows for the same resource in different
    environments, preventing silent overwrite collisions on the 6-field core PK.
    """

    __tablename__ = "ccloud_billing"

    ecosystem: str = Field(primary_key=True)
    tenant_id: str = Field(primary_key=True)
    timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True), primary_key=True))
    env_id: str = Field(primary_key=True)  # CCloud-specific
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
    metadata_json: str | None = Field(default=None)
