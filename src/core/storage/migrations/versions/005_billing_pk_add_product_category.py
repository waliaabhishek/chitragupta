"""billing table: add product_category to primary key

Revision ID: 005
Revises: 004
Create Date: 2026-03-06

"""

from __future__ import annotations

import logging

from alembic import op

logger = logging.getLogger(__name__)

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("billing") as batch_op:
        batch_op.drop_index("ix_billing_product_category")
        batch_op.create_primary_key(
            "pk_billing",
            ["ecosystem", "tenant_id", "timestamp", "resource_id", "product_type", "product_category"],
        )


def downgrade() -> None:
    # WARNING: Downgrade will fail if billing table contains rows that differ
    # only by product_category (i.e., same ecosystem/tenant/timestamp/resource/product_type
    # but different product_category). Such rows cannot coexist under the 5-field PK.
    with op.batch_alter_table("billing") as batch_op:
        batch_op.create_primary_key(
            "pk_billing",
            ["ecosystem", "tenant_id", "timestamp", "resource_id", "product_type"],
        )
        batch_op.create_index("ix_billing_product_category", ["product_category"])
