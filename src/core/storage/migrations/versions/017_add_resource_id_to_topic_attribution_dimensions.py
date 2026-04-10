"""Add resource_id column to topic_attribution_dimensions.

Revision ID: 017
Revises: 016
Create Date: 2026-04-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "017"
down_revision = "016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Step 1: add nullable column (plain op.add_column suffices for nullable adds)
    op.add_column("topic_attribution_dimensions", sa.Column("resource_id", sa.String(), nullable=True))

    # Step 2: backfill from existing columns
    op.execute("UPDATE topic_attribution_dimensions SET resource_id = cluster_resource_id || ':topic:' || topic_name")

    # Step 3: make NOT NULL now that all rows are populated
    with op.batch_alter_table("topic_attribution_dimensions") as batch_op:
        batch_op.alter_column("resource_id", existing_type=sa.String(), nullable=False)
        batch_op.create_index("ix_topic_attr_dim_resource_id", ["resource_id"])


def downgrade() -> None:
    with op.batch_alter_table("topic_attribution_dimensions") as batch_op:
        batch_op.drop_index("ix_topic_attr_dim_resource_id")
        batch_op.drop_column("resource_id")
