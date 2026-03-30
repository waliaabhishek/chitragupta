"""Add topic_attribution_dimensions, topic_attribution_facts, pipeline_state overlay columns.

Revision ID: 012
Revises: 011
Create Date: 2026-03-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "012"
down_revision = "011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "topic_attribution_dimensions",
        sa.Column("dimension_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("env_id", sa.String(), nullable=False, server_default=""),
        sa.Column("cluster_resource_id", sa.String(), nullable=False),
        sa.Column("topic_name", sa.String(), nullable=False, server_default=""),
        sa.Column("product_category", sa.String(), nullable=False, server_default=""),
        sa.Column("product_type", sa.String(), nullable=False, server_default=""),
        sa.Column("attribution_method", sa.String(), nullable=False, server_default=""),
        sa.UniqueConstraint(
            "ecosystem",
            "tenant_id",
            "env_id",
            "cluster_resource_id",
            "topic_name",
            "product_category",
            "product_type",
            "attribution_method",
            name="uq_topic_attribution_dimensions",
        ),
    )
    op.create_index("ix_topic_attr_dim_eco_tenant", "topic_attribution_dimensions", ["ecosystem", "tenant_id"])
    op.create_index(
        "ix_topic_attr_dim_cluster", "topic_attribution_dimensions", ["ecosystem", "tenant_id", "cluster_resource_id"]
    )
    op.create_index("ix_topic_attribution_dimensions_ecosystem", "topic_attribution_dimensions", ["ecosystem"])
    op.create_index("ix_topic_attribution_dimensions_tenant_id", "topic_attribution_dimensions", ["tenant_id"])
    op.create_index(
        "ix_topic_attribution_dimensions_cluster_resource_id", "topic_attribution_dimensions", ["cluster_resource_id"]
    )
    op.create_index("ix_topic_attribution_dimensions_product_type", "topic_attribution_dimensions", ["product_type"])

    op.create_table(
        "topic_attribution_facts",
        sa.Column("timestamp", sa.DateTime(timezone=True), primary_key=True),
        sa.Column(
            "dimension_id", sa.Integer(), sa.ForeignKey("topic_attribution_dimensions.dimension_id"), primary_key=True
        ),
        sa.Column("amount", sa.String(), nullable=False, server_default=""),
    )
    op.create_index("ix_topic_attr_facts_dim_ts", "topic_attribution_facts", ["dimension_id", "timestamp"])

    op.add_column(
        "pipeline_state", sa.Column("topic_overlay_gathered", sa.Boolean(), server_default="0", nullable=False)
    )
    op.add_column(
        "pipeline_state", sa.Column("topic_attribution_calculated", sa.Boolean(), server_default="0", nullable=False)
    )


def downgrade() -> None:
    op.drop_column("pipeline_state", "topic_attribution_calculated")
    op.drop_column("pipeline_state", "topic_overlay_gathered")
    op.drop_table("topic_attribution_facts")
    op.drop_table("topic_attribution_dimensions")
