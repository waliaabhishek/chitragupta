"""baseline

Revision ID: 001
Revises:
Create Date: 2026-02-22

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "resources",
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("resource_id", sa.String(), nullable=False),
        sa.Column("resource_type", sa.String(), nullable=False),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("parent_id", sa.String(), nullable=True),
        sa.Column("owner_id", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="active"),
        sa.Column("cloud", sa.String(), nullable=True),
        sa.Column("region", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("ecosystem", "tenant_id", "resource_id"),
    )
    op.create_index("ix_resources_resource_type", "resources", ["resource_type"])
    op.create_index("ix_resources_parent_id", "resources", ["parent_id"])
    op.create_index("ix_resources_owner_id", "resources", ["owner_id"])
    op.create_index("ix_resources_status", "resources", ["status"])
    op.create_index("ix_resources_cloud", "resources", ["cloud"])
    op.create_index("ix_resources_region", "resources", ["region"])

    op.create_table(
        "identities",
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("identity_id", sa.String(), nullable=False),
        sa.Column("identity_type", sa.String(), nullable=False),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("ecosystem", "tenant_id", "identity_id"),
    )
    op.create_index("ix_identities_identity_type", "identities", ["identity_type"])

    op.create_table(
        "billing",
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resource_id", sa.String(), nullable=False),
        sa.Column("product_type", sa.String(), nullable=False),
        sa.Column("product_category", sa.String(), nullable=False),
        sa.Column("quantity", sa.String(), nullable=False),
        sa.Column("unit_price", sa.String(), nullable=False),
        sa.Column("total_cost", sa.String(), nullable=False),
        sa.Column("currency", sa.String(), nullable=False, server_default="USD"),
        sa.Column("granularity", sa.String(), nullable=False, server_default="daily"),
        sa.Column("metadata_json", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("ecosystem", "tenant_id", "timestamp", "resource_id", "product_type"),
    )
    op.create_index("ix_billing_product_category", "billing", ["product_category"])

    op.create_table(
        "chargeback_dimensions",
        sa.Column("dimension_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("resource_id", sa.String(), nullable=True),
        sa.Column("product_category", sa.String(), nullable=False),
        sa.Column("product_type", sa.String(), nullable=False),
        sa.Column("identity_id", sa.String(), nullable=False),
        sa.Column("cost_type", sa.String(), nullable=False),
        sa.Column("allocation_method", sa.String(), nullable=True),
        sa.Column("allocation_detail", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("dimension_id"),
        sa.UniqueConstraint(
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
    op.create_index("ix_chargeback_dimensions_ecosystem", "chargeback_dimensions", ["ecosystem"])
    op.create_index("ix_chargeback_dimensions_tenant_id", "chargeback_dimensions", ["tenant_id"])
    op.create_index("ix_chargeback_dimensions_resource_id", "chargeback_dimensions", ["resource_id"])
    op.create_index("ix_chargeback_dimensions_product_type", "chargeback_dimensions", ["product_type"])
    op.create_index("ix_chargeback_dimensions_identity_id", "chargeback_dimensions", ["identity_id"])

    op.create_table(
        "chargeback_facts",
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dimension_id", sa.Integer(), sa.ForeignKey("chargeback_dimensions.dimension_id"), nullable=False),
        sa.Column("amount", sa.String(), nullable=False),
        sa.Column("tags_json", sa.String(), nullable=False, server_default="[]"),
        sa.PrimaryKeyConstraint("timestamp", "dimension_id"),
    )

    op.create_table(
        "pipeline_state",
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("tracking_date", sa.Date(), nullable=False),
        sa.Column("billing_gathered", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("resources_gathered", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("chargeback_calculated", sa.Boolean(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("ecosystem", "tenant_id", "tracking_date"),
    )

    op.create_table(
        "custom_tags",
        sa.Column("tag_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("dimension_id", sa.Integer(), sa.ForeignKey("chargeback_dimensions.dimension_id"), nullable=False),
        sa.Column("tag_key", sa.String(), nullable=False),
        sa.Column("tag_value", sa.String(), nullable=False),
        sa.Column("created_by", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("tag_id"),
    )
    op.create_index("ix_custom_tags_dimension_id", "custom_tags", ["dimension_id"])


def downgrade() -> None:
    op.drop_table("custom_tags")
    op.drop_table("chargeback_facts")
    op.drop_table("pipeline_state")
    op.drop_table("chargeback_dimensions")
    op.drop_table("billing")
    op.drop_table("identities")
    op.drop_table("resources")
