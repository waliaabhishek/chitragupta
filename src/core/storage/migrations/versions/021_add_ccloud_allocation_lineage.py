from __future__ import annotations

"""Add Confluent allocation lineage and source-to-billing associations.

Revision ID: 021
Revises: 020
Create Date: 2026-07-20
"""

import sqlalchemy as sa  # noqa: E402
from alembic import op  # noqa: E402

revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ccloud_cost_source_records") as batch_op:
        batch_op.add_column(sa.Column("billing_timestamp", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("billing_env_id", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("billing_resource_id", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("billing_product_type", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("billing_product_category", sa.String(), nullable=True))

    op.create_table(
        "ccloud_allocation_lineage_runs",
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("tracking_date", sa.Date(), nullable=False),
        sa.Column("calculation_id", sa.String(), nullable=False),
        sa.Column("calculation_completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("capture_status", sa.String(), nullable=False),
        sa.Column("capture_reason", sa.String(), nullable=True),
        sa.Column("portion_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("ecosystem", "tenant_id", "tracking_date"),
    )
    op.create_table(
        "ccloud_allocation_lineage_portions",
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("tracking_date", sa.Date(), nullable=False),
        sa.Column("calculation_id", sa.String(), nullable=False),
        sa.Column("origin_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("origin_env_id", sa.String(), nullable=False),
        sa.Column("origin_resource_id", sa.String(), nullable=False),
        sa.Column("origin_product_type", sa.String(), nullable=False),
        sa.Column("origin_product_category", sa.String(), nullable=False),
        sa.Column("portion_ordinal", sa.Integer(), nullable=False),
        sa.Column("target_kind", sa.String(), nullable=False),
        sa.Column("target_id", sa.String(), nullable=True),
        sa.Column("allocated_cost", sa.String(), nullable=False),
        sa.Column("allocated_quantity", sa.String(), nullable=False),
        sa.Column("allocation_ratio", sa.String(), nullable=False),
        sa.Column("method_id", sa.String(), nullable=False),
        sa.Column("method_version", sa.String(), nullable=False),
        sa.Column("method_details_json", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint(
            "ecosystem",
            "tenant_id",
            "tracking_date",
            "calculation_id",
            "origin_timestamp",
            "origin_env_id",
            "origin_resource_id",
            "origin_product_type",
            "origin_product_category",
            "portion_ordinal",
        ),
    )
    op.create_index(
        "ix_ccloud_allocation_lineage_tenant_calculation_date",
        "ccloud_allocation_lineage_portions",
        ["tenant_id", "calculation_id", "tracking_date"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ccloud_allocation_lineage_tenant_calculation_date",
        table_name="ccloud_allocation_lineage_portions",
    )
    op.drop_table("ccloud_allocation_lineage_portions")
    op.drop_table("ccloud_allocation_lineage_runs")
    with op.batch_alter_table("ccloud_cost_source_records") as batch_op:
        batch_op.drop_column("billing_product_category")
        batch_op.drop_column("billing_product_type")
        batch_op.drop_column("billing_resource_id")
        batch_op.drop_column("billing_env_id")
        batch_op.drop_column("billing_timestamp")
