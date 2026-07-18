"""Add native Confluent Cost source-evidence storage.

Revision ID: 018
Revises: 017
Create Date: 2026-07-18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ccloud_cost_source_records",
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("source_record_id", sa.String(), nullable=False),
        sa.Column("identity_scheme", sa.String(), nullable=False),
        sa.Column("provider_cost_id", sa.String(), nullable=True),
        sa.Column("source_period_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("collection_window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("collection_window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_scope_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_scope_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("allocation_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("retention_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("granularity", sa.String(), nullable=True),
        sa.Column("product", sa.String(), nullable=True),
        sa.Column("line_type", sa.String(), nullable=True),
        sa.Column("amount", sa.String(), nullable=True),
        sa.Column("original_amount", sa.String(), nullable=True),
        sa.Column("discount_amount", sa.String(), nullable=True),
        sa.Column("price", sa.String(), nullable=True),
        sa.Column("quantity", sa.String(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("network_access_type", sa.String(), nullable=True),
        sa.Column("resource_id", sa.String(), nullable=True),
        sa.Column("resource_name", sa.String(), nullable=True),
        sa.Column("environment_id", sa.String(), nullable=True),
        sa.Column("tier_dimensions_json", sa.String(), nullable=False),
        sa.Column("malformed", sa.Boolean(), nullable=False),
        sa.Column("diagnostics_json", sa.String(), nullable=False),
        sa.Column("raw_payload_json", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint(
            "ecosystem",
            "tenant_id",
            "source_record_id",
            "evidence_scope_start",
            "evidence_scope_end",
        ),
    )
    op.create_index(
        "ix_ccloud_cost_source_allocation",
        "ccloud_cost_source_records",
        ["ecosystem", "tenant_id", "allocation_timestamp"],
    )
    op.create_index(
        "ix_ccloud_cost_source_retention",
        "ccloud_cost_source_records",
        ["ecosystem", "tenant_id", "retention_timestamp"],
    )
    op.create_index(
        "ix_ccloud_cost_source_undated_scope",
        "ccloud_cost_source_records",
        ["ecosystem", "tenant_id", "source_period_start", "evidence_scope_start", "evidence_scope_end"],
    )


def downgrade() -> None:
    op.drop_index("ix_ccloud_cost_source_undated_scope", table_name="ccloud_cost_source_records")
    op.drop_index("ix_ccloud_cost_source_retention", table_name="ccloud_cost_source_records")
    op.drop_index("ix_ccloud_cost_source_allocation", table_name="ccloud_cost_source_records")
    op.drop_table("ccloud_cost_source_records")
