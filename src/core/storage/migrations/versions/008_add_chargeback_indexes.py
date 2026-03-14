"""Add composite indexes on chargeback_facts and chargeback_dimensions

Revision ID: 008
Revises: 007
Create Date: 2026-03-14

"""

from __future__ import annotations

from alembic import op

revision = "008"
down_revision = "007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_chargeback_facts_dimension_timestamp",
        "chargeback_facts",
        ["dimension_id", "timestamp"],
    )
    op.create_index(
        "ix_chargeback_dimensions_eco_tenant",
        "chargeback_dimensions",
        ["ecosystem", "tenant_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_chargeback_dimensions_eco_tenant", table_name="chargeback_dimensions")
    op.drop_index("ix_chargeback_facts_dimension_timestamp", table_name="chargeback_facts")
