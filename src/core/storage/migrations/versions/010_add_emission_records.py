"""Add emission_records table

Revision ID: 010
Revises: 009
Create Date: 2026-03-22
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "emission_records",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("emitter_name", sa.String(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="1"),
        sa.UniqueConstraint("ecosystem", "tenant_id", "emitter_name", "date", name="uq_emission_records"),
    )
    op.create_index("ix_emission_records_ecosystem", "emission_records", ["ecosystem"])
    op.create_index("ix_emission_records_tenant_id", "emission_records", ["tenant_id"])
    op.create_index("ix_emission_records_emitter_name", "emission_records", ["emitter_name"])
    op.create_index("ix_emission_records_date", "emission_records", ["date"])


def downgrade() -> None:
    op.drop_table("emission_records")
