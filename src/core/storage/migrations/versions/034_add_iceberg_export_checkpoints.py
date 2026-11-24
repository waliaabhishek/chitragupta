"""Persist Iceberg publication state for each destination and tenant day."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "034"
down_revision = "033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "iceberg_export_checkpoints",
        sa.Column("table_uuid", sa.String(), primary_key=True),
        sa.Column("ecosystem", sa.String(), primary_key=True),
        sa.Column("tenant_id", sa.String(), primary_key=True),
        sa.Column("tracking_date", sa.Date(), primary_key=True),
        sa.Column("attempted_revision", sa.String(), nullable=True),
        sa.Column("attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(), nullable=True),
        sa.Column("error_code", sa.String(), nullable=True),
        sa.Column("confirmed_revision", sa.String(), nullable=True),
        sa.Column("source_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("snapshot_id", sa.BigInteger(), nullable=True),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("row_count", sa.Integer(), nullable=True),
        sa.Column("replay_requested", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.create_index(
        "ix_iceberg_export_checkpoints_retention",
        "iceberg_export_checkpoints",
        ["ecosystem", "tenant_id", "tracking_date"],
    )


def downgrade() -> None:
    op.drop_table("iceberg_export_checkpoints")
