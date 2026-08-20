"""Add durable self-managed Kafka target-scope state."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "033"
down_revision = "032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "self_managed_kafka_scope_state",
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("cluster_id", sa.String(), nullable=False),
        sa.Column("metrics_identifier_label", sa.String(), nullable=False),
        sa.Column("metrics_identifier", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_blocked_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_blocked_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_failure_reason", sa.String(), nullable=True),
        sa.Column("last_failure_status", sa.String(), nullable=True),
        sa.Column("last_failure_detail", sa.String(), nullable=True),
        sa.Column("last_probe_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_probe_status", sa.String(), nullable=True),
        sa.Column("recovered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recovery_cursor_date", sa.Date(), nullable=True),
        sa.Column("retention_gap_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retention_gap_end", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("ecosystem", "tenant_id", "cluster_id"),
    )


def downgrade() -> None:
    op.drop_table("self_managed_kafka_scope_state")
