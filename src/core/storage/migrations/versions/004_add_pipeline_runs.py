"""add pipeline_runs table

Revision ID: 004
Revises: 003
Create Date: 2026-02-26

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_name", sa.String(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("dates_gathered", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("dates_calculated", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rows_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.String(), nullable=True),
    )
    op.create_index("ix_pipeline_runs_tenant_name", "pipeline_runs", ["tenant_name"])


def downgrade() -> None:
    op.drop_index("ix_pipeline_runs_tenant_name", "pipeline_runs")
    op.drop_table("pipeline_runs")
