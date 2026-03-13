"""Add stage and current_date columns to pipeline_runs for progress tracking

Revision ID: 007
Revises: 006
Create Date: 2026-03-13

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("pipeline_runs", sa.Column("stage", sa.String(), nullable=True))
    op.add_column("pipeline_runs", sa.Column("current_date", sa.Date(), nullable=True))


def downgrade() -> None:
    op.drop_column("pipeline_runs", "current_date")
    op.drop_column("pipeline_runs", "stage")
