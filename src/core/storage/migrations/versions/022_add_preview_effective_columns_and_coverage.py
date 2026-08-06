from __future__ import annotations

"""Add Preview v5 effective columns and evidence coverage.

Revision ID: 022
Revises: 021
Create Date: 2026-07-20
"""

import sqlalchemy as sa  # noqa: E402
from alembic import op  # noqa: E402

revision = "022"
down_revision = "021"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("preview_requests") as batch_op:
        batch_op.add_column(sa.Column("effective_columns_json", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("effective_coverage_start_date", sa.Date(), nullable=True))
        batch_op.add_column(sa.Column("effective_coverage_end_date", sa.Date(), nullable=True))
        batch_op.add_column(sa.Column("availability_cutoff_end_date", sa.Date(), nullable=True))
        batch_op.add_column(sa.Column("monthly_status", sa.String(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("preview_requests") as batch_op:
        batch_op.drop_column("monthly_status")
        batch_op.drop_column("availability_cutoff_end_date")
        batch_op.drop_column("effective_coverage_end_date")
        batch_op.drop_column("effective_coverage_start_date")
        batch_op.drop_column("effective_columns_json")
