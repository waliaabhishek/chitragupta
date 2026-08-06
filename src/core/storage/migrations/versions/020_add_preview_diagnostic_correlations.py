from __future__ import annotations

"""Add safe source correlations to Preview diagnostics.

Revision ID: 020
Revises: 019
Create Date: 2026-07-19
"""

import sqlalchemy as sa  # noqa: E402 - future import is intentionally first
from alembic import op  # noqa: E402 - future import is intentionally first

revision = "020"
down_revision = "019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("preview_requests") as batch_op:
        batch_op.add_column(sa.Column("diagnostic_source_correlation_ids_json", sa.Text(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("preview_requests") as batch_op:
        batch_op.drop_column("diagnostic_source_correlation_ids_json")
