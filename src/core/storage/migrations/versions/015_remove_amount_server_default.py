"""Remove server_default from topic_attribution_facts.amount column.

Revision ID: 015
Revises: 014
Create Date: 2026-03-31
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("topic_attribution_facts") as batch_op:
        batch_op.alter_column(
            "amount",
            existing_type=sa.String(),
            nullable=False,
            server_default=None,
        )


def downgrade() -> None:
    with op.batch_alter_table("topic_attribution_facts") as batch_op:
        batch_op.alter_column(
            "amount",
            existing_type=sa.String(),
            nullable=False,
            server_default="",
        )
