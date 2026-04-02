"""Add topic_attribution_attempts to billing and ccloud_billing tables.

Revision ID: 016
Revises: 015
Create Date: 2026-04-02
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "016"
down_revision = "015"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "billing",
        sa.Column("topic_attribution_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "ccloud_billing",
        sa.Column("topic_attribution_attempts", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("billing", "topic_attribution_attempts")
    op.drop_column("ccloud_billing", "topic_attribution_attempts")
