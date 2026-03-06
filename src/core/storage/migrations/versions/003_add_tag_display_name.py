"""add display_name to custom_tags and unique constraint

Revision ID: 003
Revises: 002
Create Date: 2026-02-26

"""

from __future__ import annotations

import logging

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("custom_tags", sa.Column("display_name", sa.String(), nullable=False, server_default=""))
    # Batch mode required for SQLite constraint alterations
    with op.batch_alter_table("custom_tags", schema=None) as batch_op:
        batch_op.create_unique_constraint("uq_custom_tag_dimension_key", ["dimension_id", "tag_key"])


def downgrade() -> None:
    with op.batch_alter_table("custom_tags", schema=None) as batch_op:
        batch_op.drop_constraint("uq_custom_tag_dimension_key", type_="unique")
    op.drop_column("custom_tags", "display_name")
