"""add allocation_attempts to billing

Revision ID: 002
Revises: 001
Create Date: 2026-02-22

"""

from __future__ import annotations

import logging

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("billing", sa.Column("allocation_attempts", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    op.drop_column("billing", "allocation_attempts")
