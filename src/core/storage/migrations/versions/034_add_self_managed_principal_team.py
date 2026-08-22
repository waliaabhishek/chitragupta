"""Add nullable principal-team chargeback fact snapshot."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "034"
down_revision = "033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("chargeback_facts", sa.Column("principal_team", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("chargeback_facts", "principal_team")
