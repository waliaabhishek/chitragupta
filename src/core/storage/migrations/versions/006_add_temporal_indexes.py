"""Add indexes on temporal columns for resources and identities

Revision ID: 006
Revises: ddebea2fe0a8
Create Date: 2026-03-07

"""

from __future__ import annotations

from alembic import op

revision = "006"
down_revision = "ddebea2fe0a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_resources_created_at", "resources", ["created_at"])
    op.create_index("ix_resources_deleted_at", "resources", ["deleted_at"])
    op.create_index("ix_identities_created_at", "identities", ["created_at"])
    op.create_index("ix_identities_deleted_at", "identities", ["deleted_at"])


def downgrade() -> None:
    op.drop_index("ix_identities_deleted_at", table_name="identities")
    op.drop_index("ix_identities_created_at", table_name="identities")
    op.drop_index("ix_resources_deleted_at", table_name="resources")
    op.drop_index("ix_resources_created_at", table_name="resources")
