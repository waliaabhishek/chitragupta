"""Replace custom_tags with entity-level tags table.

Revision ID: 011
Revises: 010
Create Date: 2026-03-25
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "011"
down_revision = "010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("custom_tags")

    op.create_table(
        "tags",
        sa.Column("tag_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("entity_type", sa.String(), nullable=False),
        sa.Column("entity_id", sa.String(), nullable=False),
        sa.Column("tag_key", sa.String(), nullable=False),
        sa.Column("tag_value", sa.String(), nullable=False),
        sa.Column("created_by", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "tenant_id",
            "entity_type",
            "entity_id",
            "tag_key",
            name="uq_tags_entity_key",
        ),
    )
    op.create_index("ix_tags_entity", "tags", ["tenant_id", "entity_type", "entity_id"])
    op.create_index("ix_tags_tenant_key", "tags", ["tenant_id", "tag_key"])
    op.create_index("ix_tags_tenant_key_value", "tags", ["tenant_id", "tag_key", "tag_value"])


def downgrade() -> None:
    op.drop_table("tags")

    op.create_table(
        "custom_tags",
        sa.Column("tag_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("dimension_id", sa.Integer(), nullable=False),
        sa.Column("tag_key", sa.String(), nullable=False, server_default=""),
        sa.Column("tag_value", sa.String(), nullable=False, server_default=""),
        sa.Column("display_name", sa.String(), nullable=False, server_default=""),
        sa.Column("created_by", sa.String(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("dimension_id", "tag_key", name="uq_custom_tag_dimension_key"),
    )
    op.create_index("ix_custom_tags_dimension_id", "custom_tags", ["dimension_id"])
