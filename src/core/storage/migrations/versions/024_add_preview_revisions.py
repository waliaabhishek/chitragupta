from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "024"
down_revision = "023"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "preview_revisions",
        sa.Column("revision_id", sa.String(), nullable=False),
        sa.Column("tenant_name_at_publication", sa.String(), nullable=False),
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("month_start", sa.Date(), nullable=False),
        sa.Column("month_end", sa.Date(), nullable=False),
        sa.Column("monthly_status", sa.String(), nullable=False),
        sa.Column("material_sha256", sa.String(), nullable=False),
        sa.Column("source_snapshot_json", sa.Text(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("supersedes_revision_id", sa.String(), nullable=True),
        sa.Column("superseded_by_revision_id", sa.String(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("storage_key", sa.String(), nullable=False),
        sa.Column("manifest_metadata_json", sa.Text(), nullable=False),
        sa.Column("file_metadata_json", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("revision_id"),
    )
    op.create_index(
        "ix_preview_revisions_supersedes",
        "preview_revisions",
        ["supersedes_revision_id"],
        unique=False,
    )
    op.create_index(
        "ix_preview_revisions_superseded_by",
        "preview_revisions",
        ["superseded_by_revision_id"],
        unique=False,
    )
    op.create_index(
        "ux_preview_revisions_owner_month_current",
        "preview_revisions",
        ["ecosystem", "tenant_id", "month_start"],
        unique=True,
        sqlite_where=sa.text("is_current = 1"),
        postgresql_where=sa.text("is_current IS TRUE"),
    )


def downgrade() -> None:
    op.drop_index("ux_preview_revisions_owner_month_current", table_name="preview_revisions")
    op.drop_index("ix_preview_revisions_superseded_by", table_name="preview_revisions")
    op.drop_index("ix_preview_revisions_supersedes", table_name="preview_revisions")
    op.drop_table("preview_revisions")
