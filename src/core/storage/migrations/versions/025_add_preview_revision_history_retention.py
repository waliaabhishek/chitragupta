from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "025"
down_revision = "024"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "preview_revisions",
        sa.Column("retention_pending_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_preview_revisions_owner_month_visible_history",
        "preview_revisions",
        [
            "ecosystem",
            "tenant_id",
            "month_start",
            "retention_pending_at",
            "published_at",
            "revision_id",
        ],
        unique=False,
    )
    op.create_index(
        "ix_preview_revisions_owner_retention_due",
        "preview_revisions",
        [
            "ecosystem",
            "tenant_id",
            "retention_pending_at",
            "month_end",
            "published_at",
            "revision_id",
        ],
        unique=False,
    )
    op.create_index(
        "ix_preview_revisions_owner_retention_pending",
        "preview_revisions",
        ["ecosystem", "tenant_id", "retention_pending_at", "revision_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_preview_revisions_owner_retention_pending", table_name="preview_revisions")
    op.drop_index("ix_preview_revisions_owner_retention_due", table_name="preview_revisions")
    op.drop_index("ix_preview_revisions_owner_month_visible_history", table_name="preview_revisions")
    op.drop_column("preview_revisions", "retention_pending_at")
