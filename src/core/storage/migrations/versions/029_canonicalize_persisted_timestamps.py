from __future__ import annotations

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy import inspect

from core.storage.migrations.timestamp_precision import (
    canonicalize_persisted_timestamps,
    downgrade_sqlite_timestamp_format,
)

revision = "029"
down_revision = "028"
branch_labels = None
depends_on = None

ONLINE_REQUIRED_MESSAGE = (
    "migration 029 requires an online database connection to preflight persisted timestamps; "
    "run Alembic upgrade or downgrade without --sql"
)


def upgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError(ONLINE_REQUIRED_MESSAGE)
    connection = op.get_bind()
    canonicalize_persisted_timestamps(connection)

    if not inspect(connection).has_table("preview_revisions"):
        return
    op.add_column(
        "preview_revisions",
        sa.Column(
            "retention_retry_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    indexes = {index["name"] for index in inspect(connection).get_indexes("preview_revisions")}
    if "ix_preview_revisions_owner_retention_pending" in indexes:
        op.drop_index(
            "ix_preview_revisions_owner_retention_pending",
            table_name="preview_revisions",
        )
    op.create_index(
        "ix_preview_revisions_owner_retention_pending",
        "preview_revisions",
        [
            "ecosystem",
            "tenant_id",
            "retention_retry_count",
            "retention_pending_at",
            "revision_id",
        ],
        unique=False,
    )


def downgrade() -> None:
    if context.is_offline_mode():
        raise RuntimeError(ONLINE_REQUIRED_MESSAGE)
    connection = op.get_bind()
    downgrade_sqlite_timestamp_format(connection)

    if not inspect(connection).has_table("preview_revisions"):
        return
    indexes = {index["name"] for index in inspect(connection).get_indexes("preview_revisions")}
    if "ix_preview_revisions_owner_retention_pending" in indexes:
        op.drop_index(
            "ix_preview_revisions_owner_retention_pending",
            table_name="preview_revisions",
        )
    op.create_index(
        "ix_preview_revisions_owner_retention_pending",
        "preview_revisions",
        ["ecosystem", "tenant_id", "retention_pending_at", "revision_id"],
        unique=False,
    )
    op.drop_column("preview_revisions", "retention_retry_count")
