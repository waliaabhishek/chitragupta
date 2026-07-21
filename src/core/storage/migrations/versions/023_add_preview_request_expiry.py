from __future__ import annotations

from datetime import timedelta

import sqlalchemy as sa
from alembic import op

revision = "023"
down_revision = "022"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("preview_requests") as batch_op:
        batch_op.add_column(sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("worker_id", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_index(
            "ix_preview_requests_owner_expiry",
            ["ecosystem", "tenant_id", "status", "expires_at"],
            unique=False,
        )
        batch_op.create_index(
            "ix_preview_requests_owner_recovery",
            ["ecosystem", "tenant_id", "status", "created_at", "lease_expires_at"],
            unique=False,
        )
        batch_op.create_index(
            "ix_preview_requests_owner_lease",
            ["ecosystem", "tenant_id", "status", "lease_expires_at", "worker_id"],
            unique=False,
        )
    preview_requests = sa.table(
        "preview_requests",
        sa.column("request_id", sa.String()),
        sa.column("status", sa.String()),
        sa.column("completed_at", sa.DateTime(timezone=True)),
        sa.column("expires_at", sa.DateTime(timezone=True)),
    )
    connection = op.get_bind()
    if op.get_context().as_sql:
        op.execute(
            sa.update(preview_requests)
            .where(
                preview_requests.c.status.in_(("ready", "expired")),
                preview_requests.c.completed_at.is_not(None),
            )
            .values(expires_at=preview_requests.c.completed_at + sa.text("INTERVAL '7 days'"))
        )
        return
    completed_rows = connection.execute(
        sa.select(preview_requests.c.request_id, preview_requests.c.completed_at).where(
            preview_requests.c.status.in_(("ready", "expired")),
            preview_requests.c.completed_at.is_not(None),
        )
    )
    update_expiry = (
        sa.update(preview_requests)
        .where(preview_requests.c.request_id == sa.bindparam("backfill_request_id"))
        .values(expires_at=sa.bindparam("backfill_expires_at", type_=sa.DateTime(timezone=True)))
    )
    batch = list(completed_rows.fetchmany(400))
    while batch:
        next_batch = list(completed_rows.fetchmany(400))
        if len(next_batch) == 1:
            batch.extend(next_batch)
            next_batch = []
        connection.execute(
            update_expiry,
            [
                {
                    "backfill_request_id": request_id,
                    "backfill_expires_at": completed_at + timedelta(days=7),
                }
                for request_id, completed_at in batch
            ],
        )
        batch = next_batch


def downgrade() -> None:
    with op.batch_alter_table("preview_requests") as batch_op:
        batch_op.drop_index("ix_preview_requests_owner_lease")
        batch_op.drop_index("ix_preview_requests_owner_recovery")
        batch_op.drop_index("ix_preview_requests_owner_expiry")
        batch_op.drop_column("lease_expires_at")
        batch_op.drop_column("worker_id")
        batch_op.drop_column("expires_at")
