from __future__ import annotations

"""Add FOCUS preview requests and per-date calculation identity.

Revision ID: 019
Revises: 018
Create Date: 2026-07-19
"""

import sqlalchemy as sa  # noqa: E402 - future import is intentionally first for module contract
from alembic import op  # noqa: E402 - future import is intentionally first for module contract

revision = "019"
down_revision = "018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("pipeline_state") as batch_op:
        batch_op.add_column(sa.Column("calculation_id", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("calculation_completed_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("calculation_run_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_pipeline_state_calculation_run_id_pipeline_runs",
            "pipeline_runs",
            ["calculation_run_id"],
            ["id"],
        )
        batch_op.create_index(
            "ix_pipeline_state_preview_coverage",
            [
                "ecosystem",
                "tenant_id",
                "tracking_date",
                "chargeback_calculated",
                "calculation_id",
                "calculation_completed_at",
            ],
        )

    op.create_table(
        "preview_requests",
        sa.Column("request_id", sa.String(), nullable=False),
        sa.Column("tenant_name", sa.String(), nullable=False),
        sa.Column("ecosystem", sa.String(), nullable=False),
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("grain", sa.String(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("column_profile", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("calculation_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_through", sa.DateTime(timezone=True), nullable=True),
        sa.Column("calculation_coverage_json", sa.String(), nullable=True),
        sa.Column("diagnostic_code", sa.String(), nullable=True),
        sa.Column("diagnostic_message", sa.String(), nullable=True),
        sa.Column("diagnostic_retryable", sa.Boolean(), nullable=True),
        sa.Column("storage_key", sa.String(), nullable=True),
        sa.Column("manifest_metadata_json", sa.String(), nullable=True),
        sa.Column("data_files_json", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("request_id"),
    )
    op.create_index(
        "ix_preview_requests_owner_created",
        "preview_requests",
        ["ecosystem", "tenant_id", "created_at"],
    )
    op.create_index(
        "ix_preview_requests_owner_status",
        "preview_requests",
        ["ecosystem", "tenant_id", "status"],
    )


def downgrade() -> None:
    op.drop_index("ix_preview_requests_owner_status", table_name="preview_requests")
    op.drop_index("ix_preview_requests_owner_created", table_name="preview_requests")
    op.drop_table("preview_requests")
    with op.batch_alter_table("pipeline_state") as batch_op:
        batch_op.drop_index("ix_pipeline_state_preview_coverage")
        batch_op.drop_constraint("fk_pipeline_state_calculation_run_id_pipeline_runs", type_="foreignkey")
        batch_op.drop_column("calculation_run_id")
        batch_op.drop_column("calculation_completed_at")
        batch_op.drop_column("calculation_id")
