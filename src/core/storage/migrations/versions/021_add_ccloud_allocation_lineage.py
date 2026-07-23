from __future__ import annotations

"""Add Confluent allocation lineage and source-to-billing associations.

Revision ID: 021
Revises: 020
Create Date: 2026-07-20
"""

from core.storage.migrations.preview_hook import (  # noqa: E402
    run_preview_evidence_downgrade_step,
    run_preview_evidence_step,
)

revision = "021"
down_revision = "020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    run_preview_evidence_step("021")


def downgrade() -> None:
    run_preview_evidence_downgrade_step("021")
