"""Add native Confluent Cost source-evidence storage.

Revision ID: 018
Revises: 017
Create Date: 2026-07-18
"""

from __future__ import annotations

from core.storage.migrations.preview_hook import (
    run_preview_evidence_downgrade_step,
    run_preview_evidence_step,
)

revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    run_preview_evidence_step("018")


def downgrade() -> None:
    run_preview_evidence_downgrade_step("018")
