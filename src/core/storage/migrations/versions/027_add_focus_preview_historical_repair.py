from __future__ import annotations

from core.storage.migrations.preview_hook import (
    run_preview_evidence_downgrade_step,
    run_preview_evidence_step,
)

revision = "027"
down_revision = "026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    run_preview_evidence_step("027")


def downgrade() -> None:
    run_preview_evidence_downgrade_step("027")
