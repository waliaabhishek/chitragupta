"""Run selected plugin-owned storage work at the core schema head."""

from __future__ import annotations

from core.storage.migrations.plugin_storage_hook import (
    run_plugin_storage_downgrade_step,
    run_plugin_storage_step,
)

revision = "033"
down_revision = "032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    run_plugin_storage_step("033")


def downgrade() -> None:
    run_plugin_storage_downgrade_step("033")
