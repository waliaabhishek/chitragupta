"""TASK-011 TDD red-phase tests: DIP — remove CCloud enum members from core AllocationDetail.

Verifies:
1. AllocationDetail has no Flink/cluster-linking plugin-specific members.
2. plugins.confluent_cloud.constants exports CLUSTER_LINKING_COST and
   NO_FLINK_STMT_NAME_TO_OWNER_MAP as plain str constants.
3. FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER is deleted (not in plugin constants).
4. String values match what the old enum members carried.
5. No file under src/core/ imports from plugins.confluent_cloud.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


class TestAllocationDetailCorePurity:
    """AllocationDetail must not contain plugin-specific members."""

    def test_no_flink_stmt_name_to_owner_map_member(self) -> None:
        """AllocationDetail must NOT have NO_FLINK_STMT_NAME_TO_OWNER_MAP."""
        from core.models.chargeback import AllocationDetail

        assert not hasattr(AllocationDetail, "NO_FLINK_STMT_NAME_TO_OWNER_MAP"), (
            "AllocationDetail.NO_FLINK_STMT_NAME_TO_OWNER_MAP must be removed (plugin-specific)"
        )

    def test_no_failed_to_locate_flink_statement_owner_member(self) -> None:
        """AllocationDetail must NOT have FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER."""
        from core.models.chargeback import AllocationDetail

        assert not hasattr(AllocationDetail, "FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER"), (
            "AllocationDetail.FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER must be removed (plugin-specific)"
        )

    def test_no_cluster_linking_cost_member(self) -> None:
        """AllocationDetail must NOT have CLUSTER_LINKING_COST."""
        from core.models.chargeback import AllocationDetail

        assert not hasattr(AllocationDetail, "CLUSTER_LINKING_COST"), (
            "AllocationDetail.CLUSTER_LINKING_COST must be removed (plugin-specific)"
        )


class TestPluginConstantsExist:
    """plugins.confluent_cloud.constants must export the relocated constants."""

    def test_constants_module_importable(self) -> None:
        """plugins.confluent_cloud.constants must exist and be importable."""
        import importlib

        importlib.import_module("plugins.confluent_cloud.constants")

    def test_cluster_linking_cost_is_plain_str(self) -> None:
        """CLUSTER_LINKING_COST must be a plain str, not an enum member."""
        from plugins.confluent_cloud.constants import CLUSTER_LINKING_COST

        assert isinstance(CLUSTER_LINKING_COST, str)
        assert type(CLUSTER_LINKING_COST) is str, "CLUSTER_LINKING_COST must be plain str, not an enum subclass"

    def test_no_flink_stmt_name_to_owner_map_is_plain_str(self) -> None:
        """NO_FLINK_STMT_NAME_TO_OWNER_MAP must be a plain str, not an enum member."""
        from plugins.confluent_cloud.constants import NO_FLINK_STMT_NAME_TO_OWNER_MAP

        assert isinstance(NO_FLINK_STMT_NAME_TO_OWNER_MAP, str)
        assert type(NO_FLINK_STMT_NAME_TO_OWNER_MAP) is str, (
            "NO_FLINK_STMT_NAME_TO_OWNER_MAP must be plain str, not an enum subclass"
        )


class TestDeadCodeDeleted:
    """FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER must not appear anywhere in plugin constants."""

    def test_failed_to_locate_flink_statement_owner_absent_from_constants(self) -> None:
        """FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER must not be exported from plugin constants."""
        import plugins.confluent_cloud.constants as mod

        assert not hasattr(mod, "FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER"), (
            "FAILED_TO_LOCATE_FLINK_STATEMENT_OWNER must be deleted, not moved to constants"
        )


class TestStringValuePreservation:
    """Plugin constants must carry the same string values as the old enum members."""

    def test_cluster_linking_cost_value(self) -> None:
        """CLUSTER_LINKING_COST value must equal 'cluster_linking_cost'."""
        from plugins.confluent_cloud.constants import CLUSTER_LINKING_COST

        assert CLUSTER_LINKING_COST == "cluster_linking_cost"

    def test_no_flink_stmt_name_to_owner_map_value(self) -> None:
        """NO_FLINK_STMT_NAME_TO_OWNER_MAP value must equal 'no_flink_stmt_name_to_owner_map'."""
        from plugins.confluent_cloud.constants import NO_FLINK_STMT_NAME_TO_OWNER_MAP

        assert NO_FLINK_STMT_NAME_TO_OWNER_MAP == "no_flink_stmt_name_to_owner_map"


class TestNoCoreToPluginImport:
    """No file under src/core/ may import from plugins.confluent_cloud."""

    def test_no_core_file_imports_ccloud_plugin(self) -> None:
        """grep -r 'plugins.confluent_cloud' src/core/ must return no matches."""
        repo_root = Path(__file__).parents[4]
        core_dir = repo_root / "src" / "core"

        result = subprocess.run(
            ["grep", "-r", "plugins.confluent_cloud", str(core_dir)],
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0, f"Found imports of plugins.confluent_cloud inside src/core/:\n{result.stdout}"
