from __future__ import annotations

from pathlib import Path

from core.preview import mapping

RESOLVED_GAPS = {
    "allocation_lineage_and_tag_projection_pending",
    "allocation_ratio_deferred",
    "allocation_method_version_deferred",
}
RESOLVED_COLUMNS = {
    "AllocatedMethodDetails",
    "AllocatedTags",
    "Tags",
    "x_ChitraguptaAllocationRatio",
    "x_ChitraguptaAllocationMethodVersion",
}
TASK_254_05_LINE_TYPES = {
    "AUDIT_LOG_READ",
    "SUPPORT",
    "PROMO_CREDIT",
    "KAFKA_REST_PRODUCE",
    "KAFKA_STREAMS",
    "CONNECT_NUM_RECORDS",
    "CLUSTER_LINKING_PER_LINK",
    "CLUSTER_LINKING_READ",
    "CLUSTER_LINKING_WRITE",
    "USM_CONNECTED_NODE",
    "TABLEFLOW_DATA_PROCESSED",
    "TABLEFLOW_NUM_TOPICS",
    "TABLEFLOW_STORAGE",
}


def test_profile_v4_resolves_only_task_254_05_gaps_and_makes_five_columns_applicable() -> None:
    assert mapping.MAPPING_PROFILE_VERSION == "focus-1.4-preview-v1"
    assert RESOLVED_GAPS.isdisjoint({gap.code for gap in mapping.KNOWN_GAPS})
    rules = {rule.column: rule for rule in (*mapping.FOCUS_1_4_COLUMN_RULES, *mapping.CUSTOM_EVIDENCE_RULES)}
    for column in RESOLVED_COLUMNS:
        assert rules[column].applicability is mapping.PreviewApplicability.APPLICABLE
        assert rules[column].gap_code is None
        assert rules[column].owner_task is None
        assert column in mapping.MAPPED_COLUMNS


def test_every_current_native_line_type_is_lineage_ready_without_changing_tableflow_context() -> None:
    assert mapping.FOCUS_1_4_NATIVE_LINE_READINESS_V1.keys() >= TASK_254_05_LINE_TYPES
    assert {mapping.FOCUS_1_4_NATIVE_LINE_READINESS_V1[line_type] for line_type in TASK_254_05_LINE_TYPES} == {
        mapping.PreviewLineageReadiness.READY
    }
    assert (
        mapping.FOCUS_1_4_SERVICE_RULES_V1[mapping.PreviewServiceRuleKey.TABLEFLOW].context_strategy
        == "unsupported_provider_context"
    )


def test_global_handler_allocator_and_generic_export_modules_are_not_task_254_05_policy_owners() -> None:
    design_owned_source_paths = {
        Path("src/plugins/confluent_cloud/allocators/default_allocators.py"),
        Path("src/plugins/confluent_cloud/allocators/kafka_allocators.py"),
        Path("src/plugins/confluent_cloud/allocators/org_wide_allocators.py"),
        Path("src/plugins/confluent_cloud/handlers/default.py"),
        Path("src/plugins/confluent_cloud/handlers/org_wide.py"),
        Path("src/core/export.py"),
    }
    for path in design_owned_source_paths:
        if path.exists():
            assert "TASK-254.05" not in path.read_text()
