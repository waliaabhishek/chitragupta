from __future__ import annotations

from dataclasses import FrozenInstanceError
from importlib import import_module
from pathlib import Path

import pytest

from core.preview import mapping
from tests.unit.core.preview.test_revision_mapping import EXPECTED_PUBLIC_KNOWN_GAPS

RESOLVED_COLUMNS = {
    "AllocatedMethodDetails",
    "AllocatedTags",
    "Tags",
    "x_ChitraguptaAllocationRatio",
    "x_ChitraguptaAllocationMethodVersion",
}


def test_focus_preview_capability_is_one_immutable_literal_authority() -> None:
    capability_module = import_module("core.preview.capability")
    validation = import_module("core.preview.manifest_validation")
    capability = capability_module.FOCUS_PREVIEW_CAPABILITY

    assert capability.mapping_profile_version == "focus-1.4-preview-v1"
    assert capability.target_focus_version == "1.4"
    assert capability.conformance_status == "non_conforming"
    assert capability_module.preview_manifest_known_gaps() == EXPECTED_PUBLIC_KNOWN_GAPS
    assert mapping.FOCUS_PREVIEW_CAPABILITY is capability
    assert validation.FOCUS_PREVIEW_CAPABILITY is capability
    assert capability.mapping_profile_version == mapping.MAPPING_PROFILE_VERSION
    assert mapping.KNOWN_GAPS is capability.known_gaps
    assert mapping.preview_manifest_known_gaps() == EXPECTED_PUBLIC_KNOWN_GAPS
    with pytest.raises(FrozenInstanceError):
        capability.target_focus_version = "1.5"  # type: ignore[misc]


def test_current_allocation_lineage_columns_remain_applicable() -> None:
    assert mapping.MAPPING_PROFILE_VERSION == "focus-1.4-preview-v1"
    rules = {rule.column: rule for rule in (*mapping.FOCUS_1_4_COLUMN_RULES, *mapping.CUSTOM_EVIDENCE_RULES)}
    for column in RESOLVED_COLUMNS:
        assert rules[column].applicability is mapping.PreviewApplicability.APPLICABLE
        assert rules[column].gap_code is None
        assert rules[column].owner_task is None
        assert column in mapping.MAPPED_COLUMNS


def test_contracted_unit_price_columns_are_now_applicable_profile_columns() -> None:
    rules = {rule.column: rule for rule in mapping.FOCUS_1_4_COLUMN_RULES}

    for column in ("ContractedUnitPrice", "PricingCurrencyContractedUnitPrice"):
        assert rules[column].applicability is mapping.PreviewApplicability.APPLICABLE
        assert column in mapping.MAPPED_COLUMNS
        assert column not in mapping.PROFILE_NOT_APPLICABLE_COLUMNS


def test_active_applicability_states_and_tableflow_context_remain_unchanged() -> None:
    applicability_values = {item.value for item in mapping.PreviewApplicability}

    assert applicability_values == {"applicable", "not_applicable", "declared_gap"}
    assert (
        mapping.FOCUS_1_4_SERVICE_RULES_V1[mapping.PreviewServiceRuleKey.TABLEFLOW].context_strategy
        == "unsupported_provider_context"
    )


def test_global_handler_allocator_and_generic_export_modules_remain_free_of_preview_policy() -> None:
    design_owned_source_paths = {
        Path("src/plugins/confluent_cloud/allocators/default_allocators.py"),
        Path("src/plugins/confluent_cloud/allocators/kafka_allocators.py"),
        Path("src/plugins/confluent_cloud/allocators/org_wide_allocators.py"),
        Path("src/plugins/confluent_cloud/handlers/default.py"),
        Path("src/plugins/confluent_cloud/handlers/org_wide.py"),
        Path("src/core/export.py"),
    }
    obsolete_task = "TASK-254" + ".05"
    for path in design_owned_source_paths:
        if path.exists():
            assert obsolete_task not in path.read_text()
