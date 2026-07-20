from __future__ import annotations

from dataclasses import replace
from typing import Any
from unittest.mock import MagicMock

import pytest

from core.models.resource import CoreResource, Resource
from core.storage.interface import ResourceRepository
from tests.unit.core.preview.conftest import preview_module


def _resource(
    resource_id: str,
    resource_type: str,
    *,
    parent_id: str | None = "env-1",
    tenant_id: str = "tenant-1",
    metadata: dict[str, Any] | None = None,
) -> CoreResource:
    return CoreResource(
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        resource_id=resource_id,
        resource_type=resource_type,
        display_name=f"Name {resource_id}",
        parent_id=parent_id,
        metadata=metadata if metadata is not None else {"provider_cloud": "AWS", "provider_region": "us-east-1"},
    )


def _repository(*resources: Resource) -> MagicMock:
    by_id = {resource.resource_id: resource for resource in resources}
    repository = MagicMock(spec=ResourceRepository)
    repository.get.side_effect = lambda _ecosystem, _tenant_id, resource_id: by_id.get(resource_id)
    return repository


def _semantics(mapping: Any, key: Any) -> Any:
    return mapping.PreviewChargeSemantics(
        mapping.PreviewChargeKind.METERED_USAGE,
        "Usage",
        "Usage-Based",
        key,
        True,
        True,
    )


@pytest.mark.parametrize(
    ("rule_key", "origin", "authorities"),
    [
        ("KAFKA", _resource("lkc-1", "kafka_cluster"), ()),
        ("CLUSTER_LINK", _resource("lkc-1", "kafka_cluster"), ()),
        ("USM", _resource("lkc-1", "kafka_cluster"), ()),
        ("DATA_GOVERNANCE", _resource("lsrc-1", "schema_registry"), ()),
        (
            "CONNECT",
            _resource("lcc-1", "connector", parent_id="lkc-parent", metadata={"env_id": "env-1"}),
            (_resource("lkc-parent", "kafka_cluster"),),
        ),
        (
            "KSQLDB",
            _resource("lksqlc-1", "ksqldb_cluster", metadata={"kafka_cluster_id": "lkc-parent"}),
            (_resource("lkc-parent", "kafka_cluster"),),
        ),
        ("FLINK", _resource("lfcp-1", "flink_compute_pool"), ()),
        (
            "FLINK",
            _resource("lfstmt-1", "flink_statement", metadata={"compute_pool_id": "lfcp-1"}),
            (_resource("lfcp-1", "flink_compute_pool"),),
        ),
    ],
)
def test_provider_context_resolves_every_supported_strategy(
    valid_source_evidence: object,
    rule_key: str,
    origin: CoreResource,
    authorities: tuple[CoreResource, ...],
) -> None:
    mapping = preview_module("mapping")
    source = replace(
        valid_source_evidence,
        resource_id=origin.resource_id,
        resource_name=origin.display_name,
        environment_id="env-1",
    )

    context = mapping.resolve_provider_resource_context(
        source=source,
        semantics=_semantics(mapping, mapping.PreviewServiceRuleKey[rule_key]),
        origin_resource=origin,
        resources=_repository(*authorities),
    )

    assert context.resource_id == origin.resource_id
    assert context.resource_name == origin.display_name
    assert context.resource_type == origin.resource_type
    assert context.host_provider_code == "AWS"
    assert context.region_id == "us-east-1"


@pytest.mark.parametrize(
    ("native_product", "description", "origin", "authorities", "expected_rule"),
    [
        ("KAFKA", "Refund Kafka usage", _resource("lkc-1", "kafka_cluster"), (), "KAFKA"),
        (
            "CONNECT",
            "Refund Connect usage",
            _resource("lcc-1", "connector", parent_id="lkc-parent", metadata={"env_id": "env-1"}),
            (_resource("lkc-parent", "kafka_cluster"),),
            "CONNECT",
        ),
        (
            "KSQL",
            "Refund ksqlDB usage",
            _resource("lksqlc-1", "ksqldb_cluster", metadata={"kafka_cluster_id": "lkc-parent"}),
            (_resource("lkc-parent", "kafka_cluster"),),
            "KSQLDB",
        ),
        ("FLINK", "Refund Flink usage", _resource("lfcp-1", "flink_compute_pool"), (), "FLINK"),
        (
            "FLINK",
            "Refund Flink statement usage",
            _resource("lfstmt-1", "flink_statement", metadata={"compute_pool_id": "lfcp-1"}),
            (_resource("lfcp-1", "flink_compute_pool"),),
            "FLINK",
        ),
        (
            "STREAM_GOVERNANCE",
            "Refund governance usage",
            _resource("lsrc-1", "schema_registry"),
            (),
            "DATA_GOVERNANCE",
        ),
        (
            "CLUSTER_LINK",
            "Refund cluster linking usage",
            _resource("lkc-1", "kafka_cluster"),
            (),
            "CLUSTER_LINK",
        ),
        ("USM", "Refund USM usage", _resource("lkc-1", "kafka_cluster"), (), "USM"),
    ],
)
def test_promo_refund_context_dispatch_uses_retained_service_rule_key(
    valid_source_evidence: object,
    native_product: str,
    description: str,
    origin: CoreResource,
    authorities: tuple[CoreResource, ...],
    expected_rule: str,
) -> None:
    mapping = preview_module("mapping")
    source = replace(
        valid_source_evidence,
        native_product=native_product,
        native_line_type="PROMO_CREDIT",
        native_description=description,
        amount=-valid_source_evidence.amount,  # type: ignore[attr-defined,operator]
        original_amount=-valid_source_evidence.original_amount,  # type: ignore[attr-defined,operator]
        discount_amount=-valid_source_evidence.discount_amount,  # type: ignore[attr-defined,operator]
        price=-valid_source_evidence.price,  # type: ignore[attr-defined,operator]
        resource_id=origin.resource_id,
        resource_name=origin.display_name,
        environment_id="env-1",
    )
    classification = mapping.classify_daily_full_source(
        request_start=source.source_period_start,
        request_end=source.source_period_end,
        source=source,
    )
    assert isinstance(classification, mapping.AcceptedPreviewSource)
    assert classification.semantics.service_rule_key is mapping.PreviewServiceRuleKey[expected_rule]

    context = mapping.resolve_provider_resource_context(
        source=replace(source, native_product="must-not-be-rederived"),
        semantics=classification.semantics,
        origin_resource=origin,
        resources=_repository(*authorities),
    )

    assert context.resource_id == origin.resource_id
    assert context.resource_type == origin.resource_type


@pytest.mark.parametrize(
    ("rule_key", "origin", "authorities"),
    [
        ("KAFKA", _resource("lkc-1", "kafka_cluster", parent_id="env-other"), ()),
        ("CLUSTER_LINK", _resource("lkc-1", "kafka_cluster", parent_id="env-other"), ()),
        ("USM", _resource("lkc-1", "kafka_cluster", parent_id="env-other"), ()),
        ("DATA_GOVERNANCE", _resource("lsrc-1", "schema_registry", parent_id="env-other"), ()),
        (
            "CONNECT",
            _resource("lcc-1", "connector", parent_id="lkc-parent", metadata={"env_id": "env-1"}),
            (_resource("lkc-parent", "kafka_cluster", parent_id="env-other"),),
        ),
        (
            "KSQLDB",
            _resource("lksqlc-1", "ksqldb_cluster", metadata={"kafka_cluster_id": "lkc-parent"}),
            (_resource("lkc-parent", "kafka_cluster", parent_id="env-other"),),
        ),
        ("FLINK", _resource("lfcp-1", "flink_compute_pool", parent_id="env-other"), ()),
        (
            "FLINK",
            _resource("lfstmt-1", "flink_statement", metadata={"compute_pool_id": "lfcp-1"}),
            (_resource("lfcp-1", "flink_compute_pool", parent_id="env-other"),),
        ),
    ],
)
def test_provider_context_rejects_cross_environment_authority(
    valid_source_evidence: object,
    rule_key: str,
    origin: CoreResource,
    authorities: tuple[CoreResource, ...],
) -> None:
    mapping = preview_module("mapping")
    source = replace(valid_source_evidence, resource_id=origin.resource_id, environment_id="env-1")

    with pytest.raises(mapping.PreviewProviderContextIncompleteError):
        mapping.resolve_provider_resource_context(
            source=source,
            semantics=_semantics(mapping, mapping.PreviewServiceRuleKey[rule_key]),
            origin_resource=origin,
            resources=_repository(*authorities),
        )


@pytest.mark.parametrize(
    ("rule_key", "origin", "wrong_authority"),
    [
        (
            "CONNECT",
            _resource("lcc-1", "connector", parent_id="parent", metadata={"env_id": "env-1"}),
            _resource("parent", "schema_registry"),
        ),
        (
            "KSQLDB",
            _resource("lksqlc-1", "ksqldb_cluster", metadata={"kafka_cluster_id": "parent"}),
            _resource("parent", "connector"),
        ),
        (
            "FLINK",
            _resource("lfstmt-1", "flink_statement", metadata={"compute_pool_id": "parent"}),
            _resource("parent", "kafka_cluster"),
        ),
    ],
)
def test_provider_context_rejects_wrong_reference_type_without_fallback(
    valid_source_evidence: object,
    rule_key: str,
    origin: CoreResource,
    wrong_authority: CoreResource,
) -> None:
    mapping = preview_module("mapping")
    source = replace(valid_source_evidence, resource_id=origin.resource_id, environment_id="env-1")

    with pytest.raises(mapping.PreviewProviderContextIncompleteError):
        mapping.resolve_provider_resource_context(
            source=source,
            semantics=_semantics(mapping, mapping.PreviewServiceRuleKey[rule_key]),
            origin_resource=origin,
            resources=_repository(wrong_authority),
        )


@pytest.mark.parametrize(
    ("rule_key", "origin", "authorities"),
    [
        ("KAFKA", None, ()),
        ("KAFKA", _resource("lkc-1", "connector"), ()),
        ("CLUSTER_LINK", None, ()),
        ("CLUSTER_LINK", _resource("lkc-1", "connector"), ()),
        ("USM", None, ()),
        ("USM", _resource("lkc-1", "schema_registry"), ()),
        ("DATA_GOVERNANCE", None, ()),
        ("DATA_GOVERNANCE", _resource("lsrc-1", "kafka_cluster"), ()),
        ("CONNECT", None, ()),
        ("CONNECT", _resource("lcc-1", "kafka_cluster"), ()),
        (
            "CONNECT",
            _resource("lcc-1", "connector", parent_id=None, metadata={"env_id": "env-1"}),
            (),
        ),
        (
            "CONNECT",
            _resource("lcc-1", "connector", parent_id="missing", metadata={"env_id": "env-1"}),
            (),
        ),
        ("KSQLDB", None, ()),
        ("KSQLDB", _resource("lksqlc-1", "connector"), ()),
        ("KSQLDB", _resource("lksqlc-1", "ksqldb_cluster", metadata={}), ()),
        (
            "KSQLDB",
            _resource("lksqlc-1", "ksqldb_cluster", metadata={"kafka_cluster_id": "missing"}),
            (),
        ),
        ("FLINK", None, ()),
        ("FLINK", _resource("lfcp-1", "kafka_cluster"), ()),
        ("FLINK", _resource("lfstmt-1", "flink_statement", metadata={}), ()),
        (
            "FLINK",
            _resource("lfstmt-1", "flink_statement", metadata={"compute_pool_id": "missing"}),
            (),
        ),
    ],
)
def test_provider_context_rejects_missing_or_wrong_origin_parent_and_reference(
    valid_source_evidence: object,
    rule_key: str,
    origin: CoreResource | None,
    authorities: tuple[CoreResource, ...],
) -> None:
    mapping = preview_module("mapping")
    source = replace(
        valid_source_evidence,
        resource_id=origin.resource_id if origin is not None else "missing-origin",
        environment_id="env-1",
    )

    with pytest.raises(mapping.PreviewProviderContextIncompleteError):
        mapping.resolve_provider_resource_context(
            source=source,
            semantics=_semantics(mapping, mapping.PreviewServiceRuleKey[rule_key]),
            origin_resource=origin,
            resources=_repository(*authorities),
        )


@pytest.mark.parametrize(
    "metadata",
    [
        {"cloud": "aws", "region": "us-east-1"},
        {"provider_cloud": "   ", "provider_region": "us-east-1"},
        {"provider_cloud": "AWS", "provider_region": "\t"},
    ],
    ids=("missing", "blank-cloud", "blank-region"),
)
def test_provider_context_rejects_missing_or_whitespace_raw_provider_metadata(
    valid_source_evidence: object,
    metadata: dict[str, str],
) -> None:
    mapping = preview_module("mapping")
    origin = _resource(
        "lkc-1",
        "kafka_cluster",
        metadata=metadata,
    )

    with pytest.raises(mapping.PreviewProviderContextIncompleteError):
        mapping.resolve_provider_resource_context(
            source=valid_source_evidence,
            semantics=_semantics(mapping, mapping.PreviewServiceRuleKey.KAFKA),
            origin_resource=origin,
            resources=_repository(),
        )


def test_provider_context_rejects_cross_tenant_reference(valid_source_evidence: object) -> None:
    mapping = preview_module("mapping")
    origin = _resource("lcc-1", "connector", parent_id="lkc-parent", metadata={"env_id": "env-1"})
    authority = _resource("lkc-parent", "kafka_cluster", tenant_id="tenant-other")
    source = replace(valid_source_evidence, resource_id=origin.resource_id)

    with pytest.raises(mapping.PreviewProviderContextIncompleteError):
        mapping.resolve_provider_resource_context(
            source=source,
            semantics=_semantics(mapping, mapping.PreviewServiceRuleKey.CONNECT),
            origin_resource=origin,
            resources=_repository(authority),
        )


def test_promo_refund_missing_resource_identifiers_is_structurally_incomplete(
    valid_source_evidence: object,
) -> None:
    mapping = preview_module("mapping")
    source = replace(
        valid_source_evidence,
        native_line_type="PROMO_CREDIT",
        native_description="Refund Kafka storage",
        amount=-valid_source_evidence.amount,  # type: ignore[attr-defined,operator]
        original_amount=-valid_source_evidence.original_amount,  # type: ignore[attr-defined,operator]
        discount_amount=-valid_source_evidence.discount_amount,  # type: ignore[attr-defined,operator]
        price=-valid_source_evidence.price,  # type: ignore[attr-defined,operator]
        resource_id=None,
        environment_id=None,
    )

    result = mapping.classify_daily_full_source(
        request_start=valid_source_evidence.source_period_start,  # type: ignore[attr-defined]
        request_end=valid_source_evidence.source_period_end,  # type: ignore[attr-defined]
        source=source,
    )

    assert isinstance(result, mapping.RejectedPreviewSource)
    assert result.issue is mapping.PreviewSourceIssue.RECORD_INCOMPLETE
