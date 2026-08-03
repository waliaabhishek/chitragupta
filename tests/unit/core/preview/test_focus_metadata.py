from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from importlib import import_module
from typing import Any

import pytest

from tests.unit.core.preview.test_lifecycle_snapshot_v5 import _request, _snapshot
from tests.unit.core.preview.test_monthly_v5 import _row


def _focus_metadata() -> Any:
    return import_module("core.preview.focus_metadata")


def _mapping() -> Any:
    return import_module("core.preview.mapping")


def _models() -> Any:
    return import_module("core.preview.models")


def _build_draft(
    *,
    request: Any,
    snapshot: Any,
    rows: tuple[Any, ...],
    max_csv_file_bytes: int | None = None,
) -> Any:
    mapping = _mapping()
    count = len(rows)
    return mapping.build_preview_data_package(
        request=request,
        snapshot=snapshot,
        full_rows=rows,
        reconciliation=mapping.PreviewPackageReconciliation(
            source_records=count,
            source_cost=Decimal("8") * count,
            allocated_cost=Decimal("8") * count,
            source_quantity=Decimal("5") * count,
            allocated_quantity=Decimal("5") * count,
        ),
        max_csv_file_bytes=max_csv_file_bytes,
    )


def _artifact_metadata(payload: Any) -> Any:
    models = _models()
    body = payload.body if isinstance(payload.body, bytes) else bytes(payload.body)
    return models.PreviewArtifactMetadata(
        payload.name,
        payload.media_type,
        len(body),
        hashlib.sha256(body).hexdigest(),
        payload.order,
    )


def _metadata_root(payload: Any) -> dict[str, Any]:
    body = payload.body if isinstance(payload.body, bytes) else bytes(payload.body)
    return json.loads(body)


def _replace_payload_body(payload: Any, body: bytes) -> Any:
    models = _models()
    return models.PreviewArtifactPayload(
        name=payload.name,
        media_type=payload.media_type,
        order=payload.order,
        body=body,
    )


def _requested_case(
    *,
    grain: str,
    profile: str,
    columns: tuple[str, ...],
    snapshot: Any,
    rows: tuple[Any, ...],
    max_csv_file_bytes: int | None = None,
    created_at: datetime | None = None,
) -> tuple[Any, Any, Any, tuple[Any, ...]]:
    started_at_base = datetime(2026, 7, 3, tzinfo=UTC) if created_at is None else created_at
    request = _request(
        grain=grain,
        column_profile=profile,
        effective_columns=columns,
        created_at=started_at_base,
        started_at=started_at_base + timedelta(minutes=1),
    )
    draft = _build_draft(
        request=request,
        snapshot=snapshot,
        rows=rows,
        max_csv_file_bytes=max_csv_file_bytes,
    )
    return request, snapshot, draft, tuple(draft.data_files)


def _requested_metadata_payload(
    *,
    request: Any,
    snapshot: Any,
    draft: Any,
    data_files: tuple[Any, ...],
    ready_at: datetime,
) -> Any:
    module = _focus_metadata()
    return module.build_requested_focus_metadata_artifact(
        request=request,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        ready_at=ready_at,
        expires_at=ready_at + timedelta(days=7),
    )


def _requested_package_files(
    *,
    request: Any,
    snapshot: Any,
    draft: Any,
    data_files: tuple[Any, ...],
    ready_at: datetime,
) -> tuple[Any, ...]:
    module = _focus_metadata()
    metadata = _requested_metadata_payload(
        request=request,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        ready_at=ready_at,
    )
    return module.compose_package_artifacts(
        data_files=data_files,
        focus_metadata=metadata,
    )


def _revision_metadata_payload(
    *,
    snapshot: Any,
    draft: Any,
    data_files: tuple[Any, ...],
    published_at: datetime,
) -> Any:
    module = _focus_metadata()
    mapping = _mapping()
    return module.build_revision_focus_metadata_artifact(
        revision_id="revision-1",
        tenant_name_at_publication="production",
        month="2026-07",
        start_date=date(2026, 7, 1),
        end_date=date(2026, 8, 1),
        monthly_status=snapshot.monthly_status,
        material_sha256=mapping.preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256),
        supersedes_revision_id=None,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        published_at=published_at,
    )


@pytest.mark.parametrize(
    ("grain", "profile", "columns", "snapshot", "rows"),
    [
        (
            "daily",
            "full",
            None,
            _snapshot(source_through=datetime(2026, 7, 2, tzinfo=UTC)),
            (_row(day=1, BillingCurrency="USD"),),
        ),
        (
            "monthly",
            "summary",
            None,
            _snapshot(
                start=date(2026, 7, 1),
                end=date(2026, 8, 1),
                monthly_status="settled",
                cutoff=date(2026, 8, 1),
                source_through=datetime(2026, 8, 1, tzinfo=UTC),
            ),
            (_row(day=1, BillingCurrency="USD"), _row(day=2, BillingCurrency="USD", AllocatedResourceId="sa-2")),
        ),
        (
            "monthly",
            "custom",
            ("Tags", "BilledCost", "AllocatedResourceId"),
            _snapshot(
                start=date(2026, 7, 1),
                end=date(2026, 8, 1),
                monthly_status="settled",
                cutoff=date(2026, 8, 1),
                source_through=datetime(2026, 8, 1, tzinfo=UTC),
            ),
            (_row(day=1, BillingCurrency="USD", Tags='{"team":"a"}'),),
        ),
    ],
)
def test_requested_metadata_contract_is_truthful_and_matches_exact_emitted_columns(
    grain: str,
    profile: str,
    columns: tuple[str, ...] | None,
    snapshot: Any,
    rows: tuple[Any, ...],
) -> None:
    mapping = _mapping()
    effective = (
        columns
        if columns is not None
        else (mapping.FOCUS_1_4_FULL_PROFILE_COLUMNS if profile == "full" else mapping.FOCUS_1_4_SUMMARY_COLUMNS)
    )
    request, snapshot, draft, data_files = _requested_case(
        grain=grain,
        profile=profile,
        columns=effective,
        snapshot=snapshot,
        rows=rows,
        created_at=datetime(2026, 8, 4, tzinfo=UTC) if grain == "monthly" else None,
    )

    payload = _requested_metadata_payload(
        request=request,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        ready_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    document = _metadata_root(payload)

    assert set(document) == {"x_ChitraguptaPreviewMetadata"}
    metadata = document["x_ChitraguptaPreviewMetadata"]
    assert metadata["contract_version"] == "chitragupta.focus-metadata.v1"
    assert metadata["metadata_conformance_status"] == "non_conforming_preview_metadata"
    assert metadata["target_focus_version"] == "1.4"
    assert metadata["conformance_status"] == "non_conforming"
    assert metadata["mapping_profile_version"] == "focus-1.4-preview-v1"
    assert metadata["known_gaps_authority"] == {
        "manifest": "manifest.json",
        "field": "known_gaps",
    }
    assert metadata["chitragupta_manifest"] == {
        "name": "manifest.json",
        "authority": [
            "checksums",
            "file_sizes",
            "expiry",
            "revisions",
            "known_gaps",
            "package_lifecycle",
        ],
    }
    assert metadata["x_ChitraguptaPreviewSchema"]["schema_semantics"] == (
        "target_vocabulary_import_metadata_not_focus_schema_conformance"
    )
    assert [item["column_name"] for item in metadata["x_ChitraguptaPreviewSchema"]["columns"]] == list(effective)
    assert [item["name"] for item in metadata["dataset_artifacts"]] == [item.name for item in data_files]
    assert [item["order"] for item in metadata["dataset_artifacts"]] == [item.order for item in data_files]
    assert "known_gaps" not in metadata
    assert "DataGenerator" not in document
    assert "DatasetInstance" not in document
    assert "Recency" not in document
    assert "Schema" not in document
    assert "FocusVersion" not in json.dumps(metadata["x_ChitraguptaPreviewSchema"], sort_keys=True)


def test_requested_multipart_package_appends_metadata_last_and_preserves_data_order() -> None:
    snapshot = _snapshot(
        start=date(2026, 7, 1),
        end=date(2026, 8, 1),
        monthly_status="settled",
        cutoff=date(2026, 8, 1),
        source_through=datetime(2026, 8, 1, tzinfo=UTC),
    )
    rows = (
        _row(day=1, BillingCurrency="USD"),
        _row(day=2, BillingCurrency="USD", AllocatedResourceId="sa-2"),
        _row(day=3, BillingCurrency="USD", AllocatedResourceId="sa-3"),
    )
    _, _, unpartitioned_draft, _ = _requested_case(
        grain="monthly",
        profile="full",
        columns=_mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        snapshot=snapshot,
        rows=rows,
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    lines = unpartitioned_draft.data_files[0].body.splitlines(keepends=True)
    max_csv_file_bytes = len(lines[0]) + max(len(line) for line in lines[1:])
    request, snapshot, draft, data_files = _requested_case(
        grain="monthly",
        profile="full",
        columns=_mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        snapshot=snapshot,
        rows=rows,
        max_csv_file_bytes=max_csv_file_bytes,
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    package_files = _requested_package_files(
        request=request,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        ready_at=datetime(2026, 8, 4, tzinfo=UTC),
    )

    assert len(package_files) == len(data_files) + 1
    assert [item.name for item in package_files[:-1]] == [item.name for item in data_files]
    assert [item.order for item in package_files] == list(range(1, len(package_files) + 1))
    assert package_files[-1].name == "focus-metadata.json"
    assert package_files[-1].media_type == "application/json"


def test_metadata_marks_contracted_unit_price_columns_as_projected_and_applicable() -> None:
    request, snapshot, draft, data_files = _requested_case(
        grain="daily",
        profile="full",
        columns=_mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        snapshot=_snapshot(source_through=datetime(2026, 7, 2, tzinfo=UTC)),
        rows=(_row(day=1, BillingCurrency="USD"),),
    )
    payload = _requested_metadata_payload(
        request=request,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        ready_at=datetime(2026, 7, 3, tzinfo=UTC),
    )
    columns = {
        item["column_name"]: item
        for item in _metadata_root(payload)["x_ChitraguptaPreviewMetadata"]["x_ChitraguptaPreviewSchema"]["columns"]
    }

    assert columns["ContractedUnitPrice"]["applicability"] == "applicable"
    assert columns["ContractedUnitPrice"]["source"] == "financial projection"
    assert (
        columns["ContractedUnitPrice"]["transformation"]
        == "copy ListUnitPrice because negotiated unit-price discounts are not supported in this profile"
    )
    assert columns["PricingCurrencyContractedUnitPrice"]["applicability"] == "applicable"
    assert columns["PricingCurrencyContractedUnitPrice"]["source"] == "financial projection"
    assert (
        columns["PricingCurrencyContractedUnitPrice"]["transformation"]
        == "copy PricingCurrencyListUnitPrice because negotiated unit-price discounts are not supported in this profile"
    )


def test_requested_and_revision_delivery_semantics_differ_exactly_by_correction_series() -> None:
    request, snapshot, draft, data_files = _requested_case(
        grain="monthly",
        profile="full",
        columns=_mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        snapshot=_snapshot(
            start=date(2026, 7, 1),
            end=date(2026, 8, 1),
            monthly_status="settled",
            cutoff=date(2026, 8, 1),
            source_through=datetime(2026, 8, 1, tzinfo=UTC),
        ),
        rows=(_row(day=1, BillingCurrency="USD"),),
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    requested_payload = _requested_metadata_payload(
        request=request,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        ready_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    revision_payload = _revision_metadata_payload(
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        published_at=datetime(2026, 8, 4, tzinfo=UTC),
    )

    requested_delivery = _metadata_root(requested_payload)["x_ChitraguptaPreviewMetadata"]["delivery"]
    revision_delivery = _metadata_root(revision_payload)["x_ChitraguptaPreviewMetadata"]["delivery"]

    assert requested_delivery == {
        "delivery_handling": "one_off_download",
        "correction_handling": "not_a_correction_series",
        "snapshot": "complete_requested_snapshot",
        "consumer_action": "consume_as_immutable_requested_package",
    }
    assert revision_delivery == {
        "delivery_handling": "Overwrite",
        "correction_handling": "Replacement",
        "snapshot": "complete",
        "consumer_action": "replace_do_not_aggregate",
    }


def test_metadata_identities_ignore_lifecycle_timestamps_but_change_with_logical_content() -> None:
    request, snapshot, draft, data_files = _requested_case(
        grain="monthly",
        profile="full",
        columns=_mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        snapshot=_snapshot(
            start=date(2026, 7, 1),
            end=date(2026, 8, 1),
            monthly_status="settled",
            cutoff=date(2026, 8, 1),
            source_through=datetime(2026, 8, 1, tzinfo=UTC),
        ),
        rows=(_row(day=1, BillingCurrency="USD"),),
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    first = _metadata_root(
        _requested_metadata_payload(
            request=request,
            snapshot=snapshot,
            draft=draft,
            data_files=data_files,
            ready_at=datetime(2026, 8, 4, tzinfo=UTC),
        )
    )["x_ChitraguptaPreviewMetadata"]
    second = _metadata_root(
        _requested_metadata_payload(
            request=request,
            snapshot=snapshot,
            draft=draft,
            data_files=data_files,
            ready_at=datetime(2026, 8, 6, tzinfo=UTC),
        )
    )["x_ChitraguptaPreviewMetadata"]
    changed_request, changed_snapshot, changed_draft, changed_files = _requested_case(
        grain="monthly",
        profile="full",
        columns=_mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        snapshot=snapshot,
        rows=(_row(day=1, BillingCurrency="USD", AllocatedResourceId="sa-2"),),
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    changed = _metadata_root(
        _requested_metadata_payload(
            request=changed_request,
            snapshot=changed_snapshot,
            draft=changed_draft,
            data_files=changed_files,
            ready_at=datetime(2026, 8, 4, tzinfo=UTC),
        )
    )["x_ChitraguptaPreviewMetadata"]

    assert (
        first["x_ChitraguptaPreviewDatasetInstance"]["dataset_instance_id"]
        == (second["x_ChitraguptaPreviewDatasetInstance"]["dataset_instance_id"])
    )
    assert first["x_ChitraguptaPreviewSchema"]["schema_id"] == second["x_ChitraguptaPreviewSchema"]["schema_id"]
    assert first["x_ChitraguptaPreviewRecency"]["recency_id"] == second["x_ChitraguptaPreviewRecency"]["recency_id"]
    assert (
        first["x_ChitraguptaPreviewDatasetInstance"]["dataset_instance_id"]
        != (changed["x_ChitraguptaPreviewDatasetInstance"]["dataset_instance_id"])
    )


def test_header_only_requested_metadata_falls_back_to_lifecycle_timestamp_without_identity_drift() -> None:
    snapshot = _snapshot(
        start=date(2026, 7, 1),
        end=date(2026, 7, 1),
        monthly_status="provisional",
        cutoff=date(2026, 7, 1),
        source_through=None,
    )
    request, snapshot, draft, data_files = _requested_case(
        grain="monthly",
        profile="full",
        columns=_mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        snapshot=snapshot,
        rows=(),
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
    )
    first_ready = datetime(2026, 7, 20, tzinfo=UTC)
    second_ready = datetime(2026, 7, 21, tzinfo=UTC)
    first = _metadata_root(
        _requested_metadata_payload(
            request=request,
            snapshot=snapshot,
            draft=draft,
            data_files=data_files,
            ready_at=first_ready,
        )
    )["x_ChitraguptaPreviewMetadata"]
    second = _metadata_root(
        _requested_metadata_payload(
            request=request,
            snapshot=snapshot,
            draft=draft,
            data_files=data_files,
            ready_at=second_ready,
        )
    )["x_ChitraguptaPreviewMetadata"]

    assert first["x_ChitraguptaPreviewDatasetInstance"]["dataset_instance_last_updated"] == "2026-07-20T00:00:00Z"
    assert first["x_ChitraguptaPreviewRecency"]["recency_last_updated"] == "2026-07-20T00:00:00Z"
    assert second["x_ChitraguptaPreviewDatasetInstance"]["dataset_instance_last_updated"] == "2026-07-21T00:00:00Z"
    assert (
        first["x_ChitraguptaPreviewDatasetInstance"]["dataset_instance_id"]
        == (second["x_ChitraguptaPreviewDatasetInstance"]["dataset_instance_id"])
    )
    assert first["x_ChitraguptaPreviewRecency"]["recency_id"] == second["x_ChitraguptaPreviewRecency"]["recency_id"]


@pytest.mark.parametrize(
    ("case", "tamper"),
    [
        ("missing-metadata-file", lambda package_files, _body: package_files[:-1]),
        ("duplicate-metadata-file", lambda package_files, _body: (*package_files, package_files[-1])),
        (
            "stale-data-artifact-name",
            lambda package_files, body: (
                *package_files[:-1],
                _replace_payload_body(
                    package_files[-1],
                    (
                        json.dumps(
                            {
                                **body,
                                "x_ChitraguptaPreviewMetadata": {
                                    **body["x_ChitraguptaPreviewMetadata"],
                                    "dataset_artifacts": [
                                        {
                                            **body["x_ChitraguptaPreviewMetadata"]["dataset_artifacts"][0],
                                            "name": "other.csv",
                                        }
                                    ],
                                },
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode(),
                ),
            ),
        ),
        (
            "wrong-conformance",
            lambda package_files, body: (
                *package_files[:-1],
                _replace_payload_body(
                    package_files[-1],
                    (
                        json.dumps(
                            {
                                **body,
                                "x_ChitraguptaPreviewMetadata": {
                                    **body["x_ChitraguptaPreviewMetadata"],
                                    "conformance_status": "conforming",
                                },
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode(),
                ),
            ),
        ),
        (
            "official-schema-focus-version",
            lambda package_files, body: (
                *package_files[:-1],
                _replace_payload_body(
                    package_files[-1],
                    (
                        json.dumps(
                            {
                                **body,
                                "Schema": {"FocusVersion": "1.4"},
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode(),
                ),
            ),
        ),
        (
            "missing-dataset-instance-last-updated",
            lambda package_files, body: (
                *package_files[:-1],
                _replace_payload_body(
                    package_files[-1],
                    (
                        json.dumps(
                            {
                                **body,
                                "x_ChitraguptaPreviewMetadata": {
                                    **body["x_ChitraguptaPreviewMetadata"],
                                    "x_ChitraguptaPreviewDatasetInstance": {
                                        key: value
                                        for key, value in body["x_ChitraguptaPreviewMetadata"][
                                            "x_ChitraguptaPreviewDatasetInstance"
                                        ].items()
                                        if key != "dataset_instance_last_updated"
                                    },
                                },
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode(),
                ),
            ),
        ),
        (
            "missing-recency-last-updated",
            lambda package_files, body: (
                *package_files[:-1],
                _replace_payload_body(
                    package_files[-1],
                    (
                        json.dumps(
                            {
                                **body,
                                "x_ChitraguptaPreviewMetadata": {
                                    **body["x_ChitraguptaPreviewMetadata"],
                                    "x_ChitraguptaPreviewRecency": {
                                        key: value
                                        for key, value in body["x_ChitraguptaPreviewMetadata"][
                                            "x_ChitraguptaPreviewRecency"
                                        ].items()
                                        if key != "recency_last_updated"
                                    },
                                },
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode(),
                ),
            ),
        ),
        (
            "non-canonical-json",
            lambda package_files, body: (
                *package_files[:-1],
                _replace_payload_body(
                    package_files[-1],
                    (json.dumps(body, indent=2) + "\n").encode(),
                ),
            ),
        ),
        (
            "malformed-json",
            lambda package_files, _body: (
                *package_files[:-1],
                _replace_payload_body(package_files[-1], b"{"),
            ),
        ),
    ],
)
def test_requested_metadata_validation_rejects_missing_duplicate_and_noncanonical_contract_breaks(
    case: str,
    tamper: Any,
) -> None:
    del case
    request, snapshot, draft, data_files = _requested_case(
        grain="daily",
        profile="full",
        columns=_mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        snapshot=_snapshot(source_through=datetime(2026, 7, 2, tzinfo=UTC)),
        rows=(_row(day=1, BillingCurrency="USD"),),
    )
    package_files = _requested_package_files(
        request=request,
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        ready_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    metadata_body = _metadata_root(package_files[-1])
    tampered_package_files = tamper(package_files, metadata_body)
    staged_files = tuple(_artifact_metadata(item) for item in tampered_package_files)

    with pytest.raises(_mapping().PreviewMappingError):
        _focus_metadata().validate_requested_focus_metadata_artifact(
            request=request,
            snapshot=snapshot,
            draft=draft,
            package_files=tampered_package_files,
            staged_files=staged_files,
            ready_at=datetime(2026, 8, 4, tzinfo=UTC),
            expires_at=datetime(2026, 8, 11, tzinfo=UTC),
        )


def test_revision_metadata_validation_rejects_requested_delivery_semantics_before_current_replacement() -> None:
    snapshot = _snapshot(
        start=date(2026, 7, 1),
        end=date(2026, 8, 1),
        monthly_status="settled",
        cutoff=date(2026, 8, 1),
        source_through=datetime(2026, 8, 1, tzinfo=UTC),
    )
    request, snapshot, draft, data_files = _requested_case(
        grain="monthly",
        profile="full",
        columns=_mapping().FOCUS_1_4_FULL_PROFILE_COLUMNS,
        snapshot=snapshot,
        rows=(_row(day=1, BillingCurrency="USD"),),
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    del request
    metadata_payload = _revision_metadata_payload(
        snapshot=snapshot,
        draft=draft,
        data_files=data_files,
        published_at=datetime(2026, 8, 4, tzinfo=UTC),
    )
    body = _metadata_root(metadata_payload)
    tampered_payload = _replace_payload_body(
        metadata_payload,
        (
            json.dumps(
                {
                    **body,
                    "x_ChitraguptaPreviewMetadata": {
                        **body["x_ChitraguptaPreviewMetadata"],
                        "delivery": {
                            "delivery_handling": "one_off_download",
                            "correction_handling": "not_a_correction_series",
                            "snapshot": "complete_requested_snapshot",
                            "consumer_action": "consume_as_immutable_requested_package",
                        },
                    },
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode(),
    )
    package_files = _focus_metadata().compose_package_artifacts(
        data_files=data_files,
        focus_metadata=tampered_payload,
    )
    staged_files = tuple(_artifact_metadata(item) for item in package_files)

    with pytest.raises(_mapping().PreviewMappingError):
        _focus_metadata().validate_revision_focus_metadata_artifact(
            revision_id="revision-1",
            tenant_name_at_publication="production",
            month="2026-07",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 8, 1),
            monthly_status="settled",
            material_sha256=_mapping().preview_revision_content_sha256(logical_data_sha256=draft.logical_data_sha256),
            supersedes_revision_id=None,
            snapshot=snapshot,
            draft=draft,
            package_files=package_files,
            staged_files=staged_files,
            published_at=datetime(2026, 8, 4, tzinfo=UTC),
        )
