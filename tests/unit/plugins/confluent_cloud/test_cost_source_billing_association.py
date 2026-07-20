from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import respx

from core.preview.evidence import PreviewEvidenceScope
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.models.billing import billing_natural_key
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.plugins.confluent_cloud.storage.test_ccloud_cost_source_records import _record
from tests.unit.plugins.confluent_cloud.test_cost_input import (
    _billing_response,
    _cost_input,
    _SourceUow,
    _SourceWriter,
    _valid_cost,
)


def _association(record: object) -> tuple[object, ...]:
    return (
        record.ecosystem,
        record.tenant_id,
        record.billing_timestamp,
        record.billing_env_id,
        record.billing_resource_id,
        record.billing_product_type,
        record.billing_product_category,
    )


@respx.mock
def test_multiple_native_rows_keep_the_exact_shared_existing_aggregate_key() -> None:
    first = _valid_cost("tier-1", price="1", quantity="2", amount="2")
    second = _valid_cost("tier-2", price="2", quantity="3", amount="6")
    second["tier_dimensions"] = {"tier_start": "2", "tier_end": "5"}
    respx.get("https://api.confluent.cloud/billing/v1/costs").mock(return_value=_billing_response([first, second]))
    writer = _SourceWriter()

    aggregates = list(
        _cost_input().gather(
            "org-123",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 2, tzinfo=UTC),
            _SourceUow(writer),
        )
    )

    assert len(aggregates) == 1
    assert aggregates[0].total_cost == 8
    records = writer.calls[0][4]
    assert len(records) == 2
    expected_key = billing_natural_key(aggregates[0])
    assert [_association(record) for record in records] == [expected_key, expected_key]


@respx.mock
def test_resource_less_source_keeps_the_precise_generated_billing_resource_key() -> None:
    raw = _valid_cost("resource-less")
    raw["resource"] = {"environment": {"id": "env-1"}}
    respx.get("https://api.confluent.cloud/billing/v1/costs").mock(return_value=_billing_response([raw]))
    writer = _SourceWriter()

    aggregate = list(
        _cost_input().gather(
            "org-123",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 2, tzinfo=UTC),
            _SourceUow(writer),
        )
    )[0]
    record = writer.calls[0][4][0]

    assert aggregate.resource_id == "unresolved_billing_0"
    assert record.resource_id is None
    assert record.billing_resource_id == "unresolved_billing_0"
    assert _association(record) == billing_natural_key(aggregate)


def test_repository_round_trips_required_new_association_into_preview_evidence(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'source-association.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    record = _record(
        line_type="KAFKA_STORAGE",
        billing_timestamp=datetime(2026, 7, 2, tzinfo=UTC),
        billing_env_id="env-1",
        billing_resource_id="lkc-1",
        billing_product_type="KAFKA_STORAGE",
        billing_product_category="KAFKA",
    )
    with backend.create_unit_of_work() as uow:
        uow.billing.replace_source_window(
            "confluent_cloud",
            "org-1",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [record],
        )
        uow.commit()
    scope = PreviewEvidenceScope(
        "confluent_cloud",
        "org-1",
        datetime(2026, 7, 2, tzinfo=UTC),
        datetime(2026, 7, 3, tzinfo=UTC),
    )
    with backend.create_read_only_unit_of_work() as uow:
        persisted = tuple(uow.billing.iter_preview_sources(scope))[0]  # type: ignore[attr-defined]

    assert persisted.billing_timestamp == datetime(2026, 7, 2, tzinfo=UTC)
    assert persisted.billing_env_id == "env-1"
    assert persisted.billing_resource_id == "lkc-1"
    assert persisted.billing_product_type == "KAFKA_STORAGE"
    assert persisted.billing_product_category == "KAFKA"
    backend.dispose()


def test_repository_rejects_new_source_record_without_complete_billing_association(tmp_path: Path) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'source-association-required.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    with backend.create_unit_of_work() as uow, pytest.raises(ValueError, match="billing association"):
        uow.billing.replace_source_window(
            "confluent_cloud",
            "org-1",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [
                _record(
                    billing_timestamp=None,
                    billing_env_id=None,
                    billing_resource_id=None,
                    billing_product_type=None,
                    billing_product_category=None,
                )
            ],
        )
    backend.dispose()


@pytest.mark.parametrize(
    "override",
    [
        {"billing_timestamp": datetime(2026, 7, 3, tzinfo=UTC)},
        {"billing_env_id": "env-other"},
        {"billing_product_type": "KAFKA_PARTITION"},
        {"billing_product_category": "CONNECT"},
    ],
)
def test_repository_rejects_new_source_association_inconsistent_with_mapped_billing_identity(
    tmp_path: Path,
    override: dict[str, object],
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'source-association-inconsistent.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    values: dict[str, object] = {
        "billing_timestamp": datetime(2026, 7, 2, tzinfo=UTC),
        "billing_env_id": "env-1",
        "billing_resource_id": "lkc-1",
        "billing_product_type": "KAFKA_STORAGE",
        "billing_product_category": "KAFKA",
    }
    values.update(override)
    with backend.create_unit_of_work() as uow, pytest.raises(ValueError, match="billing association"):
        uow.billing.replace_source_window(
            "confluent_cloud",
            "org-1",
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [_record(**values)],
        )
    backend.dispose()
