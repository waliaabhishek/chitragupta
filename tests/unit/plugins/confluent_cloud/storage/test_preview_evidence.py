from __future__ import annotations

import inspect
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from sqlmodel import Session, create_engine

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.module import CoreStorageModule
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem, CCloudCostSourceRecord
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from plugins.confluent_cloud.storage.repositories import CCloudBillingRepository, CCloudChargebackRepository
from tests.unit.core.preview.conftest import preview_module


def _engine(tmp_path: Path):
    engine = create_engine(f"sqlite:///{tmp_path / 'preview-evidence.db'}")
    CoreStorageModule().register_tables(engine)
    CCloudStorageModule().register_tables(engine)
    return engine


def _source(
    source_id: str,
    *,
    tenant_id: str = "tenant-1",
    source_start: datetime | None = datetime(2026, 7, 1, tzinfo=UTC),
    source_end: datetime | None = datetime(2026, 7, 2, tzinfo=UTC),
    malformed: bool = False,
) -> CCloudCostSourceRecord:
    scope_start = source_start or datetime(2026, 7, 1, tzinfo=UTC)
    scope_end = source_end or datetime(2026, 7, 3, tzinfo=UTC)
    allocation = source_start or datetime(1970, 1, 1, tzinfo=UTC)
    return CCloudCostSourceRecord(
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        source_record_id=source_id,
        identity_scheme="provider_cost_id" if source_start else "composite_v1",
        provider_cost_id=source_id if source_start else None,
        source_period_start=source_start,
        source_period_end=source_end,
        collection_window_start=datetime(2026, 6, 30, tzinfo=UTC),
        collection_window_end=datetime(2026, 7, 4, tzinfo=UTC),
        evidence_scope_start=scope_start,
        evidence_scope_end=scope_end,
        allocation_timestamp=allocation,
        retention_timestamp=allocation if source_start else scope_end,
        granularity="DAILY",
        product="KAFKA",
        line_type="KAFKA_STORAGE",
        amount=Decimal("8.000"),
        original_amount=Decimal("10.000"),
        discount_amount=Decimal("2.000"),
        price=Decimal("2.000"),
        quantity=Decimal("5.000"),
        unit="GB",
        description="Kafka storage usage",
        network_access_type="PUBLIC_INTERNET",
        resource_id="lkc-1",
        resource_name="Orders",
        environment_id="env-1",
        billing_timestamp=allocation,
        billing_env_id="env-1",
        billing_resource_id="lkc-1",
        billing_product_type="KAFKA_STORAGE",
        billing_product_category="KAFKA",
        tier_dimensions={"z": "last", "a": "first"},
        malformed=malformed,
        diagnostics=("invalid_date:start_date",) if malformed else (),
        raw_payload={"id": source_id, "secret_extra": "preserved"},
    )


def _scope() -> object:
    evidence = preview_module("evidence")
    return evidence.PreviewEvidenceScope(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        start=datetime(2026, 7, 1, tzinfo=UTC),
        end=datetime(2026, 7, 2, tzinfo=UTC),
    )


def test_source_candidates_include_native_overlap_and_undated_evidence_overlap(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    with Session(engine) as session:
        repository = CCloudBillingRepository(session)
        repository.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [_source("valid"), _source("undated", source_start=None, source_end=None, malformed=True)],
        )
        session.commit()

    with Session(engine) as session:
        candidates = CCloudBillingRepository(session).find_preview_source_candidates(_scope())

    assert {item.source_record_id for item in candidates} == {"valid", "undated"}
    undated = next(item for item in candidates if item.source_record_id == "undated")
    assert undated.allocation_timestamp == datetime(1970, 1, 1, tzinfo=UTC)
    assert undated.malformed is True
    engine.dispose()


def test_source_candidate_query_is_tenant_scoped_deterministic_and_hard_limited_to_two(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    with Session(engine) as session:
        repository = CCloudBillingRepository(session)
        repository.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [_source("c"), _source("a"), _source("b")],
        )
        repository.replace_source_window(
            "confluent_cloud",
            "tenant-2",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [_source("other-tenant", tenant_id="tenant-2")],
        )
        session.commit()

    with Session(engine) as session:
        candidates = CCloudBillingRepository(session).find_preview_source_candidates(_scope())

    assert len(candidates) == 2
    assert [item.source_record_id for item in candidates] == ["a", "b"]
    engine.dispose()


def test_complete_source_iterator_returns_every_overlapping_record_in_order(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    with Session(engine) as session:
        repository = CCloudBillingRepository(session)
        repository.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [_source("c"), _source("a"), _source("b")],
        )
        repository.replace_source_window(
            "confluent_cloud",
            "tenant-2",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [_source("other", tenant_id="tenant-2")],
        )
        session.commit()

    with Session(engine) as session:
        rows = CCloudBillingRepository(session).iter_preview_sources(_scope())
        assert iter(rows) is rows
        first = next(rows)
        remaining = list(rows)

    assert [first.source_record_id, *(item.source_record_id for item in remaining)] == ["a", "b", "c"]
    engine.dispose()


def test_complete_source_iterator_includes_undated_malformed_evidence_overlap(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    with Session(engine) as session:
        repository = CCloudBillingRepository(session)
        repository.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [_source("valid"), _source("undated", source_start=None, source_end=None, malformed=True)],
        )
        session.commit()

    with Session(engine) as session:
        rows = list(CCloudBillingRepository(session).iter_preview_sources(_scope()))

    assert {item.source_record_id for item in rows} == {"valid", "undated"}
    engine.dispose()


def test_complete_aggregate_iterator_returns_every_scoped_row_in_origin_order(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    with Session(engine) as session:
        repository = CCloudBillingRepository(session)
        for resource_id in ("lkc-c", "lkc-a", "lkc-b"):
            repository.upsert(
                CCloudBillingLineItem(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    timestamp=datetime(2026, 7, 1, tzinfo=UTC),
                    env_id="env-1",
                    resource_id=resource_id,
                    product_category="KAFKA",
                    product_type="KAFKA_STORAGE",
                    quantity=Decimal("5"),
                    unit_price=Decimal("2"),
                    total_cost=Decimal("8"),
                    currency="USD",
                    granularity="daily",
                    metadata={},
                )
            )
        repository.upsert(
            CCloudBillingLineItem(
                ecosystem="confluent_cloud",
                tenant_id="tenant-2",
                timestamp=datetime(2026, 7, 1, tzinfo=UTC),
                env_id="env-1",
                resource_id="other",
                product_category="KAFKA",
                product_type="KAFKA_STORAGE",
                quantity=Decimal("5"),
                unit_price=Decimal("2"),
                total_cost=Decimal("8"),
                currency="USD",
                granularity="daily",
                metadata={},
            )
        )
        session.commit()

    with Session(engine) as session:
        rows = CCloudBillingRepository(session).iter_preview_aggregates(_scope())
        assert iter(rows) is rows
        first = next(rows)
        remaining = list(rows)

    assert [first.resource_id, *(item.resource_id for item in remaining)] == ["lkc-a", "lkc-b", "lkc-c"]
    engine.dispose()


def test_complete_iterator_implementation_does_not_materialize_all_rows() -> None:
    source = inspect.getsource(CCloudBillingRepository.iter_preview_sources)
    aggregates = inspect.getsource(CCloudBillingRepository.iter_preview_aggregates)

    assert ".all()" not in source
    assert "tuple(" not in source
    assert "yield_per" in source
    assert ".all()" not in aggregates
    assert "tuple(" not in aggregates
    assert "yield_per" in aggregates


def test_source_mapper_copies_all_values_and_canonicalizes_tiers(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    with Session(engine) as session:
        repository = CCloudBillingRepository(session)
        repository.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [_source("cost-1")],
        )
        session.commit()

    with Session(engine) as session:
        candidate = CCloudBillingRepository(session).find_preview_source_candidates(_scope())[0]

    assert candidate.provider_cost_id == "cost-1"
    assert candidate.amount == Decimal("8.000")
    assert candidate.original_amount == Decimal("10.000")
    assert candidate.discount_amount == Decimal("2.000")
    assert candidate.price == Decimal("2.000")
    assert candidate.quantity == Decimal("5.000")
    assert candidate.native_product == "KAFKA"
    assert candidate.native_line_type == "KAFKA_STORAGE"
    assert candidate.native_tier_dimensions == (("a", "first"), ("z", "last"))
    engine.dispose()


def test_aggregate_and_allocation_candidate_reads_use_exact_origin_keys_and_limit_two(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    source = _source("cost-1")
    with Session(engine) as session:
        billing = CCloudBillingRepository(session)
        billing.replace_source_window(
            "confluent_cloud",
            "tenant-1",
            datetime(2026, 6, 30, tzinfo=UTC),
            datetime(2026, 7, 4, tzinfo=UTC),
            [source],
        )
        for resource_id in ("lkc-1", "lkc-other"):
            billing.upsert(
                CCloudBillingLineItem(
                    ecosystem="confluent_cloud",
                    tenant_id="tenant-1",
                    timestamp=datetime(2026, 7, 1, tzinfo=UTC),
                    env_id="env-1",
                    resource_id=resource_id,
                    product_category="KAFKA",
                    product_type="KAFKA_STORAGE",
                    quantity=Decimal("5"),
                    unit_price=Decimal("2"),
                    total_cost=Decimal("8"),
                )
            )
        chargebacks = CCloudChargebackRepository(session)
        for identity_id in ("sa-1", "sa-2", "sa-3"):
            chargebacks.upsert_batch(
                [
                    ChargebackRow(
                        ecosystem="confluent_cloud",
                        tenant_id="tenant-1",
                        timestamp=datetime(2026, 7, 1, tzinfo=UTC),
                        resource_id="lkc-1",
                        product_category="KAFKA",
                        product_type="KAFKA_STORAGE",
                        identity_id=identity_id,
                        cost_type=CostType.USAGE,
                        amount=Decimal("8"),
                        allocation_method="direct",
                        metadata={"env_id": "env-1"},
                    )
                ]
            )
        session.commit()

    with Session(engine) as session:
        billing = CCloudBillingRepository(session)
        selected_source = billing.find_preview_source_candidates(_scope())[0]
        aggregates = billing.find_preview_aggregate_candidates(_scope(), selected_source)
        allocations = CCloudChargebackRepository(session).find_preview_allocation_candidates(_scope(), selected_source)

    assert len(aggregates) == 1
    assert aggregates[0].resource_id == "lkc-1"
    assert len(allocations) == 2
    assert [item.allocation_target_id for item in allocations] == ["sa-1", "sa-2"]
    engine.dispose()
