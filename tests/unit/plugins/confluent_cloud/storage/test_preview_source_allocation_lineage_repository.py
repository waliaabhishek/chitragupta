from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Context, Decimal, localcontext
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from core.models.pipeline import PipelineState
from core.preview.evidence import (
    AllocationLineageRunStatus,
    AllocationLineageUnavailableReason,
    AllocationLineageUnavailableRun,
    PreviewAllocationEvidenceDecodeError,
    PreviewEvidenceScope,
)
from core.storage.backends.sqlmodel.engine import get_or_create_engine
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.interface import (
    AllocationLineageCapture,
    AllocationLineageFact,
    AllocationLineageRunCapture,
    AllocationTargetKind,
    LineageCaptureReason,
    LineageCaptureStatus,
)
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem, CCloudCostSourceRecord
from plugins.confluent_cloud.preview_bootstrap import CCloudBootstrappedLineageRefresher
from plugins.confluent_cloud.storage.module import CCloudStorageModule

START = datetime(2026, 7, 1, tzinfo=UTC)
END = datetime(2026, 7, 2, tzinfo=UTC)
COMPLETED_AT = datetime(2026, 7, 3, tzinfo=UTC)


@pytest.fixture
def backend(tmp_path: Path) -> SQLModelBackend:
    value = SQLModelBackend(
        f"sqlite:///{tmp_path / 'tier-lineage.db'}",
        CCloudStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    value.create_tables()
    yield value
    value.dispose()


def _source(
    source_id: str,
    *,
    amount: str,
    original: str,
    quantity: str | None,
    price: str | None,
    tier: str,
    discount: str = "0",
    line_type: str = "KAFKA_STORAGE",
    product: str | None = "KAFKA",
    unit: str | None = "GB",
    malformed: bool = False,
    diagnostics: tuple[str, ...] = (),
) -> CCloudCostSourceRecord:
    return CCloudCostSourceRecord(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        source_record_id=f"provider:{source_id}",
        identity_scheme="provider_cost_id",
        provider_cost_id=source_id,
        source_period_start=START,
        source_period_end=END,
        collection_window_start=START,
        collection_window_end=END,
        evidence_scope_start=START,
        evidence_scope_end=END,
        allocation_timestamp=START,
        retention_timestamp=START,
        granularity="DAILY",
        product=product,
        line_type=line_type,
        amount=Decimal(amount),
        original_amount=Decimal(original),
        discount_amount=Decimal(discount),
        price=None if price is None else Decimal(price),
        quantity=None if quantity is None else Decimal(quantity),
        unit=unit,
        description="tiered usage",
        network_access_type="PUBLIC_INTERNET",
        resource_id="lkc-1",
        resource_name="Orders",
        environment_id="env-1",
        tier_dimensions={"tier": tier},
        malformed=malformed,
        diagnostics=diagnostics,
        raw_payload={"id": source_id, "tier_dimensions": {"tier": tier}},
        billing_timestamp=START,
        billing_env_id="env-1",
        billing_resource_id="lkc-1",
        billing_product_type=line_type,
        billing_product_category=product or "",
    )


def _billing(*, cost: str, quantity: str, line_type: str = "KAFKA_STORAGE", product: str = "KAFKA") -> Any:
    return CCloudBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        timestamp=START,
        env_id="env-1",
        resource_id="lkc-1",
        product_category=product,
        product_type=line_type,
        quantity=Decimal(quantity),
        unit_price=Decimal(0),
        total_cost=Decimal(cost),
        currency="USD",
        granularity="daily",
    )


def _fact(
    ordinal: int,
    *,
    cost: str,
    quantity: str,
    target_id: str | None,
) -> AllocationLineageFact:
    target_kind = AllocationTargetKind.UNALLOCATED if target_id is None else AllocationTargetKind.IDENTITY
    return AllocationLineageFact(
        portion_ordinal=ordinal,
        target_kind=target_kind,
        target_id=target_id,
        allocated_cost=Decimal(cost),
        allocated_quantity=Decimal(quantity),
        allocation_ratio=Decimal("0"),
        method_id="direct",
        method_version="v1",
        method_details_json=(f'{{"allocation_detail":"direct","metadata":{{}},"target_kind":"{target_kind.value}"}}'),
    )


def _run(
    facts: tuple[AllocationLineageFact, ...],
    *,
    line_type: str = "KAFKA_STORAGE",
    product: str = "KAFKA",
    status: LineageCaptureStatus = LineageCaptureStatus.COMPLETE,
    reason: LineageCaptureReason | None = None,
) -> AllocationLineageRunCapture:
    total = sum((fact.allocated_cost for fact in facts), Decimal(0))
    normalized = tuple(
        AllocationLineageFact(
            portion_ordinal=fact.portion_ordinal,
            target_kind=fact.target_kind,
            target_id=fact.target_id,
            allocated_cost=fact.allocated_cost,
            allocated_quantity=fact.allocated_quantity,
            allocation_ratio=Decimal(0) if total == 0 else fact.allocated_cost / total,
            method_id=fact.method_id,
            method_version=fact.method_version,
            method_details_json=fact.method_details_json,
        )
        for fact in facts
    )
    capture = AllocationLineageCapture(
        origin_timestamp=START,
        origin_env_id="env-1",
        origin_resource_id="lkc-1",
        origin_product_type=line_type,
        origin_product_category=product,
        status=status,
        reason=reason,
        facts=normalized if status is LineageCaptureStatus.COMPLETE else (),
    )
    return AllocationLineageRunCapture(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        tracking_date=date(2026, 7, 1),
        calculation_id="calculation-1",
        captures=(capture,),
    )


def _persist_origins(
    backend: SQLModelBackend,
    *,
    billing: CCloudBillingLineItem,
    sources: list[CCloudCostSourceRecord],
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.billing.upsert(billing)
        uow.billing.replace_source_window(  # type: ignore[attr-defined]
            "confluent_cloud",
            "org-1",
            START,
            END,
            sources,
        )
        uow.commit()


def _mark_source_as_bootstrapped_legacy(
    backend: SQLModelBackend,
    *,
    source_record_id: str,
    capture_id: str,
) -> None:
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE ccloud_cost_source_records
                SET capture_id = :capture_id,
                    billing_timestamp = NULL,
                    billing_env_id = NULL,
                    billing_resource_id = NULL,
                    billing_product_type = NULL,
                    billing_product_category = NULL
                WHERE source_record_id = :source_record_id
                """
            ),
            {
                "capture_id": capture_id,
                "source_record_id": source_record_id,
            },
        )


def _replace(backend: SQLModelBackend, run: AllocationLineageRunCapture) -> Any:
    with backend.create_preview_evidence_unit_of_work() as uow:
        value = uow.allocation_lineage.replace_calculation_lineage(
            run,
            calculation_completed_at=COMPLETED_AT,
        )
        uow.commit()
        return value


def _persist_calculated_state_and_generic_lineage(
    backend: SQLModelBackend,
    run: AllocationLineageRunCapture,
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.pipeline_state.upsert(
            PipelineState(
                ecosystem=run.ecosystem,
                tenant_id=run.tenant_id,
                tracking_date=run.tracking_date,
                billing_gathered=True,
                resources_gathered=True,
                chargeback_calculated=True,
                calculation_id=run.calculation_id,
                calculation_completed_at=COMPLETED_AT,
                calculation_run_id=None,
            )
        )
        uow.chargebacks.replace_calculation_lineage(  # type: ignore[attr-defined]
            run,
            calculation_completed_at=COMPLETED_AT,
        )
        uow.commit()


def _refresh_bootstrapped_lineage(
    backend: SQLModelBackend,
    capture_ids: tuple[str, ...],
) -> None:
    with backend.create_preview_evidence_unit_of_work() as uow:
        refresher = uow.allocation_lineage
        assert isinstance(refresher, CCloudBootstrappedLineageRefresher)
        refresher.refresh_bootstrapped_lineage(capture_ids)
        uow.commit()


def _scope() -> PreviewEvidenceScope:
    return PreviewEvidenceScope(
        ecosystem="confluent_cloud",
        tenant_id="org-1",
        start=START,
        end=END,
    )


def test_bootstrap_refresh_derives_legacy_association_and_exact_lineage(
    backend: SQLModelBackend,
) -> None:
    source = _source(
        "legacy",
        amount="8",
        original="10",
        discount="2",
        quantity="5",
        price="2",
        tier="standard",
    )
    _persist_origins(backend, billing=_billing(cost="8", quantity="5"), sources=[source])
    _mark_source_as_bootstrapped_legacy(
        backend,
        source_record_id="provider:legacy",
        capture_id="legacy:v1:capture-1",
    )
    run = _run(
        (
            _fact(0, cost="5", quantity="3.125", target_id="sa-1"),
            _fact(1, cost="3", quantity="1.875", target_id=None),
        )
    )
    _persist_calculated_state_and_generic_lineage(backend, run)

    _refresh_bootstrapped_lineage(backend, ("legacy:v1:capture-1",))

    engine = get_or_create_engine(backend._connection_string)
    with engine.connect() as connection:
        association = connection.execute(
            text(
                """
                SELECT billing_timestamp, billing_env_id, billing_resource_id,
                       billing_product_type, billing_product_category
                FROM ccloud_cost_source_records
                WHERE source_record_id = 'provider:legacy'
                """
            )
        ).one()
        generic_rows = connection.execute(
            text(
                """
                SELECT portion_ordinal, allocated_cost, allocated_quantity
                FROM ccloud_allocation_lineage_portions
                ORDER BY portion_ordinal
                """
            )
        ).all()
    with backend.create_preview_generation_read_unit_of_work() as uow:
        exact_rows = tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",)))
    assert association[1:] == ("env-1", "lkc-1", "KAFKA_STORAGE", "KAFKA")
    assert generic_rows == [(0, "5", "3.125"), (1, "3", "1.875")]
    assert [(row.allocated_cost, row.allocated_original_cost) for row in exact_rows] == [
        (Decimal("5"), Decimal("6.25")),
        (Decimal("3"), Decimal("3.75")),
    ]


@pytest.mark.parametrize(
    "capture_ids",
    [
        (),
        ("",),
        ("legacy:v1:capture-1", "legacy:v1:capture-1"),
        ("missing",),
    ],
    ids=("empty", "blank", "duplicate", "missing"),
)
def test_bootstrap_refresh_rejects_invalid_capture_selection_without_writes(
    backend: SQLModelBackend,
    capture_ids: tuple[str, ...],
) -> None:
    source = _source(
        "legacy",
        amount="8",
        original="10",
        discount="2",
        quantity="5",
        price="2",
        tier="standard",
    )
    _persist_origins(backend, billing=_billing(cost="8", quantity="5"), sources=[source])
    _mark_source_as_bootstrapped_legacy(
        backend,
        source_record_id="provider:legacy",
        capture_id="legacy:v1:capture-1",
    )
    _persist_calculated_state_and_generic_lineage(
        backend,
        _run((_fact(0, cost="8", quantity="5", target_id="sa-1"),)),
    )

    with pytest.raises(ValueError):
        _refresh_bootstrapped_lineage(backend, capture_ids)

    engine = get_or_create_engine(backend._connection_string)
    with engine.connect() as connection:
        association = connection.execute(
            text(
                """
                SELECT billing_timestamp, billing_env_id, billing_resource_id,
                       billing_product_type, billing_product_category
                FROM ccloud_cost_source_records
                WHERE source_record_id = 'provider:legacy'
                """
            )
        ).one()
        exact_count = connection.execute(
            text("SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions")
        ).scalar_one()
    assert association == (None, None, None, None, None)
    assert exact_count == 0


@pytest.mark.parametrize(
    ("capture_id", "mutation"),
    [
        (
            "legacy:v1:capture-1",
            "UPDATE ccloud_cost_source_records SET billing_env_id = 'env-1'",
        ),
        (
            "ordinary:capture-1",
            "UPDATE ccloud_cost_source_records SET capture_id = 'ordinary:capture-1'",
        ),
        (
            "legacy:v1:capture-1",
            "UPDATE ccloud_cost_source_records SET resource_id = NULL",
        ),
        (
            "legacy:v1:capture-1",
            "UPDATE ccloud_cost_source_records SET resource_id = 'lkc-other'",
        ),
        (
            "legacy:v1:capture-1",
            """
            UPDATE ccloud_cost_source_records
            SET billing_timestamp = '2026-07-01 00:00:00.000000',
                billing_env_id = 'env-1',
                billing_resource_id = 'lkc-other',
                billing_product_type = 'KAFKA_STORAGE',
                billing_product_category = 'KAFKA'
            """,
        ),
    ],
    ids=("partial", "non-legacy", "incomplete", "no-match", "conflicting"),
)
def test_bootstrap_refresh_rejects_invalid_legacy_association_without_sidecar(
    backend: SQLModelBackend,
    capture_id: str,
    mutation: str,
) -> None:
    source = _source(
        "legacy",
        amount="8",
        original="10",
        discount="2",
        quantity="5",
        price="2",
        tier="standard",
    )
    _persist_origins(backend, billing=_billing(cost="8", quantity="5"), sources=[source])
    _mark_source_as_bootstrapped_legacy(
        backend,
        source_record_id="provider:legacy",
        capture_id="legacy:v1:capture-1",
    )
    _persist_calculated_state_and_generic_lineage(
        backend,
        _run((_fact(0, cost="8", quantity="5", target_id="sa-1"),)),
    )
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(text(mutation))

    with pytest.raises(ValueError):
        _refresh_bootstrapped_lineage(backend, (capture_id,))

    with engine.connect() as connection:
        exact_count = connection.execute(
            text("SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions")
        ).scalar_one()
    assert exact_count == 0


@pytest.mark.parametrize(
    ("capture_id", "mutation"),
    [
        (
            "legacy:v1:capture-1",
            "UPDATE ccloud_cost_source_records SET billing_env_id = 'env-1'",
        ),
        (
            "ordinary:capture-1",
            "UPDATE ccloud_cost_source_records SET capture_id = 'ordinary:capture-1'",
        ),
        (
            "legacy:v1:capture-1",
            "UPDATE ccloud_cost_source_records SET resource_id = NULL",
        ),
        (
            "legacy:v1:capture-1",
            "UPDATE ccloud_cost_source_records SET resource_id = 'lkc-other'",
        ),
        (
            "legacy:v1:capture-1",
            """
            UPDATE ccloud_cost_source_records
            SET billing_timestamp = '2026-07-01 00:00:00.000000',
                billing_env_id = 'env-1',
                billing_resource_id = 'lkc-other',
                billing_product_type = 'KAFKA_STORAGE',
                billing_product_category = 'KAFKA'
            """,
        ),
    ],
    ids=("partial", "non-legacy", "incomplete", "no-match", "conflicting"),
)
def test_ordinary_lineage_rejects_invalid_legacy_association_without_partial_writes(
    backend: SQLModelBackend,
    capture_id: str,
    mutation: str,
) -> None:
    source = _source(
        "legacy",
        amount="8",
        original="10",
        discount="2",
        quantity="5",
        price="2",
        tier="standard",
    )
    _persist_origins(backend, billing=_billing(cost="8", quantity="5"), sources=[source])
    _mark_source_as_bootstrapped_legacy(
        backend,
        source_record_id="provider:legacy",
        capture_id="legacy:v1:capture-1",
    )
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(text(mutation))

    with pytest.raises(ValueError):
        _replace(
            backend,
            _run((_fact(0, cost="8", quantity="5", target_id="sa-1"),)),
        )

    with engine.connect() as connection:
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM ccloud_allocation_lineage_runs),
                    (SELECT COUNT(*) FROM ccloud_allocation_lineage_portions),
                    (SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions)
                """
            )
        ).one()
        persisted_capture = connection.execute(text("SELECT capture_id FROM ccloud_cost_source_records")).scalar_one()
    assert counts == (0, 0, 0)
    assert persisted_capture == capture_id


def test_ordinary_lineage_rejects_complete_legacy_association_conflicting_with_retained_identity(
    backend: SQLModelBackend,
) -> None:
    source = _source(
        "legacy",
        amount="8",
        original="10",
        discount="2",
        quantity="5",
        price="2",
        tier="standard",
    )
    _persist_origins(
        backend,
        billing=replace(_billing(cost="8", quantity="5"), resource_id="lkc-other"),
        sources=[source],
    )
    _mark_source_as_bootstrapped_legacy(
        backend,
        source_record_id="provider:legacy",
        capture_id="legacy:v1:capture-1",
    )
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE ccloud_cost_source_records
                SET billing_timestamp = '2026-07-01 00:00:00.000000',
                    billing_env_id = 'env-1',
                    billing_resource_id = 'lkc-other',
                    billing_product_type = 'KAFKA_STORAGE',
                    billing_product_category = 'KAFKA'
                """
            )
        )
    run = _run((_fact(0, cost="8", quantity="5", target_id="sa-1"),))
    conflicting_run = replace(
        run,
        captures=(
            replace(
                run.captures[0],
                origin_resource_id="lkc-other",
            ),
        ),
    )

    with pytest.raises(ValueError, match="billing association conflicts"):
        _replace(backend, conflicting_run)

    with engine.connect() as connection:
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM ccloud_allocation_lineage_runs),
                    (SELECT COUNT(*) FROM ccloud_allocation_lineage_portions),
                    (SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions)
                """
            )
        ).one()
    assert counts == (0, 0, 0)


def test_same_key_tiers_persist_exact_cost_quantity_and_original_cost_cells(
    backend: SQLModelBackend,
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="8", quantity="5"),
        sources=[
            _source("tier-a", amount="3", original="4", discount="1", quantity="2", price="2", tier="a"),
            _source("tier-b", amount="5", original="6", discount="1", quantity="3", price="2", tier="b"),
        ],
    )
    run = _replace(
        backend,
        _run(
            (
                _fact(0, cost="5", quantity="3.125", target_id="sa-1"),
                _fact(1, cost="3", quantity="1.875", target_id=None),
            )
        ),
    )

    with backend.create_preview_generation_read_unit_of_work() as uow:
        allocations = tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",)))
        aggregates = tuple(uow.cost_evidence.iter_preview_aggregates(_scope()))
        sources = tuple(uow.cost_evidence.iter_preview_sources(_scope()))
    assert run.portion_count == 2
    assert run.preview_portion_count == 4
    assert len(aggregates) == 2
    assert [(row.source_record_id, row.total_cost, row.quantity, row.unit_price) for row in aggregates] == [
        ("provider:tier-a", Decimal("3"), Decimal("2"), Decimal("2")),
        ("provider:tier-b", Decimal("5"), Decimal("3"), Decimal("2")),
    ]
    assert [
        (
            row.source_record_id,
            row.portion_ordinal,
            row.allocated_cost,
            row.allocated_quantity,
            row.allocated_original_cost,
            row.origin_original_cost,
        )
        for row in allocations
    ] == [
        ("provider:tier-a", 0, Decimal("1.88"), Decimal("1.250"), Decimal("2.51"), Decimal("4")),
        ("provider:tier-a", 1, Decimal("1.12"), Decimal("0.750"), Decimal("1.49"), Decimal("4")),
        ("provider:tier-b", 0, Decimal("3.12"), Decimal("1.875"), Decimal("3.74"), Decimal("6")),
        ("provider:tier-b", 1, Decimal("1.88"), Decimal("1.125"), Decimal("2.26"), Decimal("6")),
    ]
    assert [row.native_tier_dimensions for row in sources] == [
        (("tier", "a"),),
        (("tier", "b"),),
    ]


def test_sidecar_decimal_codec_supports_small_exponents_and_repeating_ratios(
    backend: SQLModelBackend,
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="3E-7", quantity="3E-7"),
        sources=[
            _source(
                "small",
                amount="3E-7",
                original="3E-7",
                quantity="3E-7",
                price="1",
                tier="small",
            )
        ],
    )
    _replace(
        backend,
        _run(
            (
                _fact(0, cost="1E-7", quantity="1E-7", target_id="sa-1"),
                _fact(1, cost="2E-7", quantity="2E-7", target_id=None),
            )
        ),
    )

    engine = get_or_create_engine(backend._connection_string)
    with engine.connect() as connection:
        persisted = connection.execute(
            text(
                """
                SELECT allocated_cost, allocation_ratio
                FROM ccloud_preview_source_allocation_lineage_portions
                ORDER BY portion_ordinal
                """
            )
        ).all()
    with backend.create_preview_generation_read_unit_of_work() as uow:
        rows = tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",)))
    with localcontext(Context(prec=38)):
        expected_ratio = Decimal(1) / Decimal(3)
        expected_second_ratio = Decimal(2) / Decimal(3)
    assert persisted == [("1E-7", str(expected_ratio)), ("2E-7", str(expected_second_ratio))]
    assert [row.allocated_cost for row in rows] == [Decimal("1E-7"), Decimal("2E-7")]
    assert rows[0].allocation_ratio == expected_ratio


@pytest.mark.parametrize(
    ("corruption", "values"),
    [
        ("DELETE FROM ccloud_preview_source_allocation_lineage_portions", {}),
        (
            "UPDATE ccloud_preview_source_allocation_lineage_portions "
            "SET method_version = 'v2' WHERE portion_ordinal = 0",
            {},
        ),
    ],
    ids=("zero-portions", "compatibility-shape"),
)
def test_preview_reader_derives_expected_sidecar_shape_independently(
    backend: SQLModelBackend,
    corruption: str,
    values: dict[str, object],
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="8", quantity="5"),
        sources=[
            _source("tier-a", amount="8", original="10", discount="2", quantity="5", price="2", tier="a"),
        ],
    )
    _replace(
        backend,
        _run(
            (
                _fact(0, cost="5", quantity="3.125", target_id="sa-1"),
                _fact(1, cost="3", quantity="1.875", target_id=None),
            )
        ),
    )
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(text(corruption), values)

    with backend.create_preview_generation_read_unit_of_work() as uow:
        runs = tuple(uow.allocation_evidence.iter_preview_allocation_runs(_scope(), ("calculation-1",)))
        assert runs[0].preview_portion_count == 2
        if corruption.startswith("DELETE"):
            assert tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",))) == ()
        else:
            with pytest.raises(PreviewAllocationEvidenceDecodeError):
                tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",)))


@pytest.mark.parametrize(
    ("source_rows", "billing_cost", "billing_quantity", "facts", "expected"),
    [
        (
            (
                ("charge", "10", "10", "5", "2", "charge"),
                ("refund", "-2", "-2", "-1", "2", "refund"),
            ),
            "8",
            "4",
            (("6", "3"), ("2", "1")),
            (
                ("charge", "7.5", "3.75"),
                ("charge", "2.5", "1.25"),
                ("refund", "-1.5", "-0.75"),
                ("refund", "-0.5", "-0.25"),
            ),
        ),
        (
            (
                ("charge", "2", "2", "1", "2", "charge"),
                ("refund", "-10", "-10", "-5", "2", "refund"),
            ),
            "-8",
            "-4",
            (("-6", "-3"), ("-2", "-1")),
            (
                ("charge", "1.5", "0.75"),
                ("charge", "0.5", "0.25"),
                ("refund", "-7.5", "-3.75"),
                ("refund", "-2.5", "-1.25"),
            ),
        ),
    ],
    ids=["positive-net", "negative-net"],
)
def test_mixed_sign_tiers_preserve_signed_rows_and_generic_columns(
    backend: SQLModelBackend,
    source_rows: tuple[tuple[str, str, str, str, str, str], ...],
    billing_cost: str,
    billing_quantity: str,
    facts: tuple[tuple[str, str], ...],
    expected: tuple[tuple[str, str, str], ...],
) -> None:
    sources = [
        _source(
            source_id,
            amount=amount,
            original=original,
            quantity=quantity,
            price=price,
            tier=tier,
        )
        for source_id, amount, original, quantity, price, tier in source_rows
    ]
    _persist_origins(
        backend,
        billing=_billing(cost=billing_cost, quantity=billing_quantity),
        sources=sources,
    )
    _replace(
        backend,
        _run(
            tuple(
                _fact(index, cost=cost, quantity=quantity, target_id=f"sa-{index + 1}")
                for index, (cost, quantity) in enumerate(facts)
            )
        ),
    )

    with backend.create_preview_generation_read_unit_of_work() as uow:
        rows = tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",)))

    assert [
        (row.source_record_id.removeprefix("provider:"), str(row.allocated_cost), str(row.allocated_quantity))
        for row in rows
    ] == list(expected)
    assert [
        sum((row.allocated_cost for row in rows if row.portion_ordinal == ordinal), Decimal(0)) for ordinal in range(2)
    ] == [Decimal(cost) for cost, _quantity in facts]


def test_zero_net_signed_quantity_uses_billed_cost_bridge_and_keeps_generic_columns_zero(
    backend: SQLModelBackend,
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="4", quantity="0.00"),
        sources=[
            _source("positive", amount="6", original="10", discount="4", quantity="5.00", price="2", tier="a"),
            _source(
                "negative",
                amount="-2",
                original="-10",
                discount="-8",
                quantity="-5.00",
                price="2",
                tier="b",
            ),
        ],
    )
    _replace(
        backend,
        _run(
            (
                _fact(0, cost="3", quantity="0.00", target_id="sa-1"),
                _fact(1, cost="1", quantity="0.00", target_id=None),
            )
        ),
    )

    with backend.create_preview_generation_read_unit_of_work() as uow:
        rows = tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",)))

    assert [row.allocated_quantity for row in rows] == [
        Decimal("3.75"),
        Decimal("1.25"),
        Decimal("-3.75"),
        Decimal("-1.25"),
    ]
    assert [
        sum((row.allocated_quantity for row in rows if row.portion_ordinal == ordinal), Decimal(0))
        for ordinal in range(2)
    ] == [
        Decimal(0),
        Decimal(0),
    ]


def test_zero_net_quantity_bridge_uses_ordinal_tie_break_and_all_zero_weight_fallback() -> None:
    from plugins.confluent_cloud.storage import preview_repositories

    apportion = preview_repositories._apportion_zero_net_quantity

    assert apportion(
        source_quantities=(Decimal("1"), Decimal("-1")),
        generic_allocated_costs=(Decimal("0"), Decimal("0"), Decimal("0")),
        portion_ordinals=(2, 0, 1),
    ) == (
        (Decimal("0"), Decimal("1"), Decimal("0")),
        (Decimal("0"), Decimal("-1"), Decimal("0")),
    )


def test_all_zero_native_quantities_emit_an_all_zero_matrix(backend: SQLModelBackend) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="8", quantity="0"),
        sources=[
            _source("tier-a", amount="3", original="0", discount="-3", quantity="0", price="0", tier="a"),
            _source("tier-b", amount="5", original="0", discount="-5", quantity="0", price="0", tier="b"),
        ],
    )
    _replace(
        backend,
        _run(
            (
                _fact(0, cost="5", quantity="0", target_id="sa-1"),
                _fact(1, cost="3", quantity="0", target_id=None),
            )
        ),
    )

    with backend.create_preview_generation_read_unit_of_work() as uow:
        rows = tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",)))

    assert len(rows) == 4
    assert {row.allocated_quantity for row in rows} == {Decimal(0)}


def test_promotional_allowance_normalizes_nullable_price_and_quantity_without_inventing_unit(
    backend: SQLModelBackend,
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="-5", quantity="0", line_type="PROMO_CREDIT", product=""),
        sources=[
            _source(
                "promo",
                amount="-5",
                original="-5",
                quantity=None,
                price=None,
                tier="allowance",
                line_type="PROMO_CREDIT",
                product=None,
                unit=None,
            )
        ],
    )
    _replace(
        backend,
        _run(
            (_fact(0, cost="-5", quantity="0", target_id="sa-1"),),
            line_type="PROMO_CREDIT",
            product="",
        ),
    )

    with backend.create_preview_generation_read_unit_of_work() as uow:
        aggregate = tuple(uow.cost_evidence.iter_preview_aggregates(_scope()))
        allocation = tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",)))
        sources = tuple(uow.cost_evidence.iter_preview_sources(_scope()))

    assert len(aggregate) == len(allocation) == 1
    assert (aggregate[0].unit_price, aggregate[0].quantity) == (Decimal(0), Decimal(0))
    assert allocation[0].origin_original_cost == Decimal("-5")
    assert allocation[0].allocated_original_cost == Decimal("-5")
    assert sources[0].unit is None


@pytest.mark.parametrize(
    "sources",
    [
        [],
        [
            _source(
                "malformed",
                amount="8",
                original="10",
                discount="2",
                quantity="5",
                price="2",
                tier="bad",
                malformed=True,
                diagnostics=("malformed:tier_dimensions",),
            )
        ],
        [_source("bad-arithmetic", amount="8", original="11", quantity="5", price="2", tier="bad")],
        [_source("nonfinite", amount="NaN", original="10", quantity="5", price="2", tier="bad")],
    ],
    ids=["missing-source", "malformed-tier", "invalid-arithmetic", "nonfinite-economics"],
)
def test_sidecar_rejects_missing_malformed_and_arithmetically_invalid_sources_atomically(
    backend: SQLModelBackend,
    sources: list[CCloudCostSourceRecord],
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="8", quantity="5"),
        sources=sources,
    )

    with pytest.raises(ValueError), backend.create_preview_evidence_unit_of_work() as uow:
        uow.allocation_lineage.replace_calculation_lineage(
            _run((_fact(0, cost="8", quantity="5", target_id="sa-1"),)),
            calculation_completed_at=COMPLETED_AT,
        )

    engine = get_or_create_engine(backend._connection_string)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM ccloud_allocation_lineage_runs")).scalar_one() == 0
        assert (
            connection.execute(
                text("SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions")
            ).scalar_one()
            == 0
        )


def test_duplicate_exact_source_association_rolls_back_compatibility_and_sidecar_atomically(
    backend: SQLModelBackend,
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="8", quantity="5"),
        sources=[
            _source("tier-a", amount="8", original="10", discount="2", quantity="5", price="2", tier="a"),
        ],
    )
    run = _run((_fact(0, cost="8", quantity="5", target_id="sa-1"),))
    duplicate = replace(run, captures=(run.captures[0], run.captures[0]))

    with (
        pytest.raises(ValueError, match="association is ambiguous"),
        backend.create_preview_evidence_unit_of_work() as uow,
    ):
        uow.allocation_lineage.replace_calculation_lineage(
            duplicate,
            calculation_completed_at=COMPLETED_AT,
        )

    engine = get_or_create_engine(backend._connection_string)
    with engine.connect() as connection:
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM ccloud_allocation_lineage_runs),
                    (SELECT COUNT(*) FROM ccloud_allocation_lineage_portions),
                    (SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions)
                """
            )
        ).one()
    assert counts == (0, 0, 0)


def test_sidecar_rejects_unequal_generic_cost_and_quantity_margins(backend: SQLModelBackend) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="8", quantity="5"),
        sources=[
            _source("tier-a", amount="3", original="4", discount="1", quantity="2", price="2", tier="a"),
            _source("tier-b", amount="5", original="6", discount="1", quantity="3", price="2", tier="b"),
        ],
    )

    with pytest.raises(ValueError), backend.create_preview_evidence_unit_of_work() as uow:
        uow.allocation_lineage.replace_calculation_lineage(
            _run(
                (
                    _fact(0, cost="7", quantity="4", target_id="sa-1"),
                    _fact(1, cost="2", quantity="2", target_id=None),
                )
            ),
            calculation_completed_at=COMPLETED_AT,
        )


def test_generic_billing_lineage_and_compatibility_count_remain_unchanged(
    backend: SQLModelBackend,
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="8", quantity="5"),
        sources=[
            _source("tier-a", amount="3", original="4", discount="1", quantity="2", price="2", tier="a"),
            _source("tier-b", amount="5", original="6", discount="1", quantity="3", price="2", tier="b"),
        ],
    )
    run = _replace(
        backend,
        _run(
            (
                _fact(0, cost="5", quantity="3.125", target_id="sa-1"),
                _fact(1, cost="3", quantity="1.875", target_id=None),
            )
        ),
    )

    engine = get_or_create_engine(backend._connection_string)
    with engine.connect() as connection:
        billing = connection.execute(text("SELECT quantity, unit_price, total_cost FROM ccloud_billing")).one()
        compatibility = connection.execute(
            text(
                """
                SELECT portion_ordinal, target_kind, target_id, allocated_cost,
                       allocated_quantity, allocation_ratio
                FROM ccloud_allocation_lineage_portions
                ORDER BY portion_ordinal
                """
            )
        ).all()
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT portion_count FROM ccloud_allocation_lineage_runs),
                    (SELECT COUNT(*) FROM ccloud_allocation_lineage_portions),
                    (SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions)
                """
            )
        ).one()
    assert billing == ("5", "0", "8")
    assert compatibility == [
        (0, "identity", "sa-1", "5", "3.125", "0.625"),
        (1, "unallocated", None, "3", "1.875", "0.375"),
    ]
    assert counts == (2, 2, 4)
    assert (run.portion_count, run.preview_portion_count) == (2, 4)


def test_marking_lineage_unavailable_deletes_compatibility_and_exact_source_portions(
    backend: SQLModelBackend,
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="8", quantity="5"),
        sources=[
            _source("tier-a", amount="3", original="4", discount="1", quantity="2", price="2", tier="a"),
            _source("tier-b", amount="5", original="6", discount="1", quantity="3", price="2", tier="b"),
        ],
    )
    _replace(
        backend,
        _run(
            (
                _fact(0, cost="5", quantity="3.125", target_id="sa-1"),
                _fact(1, cost="3", quantity="1.875", target_id=None),
            )
        ),
    )

    with backend.create_preview_evidence_unit_of_work() as uow:
        uow.allocation_lineage.mark_calculation_lineage_unavailable(
            AllocationLineageUnavailableRun(
                ecosystem="confluent_cloud",
                tenant_id="org-1",
                tracking_date=date(2026, 7, 1),
                calculation_id="calculation-2",
                calculation_completed_at=datetime(2026, 7, 4, tzinfo=UTC),
                status=AllocationLineageRunStatus.UNAVAILABLE,
                reason=AllocationLineageUnavailableReason.CAPTURE_FAILED,
            )
        )
        uow.commit()

    engine = get_or_create_engine(backend._connection_string)
    with engine.connect() as connection:
        run = connection.execute(
            text("SELECT capture_status, capture_reason, portion_count FROM ccloud_allocation_lineage_runs")
        ).one()
        counts = connection.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM ccloud_allocation_lineage_portions),
                    (SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions)
                """
            )
        ).one()
    assert run == ("unavailable", "capture_failed", 0)
    assert counts == (0, 0)


def test_preview_reader_rejects_orphaned_exact_source_sidecar_instead_of_omitting_it(
    backend: SQLModelBackend,
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="8", quantity="5"),
        sources=[
            _source("tier-a", amount="3", original="4", discount="1", quantity="2", price="2", tier="a"),
            _source("tier-b", amount="5", original="6", discount="1", quantity="3", price="2", tier="b"),
        ],
    )
    _replace(
        backend,
        _run(
            (
                _fact(0, cost="5", quantity="3.125", target_id="sa-1"),
                _fact(1, cost="3", quantity="1.875", target_id=None),
            )
        ),
    )
    engine = get_or_create_engine(backend._connection_string)
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM ccloud_cost_source_records WHERE source_record_id = 'provider:tier-a'"))

    with (
        pytest.raises(PreviewAllocationEvidenceDecodeError),
        backend.create_preview_generation_read_unit_of_work() as uow,
    ):
        tuple(uow.allocation_evidence.iter_preview_allocations(_scope(), ("calculation-1",)))


def test_zero_cost_origin_retains_existing_invalid_generic_lineage_without_sidecar(
    backend: SQLModelBackend,
) -> None:
    _persist_origins(
        backend,
        billing=_billing(cost="0", quantity="0"),
        sources=[
            _source("zero", amount="0", original="0", quantity="0", price="0", tier="zero"),
        ],
    )
    _replace(
        backend,
        _run(
            (),
            status=LineageCaptureStatus.INVALID,
            reason=LineageCaptureReason.ZERO_ORIGIN_COST,
        ),
    )

    engine = get_or_create_engine(backend._connection_string)
    with engine.connect() as connection:
        run = connection.execute(
            text("SELECT capture_status, capture_reason, portion_count FROM ccloud_allocation_lineage_runs")
        ).one()
        sidecar_count = connection.execute(
            text("SELECT COUNT(*) FROM ccloud_preview_source_allocation_lineage_portions")
        ).scalar_one()
    assert run == ("invalid", "zero_origin_cost", 0)
    assert sidecar_count == 0
