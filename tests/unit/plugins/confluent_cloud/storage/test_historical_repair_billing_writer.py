from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem
from plugins.confluent_cloud.storage.module import CCloudStorageModule

TRACKING_DATE = date(2026, 7, 1)
TIMESTAMP = datetime(2026, 7, 1, 12, tzinfo=UTC)


def _line(
    *,
    tenant_id: str = "tenant-1",
    timestamp: datetime = TIMESTAMP,
    env_id: str = "env-1",
    resource_id: str = "lkc-1",
    total_cost: Decimal = Decimal("2.00"),
) -> CCloudBillingLineItem:
    return CCloudBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id=tenant_id,
        timestamp=timestamp,
        env_id=env_id,
        resource_id=resource_id,
        product_category="kafka",
        product_type="kafka_num_ckus",
        quantity=Decimal("2"),
        unit_price=Decimal("1"),
        total_cost=total_cost,
        currency="USD",
        granularity="hourly",
    )


@pytest.fixture
def backend(tmp_path: Path):
    value = SQLModelBackend(
        f"sqlite:///{tmp_path / 'repair-billing.db'}",
        CCloudStorageModule(),
        use_migrations=False,
    )
    value.create_tables()
    yield value
    value.dispose()


def _rows(backend: object, tenant_id: str, tracking_date: date) -> list[CCloudBillingLineItem]:
    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        return uow.billing.find_by_date("confluent_cloud", tenant_id, tracking_date)


def test_ccloud_repository_and_production_uow_structurally_satisfy_repair_writer(
    backend: object,
) -> None:
    from core.storage.interface import HistoricalRepairBillingWriter
    from plugins.confluent_cloud.storage.repositories import CCloudBillingRepository

    assert isinstance(CCloudBillingRepository.__new__(CCloudBillingRepository), HistoricalRepairBillingWriter)
    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        assert isinstance(uow.billing, HistoricalRepairBillingWriter)


@pytest.mark.parametrize(
    "changed",
    [
        {"ecosystem": "other"},
        {"tenant_id": "tenant-2"},
        {"timestamp": datetime(2026, 7, 1, 12)},
        {"timestamp": datetime(2026, 7, 2, 0, tzinfo=UTC)},
    ],
)
def test_replacement_validates_scope_and_timestamp_before_deleting(
    backend: object,
    changed: dict[str, object],
) -> None:
    original = _line(total_cost=Decimal("9"))
    invalid = replace(_line(), **changed)
    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.billing.upsert(original)
        uow.commit()

    with pytest.raises(ValueError), backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.billing.replace_for_date(
            "confluent_cloud",
            "tenant-1",
            TRACKING_DATE,
            (invalid,),
        )

    assert _rows(backend, "tenant-1", TRACKING_DATE) == [original]


def test_replacement_rejects_duplicate_natural_keys_before_deleting(backend: object) -> None:
    original = _line(total_cost=Decimal("9"))
    duplicate = _line(total_cost=Decimal("1"))
    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.billing.upsert(original)
        uow.commit()

    with pytest.raises(ValueError, match="duplicate"), backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.billing.replace_for_date(
            "confluent_cloud",
            "tenant-1",
            TRACKING_DATE,
            (duplicate, duplicate),
        )

    assert _rows(backend, "tenant-1", TRACKING_DATE) == [original]


def test_replacement_rejects_core_line_without_ccloud_environment_before_deleting(
    backend: object,
) -> None:
    from core.models.billing import CoreBillingLineItem

    original = _line(total_cost=Decimal("9"))
    generic = CoreBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="tenant-1",
        timestamp=TIMESTAMP,
        resource_id="lkc-1",
        product_category="kafka",
        product_type="kafka_num_ckus",
        quantity=Decimal("1"),
        unit_price=Decimal("1"),
        total_cost=Decimal("1"),
    )
    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.billing.upsert(original)
        uow.commit()

    with pytest.raises(ValueError, match="env_id"), backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.billing.replace_for_date(
            "confluent_cloud",
            "tenant-1",
            TRACKING_DATE,
            (generic,),
        )

    assert _rows(backend, "tenant-1", TRACKING_DATE) == [original]


def test_authoritative_empty_replacement_deletes_only_exact_owner_date(
    backend: object,
) -> None:
    selected = _line()
    adjacent = _line(timestamp=TIMESTAMP + timedelta(days=1))
    other_tenant = _line(tenant_id="tenant-2")
    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        for item in (selected, adjacent, other_tenant):
            uow.billing.upsert(item)
        uow.commit()

    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        inserted = uow.billing.replace_for_date(
            "confluent_cloud",
            "tenant-1",
            TRACKING_DATE,
            (),
        )
        uow.commit()

    assert inserted == 0
    assert _rows(backend, "tenant-1", TRACKING_DATE) == []
    assert _rows(backend, "tenant-1", TRACKING_DATE + timedelta(days=1)) == [adjacent]
    assert _rows(backend, "tenant-2", TRACKING_DATE) == [other_tenant]


def test_nonempty_replacement_removes_stale_rows_and_inserts_exact_authoritative_set(
    backend: object,
) -> None:
    stale_one = _line(resource_id="lkc-stale-1", total_cost=Decimal("7"))
    stale_two = _line(resource_id="lkc-stale-2", total_cost=Decimal("8"))
    authoritative_one = _line(resource_id="lkc-new-1", total_cost=Decimal("1"))
    authoritative_two = _line(resource_id="lkc-new-2", total_cost=Decimal("2"))
    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.billing.upsert(stale_one)
        uow.billing.upsert(stale_two)
        uow.commit()

    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        inserted = uow.billing.replace_for_date(
            "confluent_cloud",
            "tenant-1",
            TRACKING_DATE,
            (authoritative_one, authoritative_two),
        )
        uow.commit()

    assert inserted == 2
    assert sorted(_rows(backend, "tenant-1", TRACKING_DATE), key=lambda row: row.resource_id) == [
        authoritative_one,
        authoritative_two,
    ]


def test_replacement_removes_release_era_midnight_timestamp_without_fractional_seconds(
    tmp_path: Path,
) -> None:
    connection_string = f"sqlite:///{tmp_path / 'legacy-repair-billing.db'}"
    backend = SQLModelBackend(
        connection_string,
        CCloudStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    engine = create_engine(connection_string)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO ccloud_billing (
                    ecosystem, tenant_id, timestamp, env_id, resource_id,
                    product_type, product_category, quantity, unit_price,
                    total_cost, currency, granularity, allocation_attempts,
                    topic_attribution_attempts
                ) VALUES (
                    'confluent_cloud', 'tenant-1', '2026-07-01 00:00:00',
                    'env-legacy', 'lkc-legacy', 'KAFKA_STORAGE', 'KAFKA',
                    '1', '9', '9', 'USD', 'daily', 0, 0
                )
                """
            )
        )

    authoritative = _line(timestamp=datetime(2026, 7, 1, tzinfo=UTC))
    with backend.create_unit_of_work() as uow:
        assert (
            uow.billing.replace_for_date(
                "confluent_cloud",
                "tenant-1",
                TRACKING_DATE,
                (authoritative,),
            )
            == 1
        )
        uow.commit()

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT env_id, resource_id, total_cost
                FROM ccloud_billing
                WHERE ecosystem = 'confluent_cloud' AND tenant_id = 'tenant-1'
                  AND date(timestamp) = '2026-07-01'
                """
            )
        ).all()
    assert rows == [("env-1", "lkc-1", "2.00")]
    engine.dispose()
    backend.dispose()


def test_caller_rollback_restores_prior_exact_date_state(backend: object) -> None:
    original = _line(total_cost=Decimal("9"))
    replacement = _line(resource_id="lkc-replacement", total_cost=Decimal("1"))
    with backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.billing.upsert(original)
        uow.commit()

    with pytest.raises(RuntimeError), backend.create_unit_of_work() as uow:  # type: ignore[attr-defined]
        uow.billing.replace_for_date(
            "confluent_cloud",
            "tenant-1",
            TRACKING_DATE,
            (replacement,),
        )
        raise RuntimeError("force caller rollback")

    assert _rows(backend, "tenant-1", TRACKING_DATE) == [original]
