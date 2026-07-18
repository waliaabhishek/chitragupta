from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlmodel import Session, col, select

from core.storage.backends.sqlmodel.engine import _engine_lock, _engines, get_or_create_engine
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule


@pytest.fixture(autouse=True)
def clean_engine_cache() -> Any:
    with _engine_lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()
    yield
    with _engine_lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()


@pytest.fixture
def source_backend(tmp_path: Any) -> tuple[SQLModelBackend, str]:
    connection_string = f"sqlite:///{tmp_path / 'source-evidence.db'}"
    backend = SQLModelBackend(connection_string, CCloudStorageModule(), use_migrations=False)
    backend.create_tables()
    yield backend, connection_string
    backend.dispose()


def _dt(day: int, *, hour: int = 0, microsecond: int = 0) -> datetime:
    return datetime(2026, 7, day, hour, microsecond=microsecond, tzinfo=UTC)


def _record(**overrides: Any) -> Any:
    from plugins.confluent_cloud.models.billing import CCloudCostSourceRecord

    values: dict[str, Any] = {
        "ecosystem": "confluent_cloud",
        "tenant_id": "org-1",
        "source_record_id": "provider:cost-1",
        "identity_scheme": "provider_cost_id",
        "provider_cost_id": "cost-1",
        "source_period_start": _dt(2),
        "source_period_end": _dt(3),
        "collection_window_start": _dt(1),
        "collection_window_end": _dt(4),
        "evidence_scope_start": _dt(2),
        "evidence_scope_end": _dt(3),
        "allocation_timestamp": _dt(2),
        "retention_timestamp": _dt(2),
        "granularity": "DAILY",
        "product": "KAFKA",
        "line_type": "KAFKA_NUM_CKU",
        "amount": Decimal("12.3400"),
        "original_amount": Decimal("15.0000"),
        "discount_amount": Decimal("2.6600"),
        "price": Decimal("1.2340"),
        "quantity": Decimal("10.0000"),
        "unit": "CKU_HOUR",
        "description": "Kafka capacity",
        "network_access_type": "PRIVATE",
        "resource_id": "lkc-1",
        "resource_name": "cluster-1",
        "environment_id": "env-1",
        "tier_dimensions": {"tier_end": "100", "tier_start": "0"},
        "malformed": False,
        "diagnostics": (),
        "raw_payload": {"z": 1, "a": {"b": 2}},
    }
    values.update(overrides)
    return CCloudCostSourceRecord(**values)


def _undated(**overrides: Any) -> Any:
    values: dict[str, Any] = {
        "source_record_id": "composite:v1:undated",
        "identity_scheme": "composite_v1",
        "provider_cost_id": None,
        "source_period_start": None,
        "source_period_end": None,
        "collection_window_start": _dt(1),
        "collection_window_end": _dt(5),
        "evidence_scope_start": _dt(1),
        "evidence_scope_end": _dt(5),
        "allocation_timestamp": datetime(1970, 1, 1, tzinfo=UTC),
        "retention_timestamp": _dt(5),
        "malformed": True,
        "diagnostics": ("missing_required:start_date",),
    }
    values.update(overrides)
    return _record(**values)


def _replace(
    backend: SQLModelBackend,
    start: datetime,
    end: datetime,
    records: list[Any],
    *,
    ecosystem: str = "confluent_cloud",
    tenant_id: str = "org-1",
) -> None:
    with backend.create_unit_of_work() as uow:
        repository = uow.billing
        repository.replace_source_window(ecosystem, tenant_id, start, end, records)  # type: ignore[attr-defined]
        uow.commit()


def _rows(connection_string: str, *, tenant_id: str = "org-1") -> list[Any]:
    from plugins.confluent_cloud.storage.tables import CCloudCostSourceTable

    engine = get_or_create_engine(connection_string)
    with Session(engine) as session:
        statement = (
            select(CCloudCostSourceTable)
            .where(col(CCloudCostSourceTable.tenant_id) == tenant_id)
            .order_by(
                col(CCloudCostSourceTable.source_record_id),
                col(CCloudCostSourceTable.evidence_scope_start),
            )
        )
        return list(session.exec(statement).all())


def _aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


class TestSourcePersistence:
    def test_real_sqlite_round_trip_preserves_every_column_exactly(
        self, source_backend: tuple[SQLModelBackend, str]
    ) -> None:
        backend, connection_string = source_backend
        source = _record()

        _replace(backend, _dt(1), _dt(4), [source])

        rows = _rows(connection_string)
        assert len(rows) == 1
        row = rows[0]
        assert row.ecosystem == source.ecosystem
        assert row.tenant_id == source.tenant_id
        assert row.source_record_id == source.source_record_id
        assert row.identity_scheme == source.identity_scheme
        assert row.provider_cost_id == source.provider_cost_id
        assert _aware(row.source_period_start) == source.source_period_start
        assert _aware(row.source_period_end) == source.source_period_end
        assert _aware(row.collection_window_start) == source.collection_window_start
        assert _aware(row.collection_window_end) == source.collection_window_end
        assert _aware(row.evidence_scope_start) == source.evidence_scope_start
        assert _aware(row.evidence_scope_end) == source.evidence_scope_end
        assert _aware(row.allocation_timestamp) == source.allocation_timestamp
        assert _aware(row.retention_timestamp) == source.retention_timestamp
        assert row.granularity == source.granularity
        assert row.product == source.product
        assert row.line_type == source.line_type
        assert row.amount == "12.3400"
        assert row.original_amount == "15.0000"
        assert row.discount_amount == "2.6600"
        assert row.price == "1.2340"
        assert row.quantity == "10.0000"
        assert row.unit == source.unit
        assert row.description == source.description
        assert row.network_access_type == source.network_access_type
        assert row.resource_id == source.resource_id
        assert row.resource_name == source.resource_name
        assert row.environment_id == source.environment_id
        assert row.tier_dimensions_json == '{"tier_end":"100","tier_start":"0"}'
        assert row.diagnostics_json == "[]"
        assert row.raw_payload_json == '{"a":{"b":2},"z":1}'
        assert json.loads(row.raw_payload_json) == source.raw_payload
        assert row.malformed is False

    def test_stable_provider_identity_is_replaced_across_runs(
        self, source_backend: tuple[SQLModelBackend, str]
    ) -> None:
        backend, connection_string = source_backend
        _replace(backend, _dt(1), _dt(4), [_record(amount=Decimal("1.00"))])

        _replace(backend, _dt(1), _dt(4), [_record(amount=Decimal("2.00"))])

        rows = _rows(connection_string)
        assert len(rows) == 1
        assert rows[0].source_record_id == "provider:cost-1"
        assert rows[0].amount == "2.00"

    def test_successful_empty_replacement_clears_only_valid_rows_in_window(
        self, source_backend: tuple[SQLModelBackend, str]
    ) -> None:
        backend, connection_string = source_backend
        outside = _record(
            source_record_id="provider:outside",
            provider_cost_id="outside",
            source_period_start=_dt(1),
            source_period_end=_dt(2),
            evidence_scope_start=_dt(1),
            evidence_scope_end=_dt(2),
            allocation_timestamp=_dt(1),
            retention_timestamp=_dt(1),
        )
        inside = _record()
        _replace(backend, _dt(1), _dt(4), [outside, inside])

        _replace(backend, _dt(2), _dt(4), [])

        rows = _rows(connection_string)
        assert [(row.source_record_id, _aware(row.allocation_timestamp)) for row in rows] == [
            ("provider:outside", _dt(1))
        ]

    @pytest.mark.parametrize(
        ("refresh_start", "refresh_end", "expected_scopes"),
        [
            (_dt(1), _dt(5), []),
            (_dt(1), _dt(3), [(_dt(3), _dt(5))]),
            (_dt(3), _dt(5), [(_dt(1), _dt(3))]),
            (_dt(2), _dt(4), [(_dt(1), _dt(2)), (_dt(4), _dt(5))]),
        ],
    )
    def test_undated_overlap_is_deleted_trimmed_or_split(
        self,
        source_backend: tuple[SQLModelBackend, str],
        refresh_start: datetime,
        refresh_end: datetime,
        expected_scopes: list[tuple[datetime, datetime]],
    ) -> None:
        backend, connection_string = source_backend
        _replace(backend, _dt(1), _dt(5), [_undated()])

        _replace(backend, refresh_start, refresh_end, [])

        rows = _rows(connection_string)
        assert [(_aware(row.evidence_scope_start), _aware(row.evidence_scope_end)) for row in rows] == expected_scopes
        assert [_aware(row.retention_timestamp) for row in rows] == [end for _, end in expected_scopes]
        assert all(_aware(row.collection_window_start) == _dt(1) for row in rows)
        assert all(_aware(row.collection_window_end) == _dt(5) for row in rows)
        assert all(json.loads(row.raw_payload_json) == {"a": {"b": 2}, "z": 1} for row in rows)

    def test_corrected_identity_removes_old_malformed_overlap(
        self, source_backend: tuple[SQLModelBackend, str]
    ) -> None:
        backend, connection_string = source_backend
        _replace(
            backend,
            _dt(1),
            _dt(4),
            [_undated(collection_window_end=_dt(4), evidence_scope_end=_dt(4), retention_timestamp=_dt(4))],
        )
        corrected = _record(source_record_id="provider:corrected", provider_cost_id="corrected")

        _replace(backend, _dt(1), _dt(4), [corrected])

        rows = _rows(connection_string)
        assert [row.source_record_id for row in rows] == ["provider:corrected"]

    def test_replacement_is_tenant_isolated(self, source_backend: tuple[SQLModelBackend, str]) -> None:
        backend, connection_string = source_backend
        other = _record(tenant_id="org-2")
        _replace(backend, _dt(1), _dt(4), [other], tenant_id="org-2")

        _replace(backend, _dt(1), _dt(4), [])

        assert _rows(connection_string) == []
        other_rows = _rows(connection_string, tenant_id="org-2")
        assert len(other_rows) == 1
        assert other_rows[0].tenant_id == "org-2"

    def test_validation_happens_before_deletion(self, source_backend: tuple[SQLModelBackend, str]) -> None:
        backend, connection_string = source_backend
        existing = _record()
        _replace(backend, _dt(1), _dt(4), [existing])
        invalid = dataclasses.replace(existing, tenant_id="wrong-tenant")

        with backend.create_unit_of_work() as uow, pytest.raises(ValueError):
            uow.billing.replace_source_window(  # type: ignore[attr-defined]
                "confluent_cloud", "org-1", _dt(1), _dt(4), [invalid]
            )

        rows = _rows(connection_string)
        assert len(rows) == 1
        assert rows[0].source_record_id == existing.source_record_id

    def test_uncommitted_replacement_rolls_back(self, source_backend: tuple[SQLModelBackend, str]) -> None:
        backend, connection_string = source_backend
        _replace(backend, _dt(1), _dt(4), [_record(amount=Decimal("1"))])

        with backend.create_unit_of_work() as uow:
            uow.billing.replace_source_window(  # type: ignore[attr-defined]
                "confluent_cloud", "org-1", _dt(1), _dt(4), [_record(amount=Decimal("2"))]
            )

        rows = _rows(connection_string)
        assert len(rows) == 1
        assert rows[0].amount == "1"


class TestSourceReplacementValidation:
    @pytest.mark.parametrize(
        ("start", "end"),
        [
            (datetime(2026, 7, 1), _dt(2)),
            (_dt(1, hour=1), _dt(2)),
            (_dt(2), _dt(2)),
            (_dt(3), _dt(2)),
        ],
    )
    def test_invalid_refresh_bounds_raise_without_writes(
        self,
        source_backend: tuple[SQLModelBackend, str],
        start: datetime,
        end: datetime,
    ) -> None:
        backend, connection_string = source_backend

        with backend.create_unit_of_work() as uow, pytest.raises(ValueError):
            uow.billing.replace_source_window(  # type: ignore[attr-defined]
                "confluent_cloud", "org-1", start, end, []
            )

        assert _rows(connection_string) == []

    @pytest.mark.parametrize(
        "case",
        [
            "ecosystem",
            "empty_scope",
            "retention",
            "collection_outside",
            "allocation_outside",
        ],
    )
    def test_invalid_records_raise_before_insert(self, source_backend: tuple[SQLModelBackend, str], case: str) -> None:
        backend, connection_string = source_backend
        source = _record()
        invalid = {
            "ecosystem": dataclasses.replace(source, ecosystem="other"),
            "empty_scope": dataclasses.replace(source, evidence_scope_end=_dt(2)),
            "retention": dataclasses.replace(source, retention_timestamp=_dt(3)),
            "collection_outside": dataclasses.replace(source, collection_window_end=_dt(5)),
            "allocation_outside": dataclasses.replace(
                source,
                source_period_start=_dt(4),
                allocation_timestamp=_dt(4),
                retention_timestamp=_dt(4),
            ),
        }[case]

        with backend.create_unit_of_work() as uow, pytest.raises(ValueError):
            uow.billing.replace_source_window(  # type: ignore[attr-defined]
                "confluent_cloud", "org-1", _dt(1), _dt(4), [invalid]
            )

        assert _rows(connection_string) == []


class TestSourceRollingRefresh:
    def test_rolling_empty_refresh_preserves_only_unrefreshed_coverage(
        self, source_backend: tuple[SQLModelBackend, str]
    ) -> None:
        backend, connection_string = source_backend
        july_1 = _record(
            source_record_id="provider:july-1",
            provider_cost_id="july-1",
            source_period_start=_dt(1),
            source_period_end=_dt(2),
            collection_window_start=_dt(1),
            collection_window_end=_dt(16),
            evidence_scope_start=_dt(1),
            evidence_scope_end=_dt(2),
            allocation_timestamp=_dt(1),
            retention_timestamp=_dt(1),
        )
        july_2 = _record(
            source_record_id="provider:july-2",
            provider_cost_id="july-2",
            source_period_start=_dt(2),
            source_period_end=_dt(3),
            collection_window_start=_dt(1),
            collection_window_end=_dt(16),
            evidence_scope_start=_dt(2),
            evidence_scope_end=_dt(3),
            allocation_timestamp=_dt(2),
            retention_timestamp=_dt(2),
        )
        undated = _undated(
            collection_window_start=_dt(1),
            collection_window_end=_dt(16),
            evidence_scope_start=_dt(1),
            evidence_scope_end=_dt(16),
            retention_timestamp=_dt(16),
        )
        _replace(backend, _dt(1), _dt(16), [july_1, july_2, undated])

        _replace(backend, _dt(2), _dt(17), [])

        rows = _rows(connection_string)
        assert [
            (row.source_record_id, _aware(row.evidence_scope_start), _aware(row.evidence_scope_end)) for row in rows
        ] == [
            ("composite:v1:undated", _dt(1), _dt(2)),
            ("provider:july-1", _dt(1), _dt(2)),
        ]

    def test_transport_repartition_changes_provenance_not_logical_source_set(
        self, source_backend: tuple[SQLModelBackend, str]
    ) -> None:
        backend, connection_string = source_backend
        first = _record(
            source_record_id="provider:first",
            provider_cost_id="first",
            source_period_start=_dt(1),
            source_period_end=_dt(2),
            collection_window_start=_dt(1),
            collection_window_end=_dt(3),
            evidence_scope_start=_dt(1),
            evidence_scope_end=_dt(2),
            allocation_timestamp=_dt(1),
            retention_timestamp=_dt(1),
        )
        second = _record(
            source_record_id="provider:second",
            provider_cost_id="second",
            source_period_start=_dt(3),
            source_period_end=_dt(4),
            collection_window_start=_dt(3),
            collection_window_end=_dt(5),
            evidence_scope_start=_dt(3),
            evidence_scope_end=_dt(4),
            allocation_timestamp=_dt(3),
            retention_timestamp=_dt(3),
        )
        _replace(backend, _dt(1), _dt(5), [first, second])
        before = [
            (row.source_record_id, _aware(row.evidence_scope_start), _aware(row.evidence_scope_end))
            for row in _rows(connection_string)
        ]

        _replace(
            backend,
            _dt(1),
            _dt(5),
            [
                dataclasses.replace(first, collection_window_end=_dt(5)),
                dataclasses.replace(second, collection_window_start=_dt(1)),
            ],
        )

        rows = _rows(connection_string)
        assert [
            (row.source_record_id, _aware(row.evidence_scope_start), _aware(row.evidence_scope_end)) for row in rows
        ] == before
        assert {(_aware(row.collection_window_start), _aware(row.collection_window_end)) for row in rows} == {
            (_dt(1), _dt(5))
        }


class TestSourceRetention:
    def test_usable_source_and_aggregate_use_strict_allocation_cutoff(
        self, source_backend: tuple[SQLModelBackend, str]
    ) -> None:
        from plugins.confluent_cloud.models.billing import CCloudBillingLineItem

        backend, connection_string = source_backend
        before = _record(source_period_end=_dt(4), evidence_scope_end=_dt(4))
        equal = _record(
            source_record_id="provider:equal",
            provider_cost_id="equal",
            source_period_start=_dt(3),
            source_period_end=_dt(4),
            evidence_scope_start=_dt(3),
            evidence_scope_end=_dt(4),
            allocation_timestamp=_dt(3),
            retention_timestamp=_dt(3),
        )
        _replace(backend, _dt(1), _dt(4), [before, equal])
        with backend.create_unit_of_work() as uow:
            for timestamp, resource_id in [(_dt(2), "before"), (_dt(3), "equal")]:
                uow.billing.upsert(
                    CCloudBillingLineItem(
                        ecosystem="confluent_cloud",
                        tenant_id="org-1",
                        timestamp=timestamp,
                        env_id="env-1",
                        resource_id=resource_id,
                        product_category="KAFKA",
                        product_type="KAFKA_NUM_CKU",
                        quantity=Decimal("1"),
                        unit_price=Decimal("1"),
                        total_cost=Decimal("1"),
                    )
                )
            deleted = uow.billing.delete_before("confluent_cloud", "org-1", _dt(3))
            uow.commit()

        assert deleted == 1
        assert [row.source_record_id for row in _rows(connection_string)] == ["provider:equal"]
        with backend.create_unit_of_work() as uow:
            aggregate_rows = uow.billing.find_by_range("confluent_cloud", "org-1", _dt(1), _dt(4))
        assert [(row.resource_id, row.timestamp) for row in aggregate_rows] == [("equal", _dt(3))]

    def test_undated_evidence_expires_only_after_scope_end(self, source_backend: tuple[SQLModelBackend, str]) -> None:
        backend, connection_string = source_backend
        source = _undated(
            collection_window_end=_dt(3),
            evidence_scope_end=_dt(3),
            retention_timestamp=_dt(3),
        )
        _replace(backend, _dt(1), _dt(3), [source])

        for cutoff in [datetime(2026, 6, 30, tzinfo=UTC), _dt(1), _dt(2, hour=12), _dt(3)]:
            with backend.create_unit_of_work() as uow:
                assert uow.billing.delete_before("confluent_cloud", "org-1", cutoff) == 0
                uow.commit()
            rows = _rows(connection_string)
            assert len(rows) == 1
            assert (_aware(rows[0].evidence_scope_start), _aware(rows[0].evidence_scope_end)) == (_dt(1), _dt(3))
            assert json.loads(rows[0].raw_payload_json) == {"a": {"b": 2}, "z": 1}

        with backend.create_unit_of_work() as uow:
            assert uow.billing.delete_before("confluent_cloud", "org-1", _dt(3, microsecond=1)) == 0
            uow.commit()
        assert _rows(connection_string) == []

    def test_split_residuals_expire_independently(self, source_backend: tuple[SQLModelBackend, str]) -> None:
        backend, connection_string = source_backend
        _replace(backend, _dt(1), _dt(5), [_undated()])
        _replace(backend, _dt(2), _dt(4), [])

        with backend.create_unit_of_work() as uow:
            uow.billing.delete_before("confluent_cloud", "org-1", _dt(3))
            uow.commit()

        rows = _rows(connection_string)
        assert [(_aware(row.evidence_scope_start), _aware(row.evidence_scope_end)) for row in rows] == [
            (_dt(4), _dt(5))
        ]
