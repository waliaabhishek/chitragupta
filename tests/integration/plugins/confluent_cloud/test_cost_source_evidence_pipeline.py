from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest
import respx
from sqlmodel import Session, col, select

from core.config.models import TenantConfig
from core.engine.orchestrator import GatherPhase
from core.plugin.registry import EcosystemBundle
from core.storage.backends.sqlmodel.engine import _engine_lock, _engines, get_or_create_engine
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud import ConfluentCloudPlugin


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


def _phase(
    *,
    days_per_query: int = 15,
    lookback_days: int = 2,
    cutoff_days: int = 1,
) -> tuple[GatherPhase, ConfluentCloudPlugin]:
    plugin = ConfluentCloudPlugin()
    plugin.initialize(
        {
            "ccloud_api": {"key": "k", "secret": "s"},
            "billing_api": {"days_per_query": days_per_query},
        }
    )
    assert plugin._connection is not None
    plugin._connection.request_interval_seconds = 0
    return (
        GatherPhase(
            ecosystem="confluent_cloud",
            tenant_id="org-1",
            tenant_config=TenantConfig(
                ecosystem="confluent_cloud",
                tenant_id="org-1",
                lookback_days=lookback_days,
                cutoff_days=cutoff_days,
            ),
            bundle=EcosystemBundle.build(plugin),
        ),
        plugin,
    )


def _backend(tmp_path: Any, plugin: ConfluentCloudPlugin) -> tuple[SQLModelBackend, str]:
    connection_string = f"sqlite:///{tmp_path / 'pipeline.db'}"
    backend = SQLModelBackend(connection_string, plugin.get_storage_module(), use_migrations=False)
    backend.create_tables()
    return backend, connection_string


def _cost(
    source_id: str | None,
    *,
    price: str = "1.25",
    quantity: str = "4",
    amount: str = "5.00",
    tier: str = "tier-1",
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "start_date": "2026-07-01",
        "end_date": "2026-07-02",
        "granularity": "DAILY",
        "product": "KAFKA",
        "line_type": "KAFKA_NUM_CKU",
        "amount": amount,
        "original_amount": amount,
        "discount_amount": "0",
        "price": price,
        "quantity": quantity,
        "unit": "CKU_HOUR",
        "description": "Kafka capacity",
        "network_access_type": "PUBLIC",
        "resource": {
            "id": "lkc-1",
            "display_name": "cluster-1",
            "environment": {"id": "env-1"},
        },
        "tier_dimensions": {"tier": tier},
    }
    if source_id is not None:
        row["id"] = source_id
    return row


def _response(rows: list[dict[str, Any]]) -> httpx.Response:
    return httpx.Response(200, json={"data": rows, "metadata": {}})


def _source_rows(connection_string: str) -> list[Any]:
    from plugins.confluent_cloud.storage.tables import CCloudCostSourceTable

    with Session(get_or_create_engine(connection_string)) as session:
        return list(
            session.exec(select(CCloudCostSourceTable).order_by(col(CCloudCostSourceTable.source_record_id))).all()
        )


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


class TestProductionCostSourceEvidencePipeline:
    @respx.mock
    def test_gather_phase_persists_two_native_tiers_and_one_unchanged_aggregate(self, tmp_path: Any) -> None:
        respx.get("https://api.confluent.cloud/billing/v1/costs").mock(
            return_value=_response(
                [
                    _cost("cost-tier-1", price="1.25", quantity="4", amount="5.00", tier="tier-1"),
                    _cost("cost-tier-2", price="2.00", quantity="3", amount="6.00", tier="tier-2"),
                ]
            )
        )
        phase, plugin = _phase()
        backend, connection_string = _backend(tmp_path, plugin)

        from plugins.confluent_cloud.models.billing import CCloudSourceWindowWriter

        with backend.create_unit_of_work() as uow:
            assert isinstance(uow.billing, CCloudSourceWindowWriter)
            gathered_dates = phase._gather_billing(uow, datetime(2026, 7, 3, 12, tzinfo=UTC))
            uow.commit()

        source_rows = _source_rows(connection_string)
        assert gathered_dates == {datetime(2026, 7, 1, tzinfo=UTC).date()}
        assert [row.source_record_id for row in source_rows] == [
            "provider:cost-tier-1",
            "provider:cost-tier-2",
        ]
        assert [row.price for row in source_rows] == ["1.25", "2.00"]
        assert [row.tier_dimensions_json for row in source_rows] == [
            '{"tier":"tier-1"}',
            '{"tier":"tier-2"}',
        ]
        with backend.create_unit_of_work() as uow:
            aggregates = uow.billing.find_by_date("confluent_cloud", "org-1", datetime(2026, 7, 1).date())
        assert len(aggregates) == 1
        assert aggregates[0].quantity == Decimal("7")
        assert aggregates[0].unit_price == Decimal("0")
        assert aggregates[0].total_cost == Decimal("11.00")
        assert aggregates[0].metadata["tiers"] == [
            {"price": "1.25", "quantity": "4", "cost": "5.00"},
            {"price": "2.00", "quantity": "3", "cost": "6.00"},
        ]
        backend.dispose()
        plugin.close()

    @respx.mock
    def test_non_midnight_owner_bounds_use_identical_http_collection_and_fallback_dates(self, tmp_path: Any) -> None:
        route = respx.get("https://api.confluent.cloud/billing/v1/costs")
        route.side_effect = [_response([_cost(None)]), _response([_cost(None)])]
        phase, plugin = _phase()
        backend, connection_string = _backend(tmp_path, plugin)

        with backend.create_unit_of_work() as uow:
            phase._gather_billing(uow, datetime(2026, 7, 3, 12, tzinfo=UTC))
            uow.commit()
        first = _source_rows(connection_string)[0]

        with backend.create_unit_of_work() as uow:
            phase._gather_billing(uow, datetime(2026, 7, 3, 22, tzinfo=UTC))
            uow.commit()
        second = _source_rows(connection_string)[0]

        assert [call.request.url.params["start_date"] for call in respx.calls] == [
            "2026-07-01",
            "2026-07-01",
        ]
        assert [call.request.url.params["end_date"] for call in respx.calls] == [
            "2026-07-02",
            "2026-07-02",
        ]
        assert (_aware(second.collection_window_start), _aware(second.collection_window_end)) == (
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 2, tzinfo=UTC),
        )
        assert first.source_record_id == second.source_record_id
        assert second.source_record_id.startswith("composite:v1:")
        backend.dispose()
        plugin.close()

    @respx.mock
    def test_failure_after_source_replacement_before_commit_restores_prior_source_set(self, tmp_path: Any) -> None:
        route = respx.get("https://api.confluent.cloud/billing/v1/costs")
        route.side_effect = [_response([_cost("prior")]), _response([_cost("replacement")])]
        phase, plugin = _phase()
        backend, connection_string = _backend(tmp_path, plugin)

        with backend.create_unit_of_work() as uow:
            phase._gather_billing(uow, datetime(2026, 7, 3, 12, tzinfo=UTC))
            uow.commit()

        with pytest.raises(RuntimeError, match="after replacement"), backend.create_unit_of_work() as uow:
            phase._gather_billing(uow, datetime(2026, 7, 3, 12, tzinfo=UTC))
            raise RuntimeError("after replacement")

        assert [row.source_record_id for row in _source_rows(connection_string)] == ["provider:prior"]
        backend.dispose()
        plugin.close()

    @respx.mock
    def test_successful_empty_response_clears_refreshed_source_evidence(self, tmp_path: Any) -> None:
        route = respx.get("https://api.confluent.cloud/billing/v1/costs")
        route.side_effect = [_response([_cost("prior")]), _response([])]
        phase, plugin = _phase()
        backend, connection_string = _backend(tmp_path, plugin)

        with backend.create_unit_of_work() as uow:
            phase._gather_billing(uow, datetime(2026, 7, 3, 12, tzinfo=UTC))
            uow.commit()
        assert [row.source_record_id for row in _source_rows(connection_string)] == ["provider:prior"]

        with backend.create_unit_of_work() as uow:
            gathered_dates = phase._gather_billing(uow, datetime(2026, 7, 3, 12, tzinfo=UTC))
            uow.commit()

        assert gathered_dates == set()
        assert _source_rows(connection_string) == []
        backend.dispose()
        plugin.close()

    @respx.mock
    def test_two_run_rolling_gather_preserves_outside_and_replaces_refreshed_evidence(self, tmp_path: Any) -> None:
        outside = _cost("outside")
        stale = _cost("stable", amount="1.00")
        stale.update(start_date="2026-07-02", end_date="2026-07-03", original_amount="1.00")
        malformed = _cost(None)
        malformed.pop("start_date")
        replacement = _cost("stable", amount="2.00")
        replacement.update(start_date="2026-07-02", end_date="2026-07-03", original_amount="2.00")
        corrected = _cost("corrected", amount="3.00")
        corrected.update(start_date="2026-07-03", end_date="2026-07-04", original_amount="3.00")
        route = respx.get("https://api.confluent.cloud/billing/v1/costs")
        route.side_effect = [
            _response([outside, stale, malformed]),
            _response([replacement, corrected]),
        ]
        phase, plugin = _phase(lookback_days=16)
        backend, connection_string = _backend(tmp_path, plugin)

        with backend.create_unit_of_work() as uow:
            phase._gather_billing(uow, datetime(2026, 7, 17, 12, tzinfo=UTC))
            uow.commit()
        with backend.create_unit_of_work() as uow:
            phase._gather_billing(uow, datetime(2026, 7, 18, 12, tzinfo=UTC))
            uow.commit()

        rows = _source_rows(connection_string)
        by_id = {row.source_record_id: row for row in rows}
        assert set(by_id) >= {"provider:outside", "provider:stable", "provider:corrected"}
        assert by_id["provider:stable"].amount == "2.00"
        assert by_id["provider:corrected"].amount == "3.00"
        residuals = [row for row in rows if row.source_record_id.startswith("composite:v1:")]
        assert len(residuals) == 1
        assert (_aware(residuals[0].evidence_scope_start), _aware(residuals[0].evidence_scope_end)) == (
            datetime(2026, 7, 1, tzinfo=UTC),
            datetime(2026, 7, 2, tzinfo=UTC),
        )
        assert all(
            not (
                row.source_record_id.startswith("composite:v1:")
                and _aware(row.evidence_scope_start) >= datetime(2026, 7, 2, tzinfo=UTC)
            )
            for row in rows
        )
        backend.dispose()
        plugin.close()

    @respx.mock
    def test_changed_transport_partition_preserves_logical_rolling_replacement(self, tmp_path: Any) -> None:
        outside = _cost("outside")
        malformed = _cost(None)
        malformed.pop("start_date")
        stale = _cost("stable", amount="1.00")
        stale.update(start_date="2026-07-06", end_date="2026-07-07", original_amount="1.00")
        replacement = _cost("stable", amount="8.00")
        replacement.update(start_date="2026-07-06", end_date="2026-07-07", original_amount="8.00")
        corrected = _cost("corrected", amount="3.00")
        corrected.update(start_date="2026-07-03", end_date="2026-07-04", original_amount="3.00")
        route = respx.get("https://api.confluent.cloud/billing/v1/costs")
        route.side_effect = [
            _response([outside, malformed]),
            _response([stale]),
            _response([]),
            _response([replacement, corrected]),
            _response([]),
            _response([]),
        ]
        first_phase, first_plugin = _phase(days_per_query=5, lookback_days=16)
        backend, connection_string = _backend(tmp_path, first_plugin)

        with backend.create_unit_of_work() as uow:
            first_phase._gather_billing(uow, datetime(2026, 7, 17, 12, tzinfo=UTC))
            uow.commit()

        second_phase, second_plugin = _phase(days_per_query=7, lookback_days=16)
        with backend.create_unit_of_work() as uow:
            second_phase._gather_billing(uow, datetime(2026, 7, 18, 12, tzinfo=UTC))
            uow.commit()

        assert [
            (call.request.url.params["start_date"], call.request.url.params["end_date"]) for call in respx.calls
        ] == [
            ("2026-07-01", "2026-07-06"),
            ("2026-07-06", "2026-07-11"),
            ("2026-07-11", "2026-07-16"),
            ("2026-07-02", "2026-07-09"),
            ("2026-07-09", "2026-07-16"),
            ("2026-07-16", "2026-07-17"),
        ]
        rows = _source_rows(connection_string)
        by_id = {row.source_record_id: row for row in rows}
        assert by_id["provider:stable"].amount == "8.00"
        assert (
            _aware(by_id["provider:stable"].collection_window_start),
            _aware(by_id["provider:stable"].collection_window_end),
        ) == (
            datetime(2026, 7, 2, tzinfo=UTC),
            datetime(2026, 7, 9, tzinfo=UTC),
        )
        assert "provider:corrected" in by_id
        residuals = [row for row in rows if row.source_record_id.startswith("composite:v1:")]
        assert len(residuals) == 1
        assert {
            (
                row.source_record_id,
                _aware(row.evidence_scope_start),
                _aware(row.evidence_scope_end),
            )
            for row in rows
        } == {
            (
                "provider:outside",
                datetime(2026, 7, 1, tzinfo=UTC),
                datetime(2026, 7, 2, tzinfo=UTC),
            ),
            (
                "provider:stable",
                datetime(2026, 7, 6, tzinfo=UTC),
                datetime(2026, 7, 7, tzinfo=UTC),
            ),
            (
                "provider:corrected",
                datetime(2026, 7, 3, tzinfo=UTC),
                datetime(2026, 7, 4, tzinfo=UTC),
            ),
            (
                residuals[0].source_record_id,
                datetime(2026, 7, 1, tzinfo=UTC),
                datetime(2026, 7, 2, tzinfo=UTC),
            ),
        }
        backend.dispose()
        first_plugin.close()
        second_plugin.close()
