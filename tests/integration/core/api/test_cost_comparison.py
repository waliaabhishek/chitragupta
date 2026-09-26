from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple
from unittest.mock import patch
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from core.api.app import create_app
from core.config.models import AppSettings, PluginSettingsBase, StorageConfig, TenantConfig
from core.models.chargeback import ChargebackRow, CostType
from core.models.pipeline import PipelineState
from core.models.topic_attribution import TopicAttributionRow
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider, install_backend

if TYPE_CHECKING:
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend


_ECO = "test-eco"
_TENANT_ID = "test-tenant"
_TENANT_NAME = "test-tenant"
_BASELINE_DAY = date(2026, 1, 10)
_COMPARISON_DAY = date(2026, 1, 11)


class _DstWeekCase(NamedTuple):
    baseline_start: date
    baseline_end: date
    comparison_start: date
    comparison_end: date
    baseline_start_at: str
    baseline_end_at: str
    comparison_start_at: str
    comparison_end_at: str
    baseline_duration_seconds: int
    comparison_duration_seconds: int
    baseline_expected_dates: tuple[str, ...]
    comparison_expected_dates: tuple[str, ...]


_DST_WEEK_CASES = (
    _DstWeekCase(
        baseline_start=date(2026, 2, 23),
        baseline_end=date(2026, 3, 1),
        comparison_start=date(2026, 3, 2),
        comparison_end=date(2026, 3, 8),
        baseline_start_at="2026-02-23T08:00:00Z",
        baseline_end_at="2026-03-02T08:00:00Z",
        comparison_start_at="2026-03-02T08:00:00Z",
        comparison_end_at="2026-03-09T07:00:00Z",
        baseline_duration_seconds=604800,
        comparison_duration_seconds=601200,
        baseline_expected_dates=(
            "2026-02-24",
            "2026-02-25",
            "2026-02-26",
            "2026-02-27",
            "2026-02-28",
            "2026-03-01",
            "2026-03-02",
        ),
        comparison_expected_dates=(
            "2026-03-03",
            "2026-03-04",
            "2026-03-05",
            "2026-03-06",
            "2026-03-07",
            "2026-03-08",
            "2026-03-09",
        ),
    ),
    _DstWeekCase(
        baseline_start=date(2026, 10, 19),
        baseline_end=date(2026, 10, 25),
        comparison_start=date(2026, 10, 26),
        comparison_end=date(2026, 11, 1),
        baseline_start_at="2026-10-19T07:00:00Z",
        baseline_end_at="2026-10-26T07:00:00Z",
        comparison_start_at="2026-10-26T07:00:00Z",
        comparison_end_at="2026-11-02T08:00:00Z",
        baseline_duration_seconds=604800,
        comparison_duration_seconds=608400,
        baseline_expected_dates=(
            "2026-10-20",
            "2026-10-21",
            "2026-10-22",
            "2026-10-23",
            "2026-10-24",
            "2026-10-25",
            "2026-10-26",
        ),
        comparison_expected_dates=(
            "2026-10-27",
            "2026-10-28",
            "2026-10-29",
            "2026-10-30",
            "2026-10-31",
            "2026-11-01",
            "2026-11-02",
        ),
    ),
)


def _comparison_params(**overrides: str | int) -> dict[str, str | int]:
    params: dict[str, str | int] = {
        "baseline_start": _BASELINE_DAY.isoformat(),
        "baseline_end": _BASELINE_DAY.isoformat(),
        "comparison_start": _COMPARISON_DAY.isoformat(),
        "comparison_end": _COMPARISON_DAY.isoformat(),
        "timezone": "UTC",
    }
    params.update(overrides)
    return params


def _chargeback_url() -> str:
    return f"/api/v1/tenants/{_TENANT_NAME}/chargebacks/comparison"


def _topic_attribution_url() -> str:
    return f"/api/v1/tenants/{_TENANT_NAME}/topic-attributions/comparison"


def _state(day: date, *, topic_attribution: bool = False) -> PipelineState:
    return PipelineState(
        ecosystem=_ECO,
        tenant_id=_TENANT_ID,
        tracking_date=day,
        chargeback_calculated=True,
        calculation_id=f"calculation-{day.isoformat()}",
        calculation_completed_at=datetime(day.year, day.month, day.day, 12, tzinfo=UTC),
        topic_overlay_gathered=topic_attribution,
        topic_attribution_calculated=topic_attribution,
    )


def _chargeback(
    day: date,
    identity_id: str,
    amount: str,
    *,
    tenant_id: str = _TENANT_ID,
    ecosystem: str = _ECO,
    resource_id: str | None = "resource-1",
    product_type: str = "KAFKA_BASE",
    cost_type: CostType = CostType.USAGE,
    timestamp: datetime | None = None,
) -> ChargebackRow:
    return ChargebackRow(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=timestamp or datetime(day.year, day.month, day.day, tzinfo=UTC),
        resource_id=resource_id,
        product_category="kafka",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=cost_type,
        amount=Decimal(amount),
        metadata={"env_id": "env-1"},
    )


def _topic(
    day: date,
    cluster: str,
    topic_name: str,
    amount: str,
    *,
    tenant_id: str = _TENANT_ID,
    ecosystem: str = _ECO,
    product_type: str = "KAFKA_BASE",
    attribution_method: str = "bytes_ratio",
    timestamp: datetime | None = None,
) -> TopicAttributionRow:
    return TopicAttributionRow(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=timestamp or datetime(day.year, day.month, day.day, tzinfo=UTC),
        env_id="env-1",
        cluster_resource_id=cluster,
        topic_name=topic_name,
        product_category="kafka",
        product_type=product_type,
        attribution_method=attribution_method,
        amount=Decimal(amount),
    )


def _set_plugin_settings(
    client: TestClient,
    *,
    chargeback_granularity: str = "daily",
    topic_attribution: object | None = None,
) -> None:
    values: dict[str, object] = {"chargeback_granularity": chargeback_granularity}
    if topic_attribution is not None:
        values["topic_attribution"] = topic_attribution
    client.app.state.settings.tenants[_TENANT_NAME].plugin_settings = PluginSettingsBase.model_validate(values)


def _add_tag(
    backend: SQLModelBackend,
    *,
    entity_type: str,
    entity_id: str,
    tag_key: str,
    tag_value: str,
    tenant_id: str = _TENANT_ID,
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.tags.add_tag(tenant_id, entity_type, entity_id, tag_key, tag_value, "test")
        uow.commit()


def _topic_resource_id(cluster: str, topic_name: str) -> str:
    return f"{cluster}:topic:{topic_name}"


def _seed_chargeback_comparison(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert_batch(
            [
                _chargeback(
                    _BASELINE_DAY,
                    "alice",
                    "0.1",
                    timestamp=datetime(2026, 1, 10, 0, 0, 1, tzinfo=UTC),
                ),
                _chargeback(_BASELINE_DAY, "alice", "0.2"),
                _chargeback(_COMPARISON_DAY, "alice", "0.3"),
                _chargeback(_BASELINE_DAY, "credit", "-1"),
                _chargeback(_COMPARISON_DAY, "credit", "-2"),
            ]
        )
        uow.pipeline_state.upsert(_state(_BASELINE_DAY))
        uow.pipeline_state.upsert(_state(_COMPARISON_DAY))
        uow.commit()


def _seed_topic_attribution_comparison(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        uow.topic_attributions.upsert_batch(
            [
                _topic(_BASELINE_DAY, "cluster-a", "orders", "0.1"),
                _topic(_BASELINE_DAY, "cluster-b", "orders", "0.2"),
                _topic(_COMPARISON_DAY, "cluster-a", "orders", "0.3"),
                _topic(_COMPARISON_DAY, "cluster-b", "orders", "0.4"),
            ]
        )
        uow.pipeline_state.upsert(_state(_BASELINE_DAY, topic_attribution=True))
        uow.pipeline_state.upsert(_state(_COMPARISON_DAY, topic_attribution=True))
        uow.commit()


class _IncapableBackend:
    """A complete ordinary backend double deliberately missing the comparison capability."""

    def __init__(self) -> None:
        self.read_only_uow_calls = 0
        self.write_uow_calls = 0

    def create_unit_of_work(self) -> object:
        self.write_uow_calls += 1
        raise AssertionError("comparison must not open a write unit of work")

    def create_read_only_unit_of_work(self) -> object:
        self.read_only_uow_calls += 1
        raise AssertionError("comparison must not fall back to a normal read unit of work")

    def create_tables(self) -> None:
        return None

    def dispose(self) -> None:
        return None


@contextmanager
def _app_with_backend(backend: object) -> Iterator[tuple[TestClient, FixedTenantBackendProvider]]:
    settings = AppSettings(
        tenants={
            _TENANT_NAME: TenantConfig(
                ecosystem=_ECO,
                tenant_id=_TENANT_ID,
                storage=StorageConfig(connection_string="sqlite:///:memory:"),
            )
        }
    )
    app = create_app(settings, mode="api")
    with TestClient(app) as client:
        provider = install_backend(app, _TENANT_NAME, backend)  # type: ignore[arg-type]
        yield client, provider


class TestComparisonApiContract:
    def test_chargeback_response_uses_decimal_strings_and_full_scope_summary(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargeback_comparison(in_memory_backend)

        response = app_with_backend.get(_chargeback_url(), params=_comparison_params(group_by="principal"))

        assert response.status_code == 200
        body = response.json()
        assert body["source"] == "chargeback"
        assert body["granularity"] == "daily"
        assert body["baseline"]["start_date"] == "2026-01-10"
        assert body["baseline"]["start_at"] == "2026-01-10T00:00:00Z"
        assert body["comparison"]["end_at"] == "2026-01-12T00:00:00Z"
        assert body["summary"] == {
            "baseline_amount": "-0.7",
            "comparison_amount": "-1.7",
            "increases": "0",
            "decreases": "-1",
            "net_change": "-1",
            "percentage_change": "142.8571428571428571428571429",
        }
        assert {row["key"] for row in body["rows"]} == {"alice", "credit"}
        assert all(isinstance(row["baseline_amount"], str) for row in body["rows"])

    def test_response_keeps_long_decimal_values_as_exact_json_strings(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert_batch(
                [
                    _chargeback(
                        _BASELINE_DAY,
                        "precise",
                        "12345678901234567890.123456789",
                        timestamp=datetime(2026, 1, 10, 0, 0, tzinfo=UTC),
                    ),
                    _chargeback(
                        _BASELINE_DAY,
                        "precise",
                        "0.000000001",
                        timestamp=datetime(2026, 1, 10, 0, 0, 1, tzinfo=UTC),
                    ),
                    _chargeback(
                        _COMPARISON_DAY,
                        "precise",
                        "12345678901234567890.123456790",
                    ),
                ]
            )
            uow.pipeline_state.upsert(_state(_BASELINE_DAY))
            uow.pipeline_state.upsert(_state(_COMPARISON_DAY))
            uow.commit()

        response = app_with_backend.get(_chargeback_url(), params=_comparison_params())

        assert response.status_code == 200
        body = response.json()
        assert body["summary"]["baseline_amount"] == "12345678901234567890.12345679"
        assert body["summary"]["comparison_amount"] == "12345678901234567890.12345679"
        assert body["rows"][0]["baseline_amount"] == "12345678901234567890.12345679"
        assert body["rows"][0]["comparison_amount"] == "12345678901234567890.12345679"

    def test_topic_attribution_response_never_reads_chargeback_facts(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargeback_comparison(in_memory_backend)
        _seed_topic_attribution_comparison(in_memory_backend)

        response = app_with_backend.get(_topic_attribution_url(), params=_comparison_params(group_by="topic"))

        assert response.status_code == 200
        body = response.json()
        assert body["source"] == "topic_attribution"
        assert body["summary"]["baseline_amount"] == "0.3"
        assert body["summary"]["comparison_amount"] == "0.7"
        assert {row["key"] for row in body["rows"]} == {
            "cluster-a:topic:orders",
            "cluster-b:topic:orders",
        }
        assert body["baseline"]["coverage"]["status"] == "unknown"
        assert body["baseline"]["coverage"]["availability_cutoff_at"] is None

    def test_empty_chargeback_facts_with_successful_pipeline_state_confirms_zero(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from core.api.routes import cost_comparison

        evaluated_at = datetime(2026, 1, 12, tzinfo=UTC)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz: object | None = None) -> datetime:
                assert tz is UTC
                return evaluated_at

        monkeypatch.setattr(cost_comparison, "datetime", FixedDateTime)
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(_state(_BASELINE_DAY))
            uow.pipeline_state.upsert(_state(_COMPARISON_DAY))
            uow.commit()

        response = app_with_backend.get(_chargeback_url(), params=_comparison_params())

        assert response.status_code == 200
        body = response.json()
        assert body["rows"] == []
        assert body["summary"]["baseline_amount"] == "0"
        assert body["summary"]["comparison_amount"] == "0"
        assert body["baseline"]["coverage"]["status"] == "complete"

    def test_structural_validation_does_not_enter_route_dependencies(self, app_with_backend: TestClient) -> None:
        app = app_with_backend.app
        provider = app.state.backend_provider

        with patch("core.api.routes.cost_comparison.get_settings") as get_settings:
            response = app_with_backend.get(_chargeback_url(), params={"baseline_start": "not-a-date"})

        assert response.status_code == 422
        assert response.json()["detail"][0]["loc"] == ["query", "baseline_start"]
        get_settings.assert_not_called()
        assert provider.acquisitions == []

    @pytest.mark.parametrize(
        ("endpoint", "field", "value"),
        [
            pytest.param("chargeback", "group_by", "cluster", id="chargeback-group"),
            pytest.param("topic", "group_by", "environment", id="topic-group"),
            pytest.param("chargeback", "movement", "sideways", id="movement"),
            pytest.param("chargeback", "sort_by", "cost", id="sort-by"),
            pytest.param("chargeback", "sort_direction", "up", id="sort-direction"),
            pytest.param("chargeback", "limit", 0, id="limit-lower-bound"),
            pytest.param("chargeback", "limit", 501, id="limit-upper-bound"),
        ],
    )
    def test_invalid_enum_and_limit_values_return_exact_422_query_locations(
        self,
        app_with_backend: TestClient,
        endpoint: str,
        field: str,
        value: str | int,
    ) -> None:
        provider = app_with_backend.app.state.backend_provider
        params = _comparison_params()
        params[field] = value
        url = _topic_attribution_url() if endpoint == "topic" else _chargeback_url()

        response = app_with_backend.get(url, params=params)

        assert response.status_code == 422
        assert response.json()["detail"][0]["loc"] == ["query", field]
        assert provider.acquisitions == []

    def test_domain_validation_returns_exact_error_without_acquiring_storage(
        self, app_with_backend: TestClient
    ) -> None:
        provider = app_with_backend.app.state.backend_provider
        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(baseline_start="2026-01-11", baseline_end="2026-01-10"),
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "baseline_start must be <= baseline_end"}
        assert provider.acquisitions == []

    def test_comparison_domain_validation_returns_exact_error_without_acquiring_storage(
        self, app_with_backend: TestClient
    ) -> None:
        provider = app_with_backend.app.state.backend_provider
        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(comparison_start="2026-01-12", comparison_end="2026-01-11"),
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "comparison_start must be <= comparison_end"}
        assert provider.acquisitions == []

    def test_unknown_tenant_precedes_domain_validation_and_never_acquires_storage(
        self, app_with_backend: TestClient
    ) -> None:
        provider = app_with_backend.app.state.backend_provider
        response = app_with_backend.get(
            "/api/v1/tenants/missing/chargebacks/comparison",
            params=_comparison_params(baseline_start="2026-01-11", baseline_end="2026-01-10"),
        )

        assert response.status_code == 404
        assert response.json() == {"detail": "Tenant 'missing' not found"}
        assert provider.acquisitions == []

    def test_rejects_tag_value_without_key_before_storage(self, app_with_backend: TestClient) -> None:
        provider = app_with_backend.app.state.backend_provider
        response = app_with_backend.get(_chargeback_url(), params=_comparison_params(tag_value="prod"))

        assert response.status_code == 400
        assert response.json() == {"detail": "tag_value requires tag_key"}
        assert provider.acquisitions == []

    def test_rejects_invalid_tag_key_before_storage(self, app_with_backend: TestClient) -> None:
        provider = app_with_backend.app.state.backend_provider
        response = app_with_backend.get(_chargeback_url(), params=_comparison_params(tag_key="bad key"))

        assert response.status_code == 400
        assert response.json() == {"detail": "Invalid tag key format: 'bad key'"}
        assert provider.acquisitions == []

    def test_monthly_timezone_error_precedes_partial_range_errors(self, app_with_backend: TestClient) -> None:
        app_with_backend.app.state.settings.tenants[_TENANT_NAME].plugin_settings.chargeback_granularity = "monthly"

        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(
                baseline_start="2026-01-02",
                baseline_end="2026-01-16",
                comparison_start="2026-02-02",
                comparison_end="2026-02-16",
                timezone="America/Los_Angeles",
            ),
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "timezone must be UTC for monthly comparison data"}

    def test_incapable_leased_backend_returns_exact_503_and_releases_lease_without_fallback(self) -> None:
        backend = _IncapableBackend()
        with _app_with_backend(backend) as (client, provider):
            response = client.get(_chargeback_url(), params=_comparison_params())

        assert response.status_code == 503
        assert response.json() == {"detail": "Storage backend does not support consistent comparison reads"}
        assert provider.lease_events == [("enter", _TENANT_NAME), ("exit", _TENANT_NAME)]
        assert backend.read_only_uow_calls == 0
        assert backend.write_uow_calls == 0

    def test_missing_backend_provider_keeps_the_existing_exact_503(self, app_with_backend: TestClient) -> None:
        app_with_backend.app.state.backend_provider = None

        response = app_with_backend.get(_chargeback_url(), params=_comparison_params())

        assert response.status_code == 503
        assert response.json() == {"detail": "Storage backend provider is unavailable"}

    def test_registered_static_routes_are_not_captured_by_chargeback_dimension_route(
        self, app_with_backend: TestClient
    ) -> None:
        paths = {route.path for route in app_with_backend.app.routes}

        assert "/api/v1/tenants/{tenant_name}/chargebacks/comparison" in paths
        assert "/api/v1/tenants/{tenant_name}/topic-attributions/comparison" in paths


class TestComparisonSourceIsolationAndFilters:
    def test_chargeback_and_topic_attribution_keep_tenant_and_ecosystem_partitions(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert_batch(
                [
                    _chargeback(_BASELINE_DAY, "visible", "1"),
                    _chargeback(_COMPARISON_DAY, "visible", "2"),
                    _chargeback(_BASELINE_DAY, "hidden-tenant", "100", tenant_id="other-tenant"),
                    _chargeback(_COMPARISON_DAY, "hidden-tenant", "200", tenant_id="other-tenant"),
                    _chargeback(_BASELINE_DAY, "hidden-ecosystem", "1000", ecosystem="other-eco"),
                    _chargeback(_COMPARISON_DAY, "hidden-ecosystem", "2000", ecosystem="other-eco"),
                ]
            )
            uow.topic_attributions.upsert_batch(
                [
                    _topic(_BASELINE_DAY, "cluster-visible", "orders", "1"),
                    _topic(_COMPARISON_DAY, "cluster-visible", "orders", "2"),
                    _topic(
                        _BASELINE_DAY,
                        "cluster-hidden-tenant",
                        "orders",
                        "100",
                        tenant_id="other-tenant",
                    ),
                    _topic(
                        _COMPARISON_DAY,
                        "cluster-hidden-tenant",
                        "orders",
                        "200",
                        tenant_id="other-tenant",
                    ),
                    _topic(
                        _BASELINE_DAY,
                        "cluster-hidden-ecosystem",
                        "orders",
                        "1000",
                        ecosystem="other-eco",
                    ),
                    _topic(
                        _COMPARISON_DAY,
                        "cluster-hidden-ecosystem",
                        "orders",
                        "2000",
                        ecosystem="other-eco",
                    ),
                ]
            )
            uow.pipeline_state.upsert(_state(_BASELINE_DAY, topic_attribution=True))
            uow.pipeline_state.upsert(_state(_COMPARISON_DAY, topic_attribution=True))
            uow.commit()

        chargeback_response = app_with_backend.get(_chargeback_url(), params=_comparison_params(group_by="principal"))
        topic_response = app_with_backend.get(_topic_attribution_url(), params=_comparison_params(group_by="topic"))

        assert chargeback_response.status_code == 200
        assert chargeback_response.json()["summary"]["baseline_amount"] == "1"
        assert {row["key"] for row in chargeback_response.json()["rows"]} == {"visible"}
        assert topic_response.status_code == 200
        assert topic_response.json()["summary"]["comparison_amount"] == "2"
        assert {row["key"] for row in topic_response.json()["rows"]} == {"cluster-visible:topic:orders"}

    @pytest.mark.parametrize(
        ("filters", "case"),
        [
            pytest.param({"identity_id": "principal-match"}, "identity", id="identity"),
            pytest.param({"product_type": "KAFKA_MATCH"}, "product", id="product"),
            pytest.param({"resource_id": "resource-match"}, "resource", id="resource"),
            pytest.param({"cost_type": "usage"}, "cost-type", id="cost-type"),
            pytest.param({"tag_key": "scope"}, "tag-presence", id="tag-presence"),
            pytest.param({"tag_key": "scope", "tag_value": "match"}, "tag-value", id="tag-value"),
        ],
    )
    def test_chargeback_filters_apply_to_both_period_streams(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        filters: dict[str, str],
        case: str,
    ) -> None:
        del case
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert_batch(
                [
                    _chargeback(
                        _BASELINE_DAY,
                        "principal-match",
                        "1",
                        resource_id="resource-match",
                        product_type="KAFKA_MATCH",
                        cost_type=CostType.USAGE,
                    ),
                    _chargeback(
                        _COMPARISON_DAY,
                        "principal-match",
                        "2",
                        resource_id="resource-match",
                        product_type="KAFKA_MATCH",
                        cost_type=CostType.USAGE,
                    ),
                    _chargeback(
                        _BASELINE_DAY,
                        "principal-other",
                        "100",
                        resource_id="resource-other",
                        product_type="KAFKA_OTHER",
                        cost_type=CostType.SHARED,
                    ),
                    _chargeback(
                        _COMPARISON_DAY,
                        "principal-other",
                        "200",
                        resource_id="resource-other",
                        product_type="KAFKA_OTHER",
                        cost_type=CostType.SHARED,
                    ),
                ]
            )
            uow.pipeline_state.upsert(_state(_BASELINE_DAY))
            uow.pipeline_state.upsert(_state(_COMPARISON_DAY))
            uow.commit()
        _add_tag(
            in_memory_backend,
            entity_type="identity",
            entity_id="principal-match",
            tag_key="scope",
            tag_value="match",
        )
        _add_tag(
            in_memory_backend,
            entity_type="identity",
            entity_id="principal-other",
            tag_key="other",
            tag_value="value",
        )

        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(group_by="principal", **filters),
        )

        assert response.status_code == 200
        body = response.json()
        assert body["summary"]["baseline_amount"] == "1"
        assert body["summary"]["comparison_amount"] == "2"
        assert [row["key"] for row in body["rows"]] == ["principal-match"]

    @pytest.mark.parametrize(
        ("filters", "case"),
        [
            pytest.param({"cluster_resource_id": "cluster-match"}, "cluster", id="cluster"),
            pytest.param({"topic_name": "orders-match"}, "topic", id="topic"),
            pytest.param({"product_type": "KAFKA_MATCH"}, "product", id="product"),
            pytest.param({"attribution_method": "bytes_ratio"}, "method", id="method"),
            pytest.param({"tag_key": "scope"}, "tag-presence", id="tag-presence"),
            pytest.param({"tag_key": "scope", "tag_value": "match"}, "tag-value", id="tag-value"),
        ],
    )
    def test_topic_attribution_filters_apply_to_both_period_streams(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        filters: dict[str, str],
        case: str,
    ) -> None:
        del case
        with in_memory_backend.create_unit_of_work() as uow:
            uow.topic_attributions.upsert_batch(
                [
                    _topic(
                        _BASELINE_DAY,
                        "cluster-match",
                        "orders-match",
                        "1",
                        product_type="KAFKA_MATCH",
                        attribution_method="bytes_ratio",
                    ),
                    _topic(
                        _COMPARISON_DAY,
                        "cluster-match",
                        "orders-match",
                        "2",
                        product_type="KAFKA_MATCH",
                        attribution_method="bytes_ratio",
                    ),
                    _topic(
                        _BASELINE_DAY,
                        "cluster-other",
                        "orders-other",
                        "100",
                        product_type="KAFKA_OTHER",
                        attribution_method="even_split",
                    ),
                    _topic(
                        _COMPARISON_DAY,
                        "cluster-other",
                        "orders-other",
                        "200",
                        product_type="KAFKA_OTHER",
                        attribution_method="even_split",
                    ),
                ]
            )
            uow.pipeline_state.upsert(_state(_BASELINE_DAY, topic_attribution=True))
            uow.pipeline_state.upsert(_state(_COMPARISON_DAY, topic_attribution=True))
            uow.commit()
        _add_tag(
            in_memory_backend,
            entity_type="resource",
            entity_id=_topic_resource_id("cluster-match", "orders-match"),
            tag_key="scope",
            tag_value="match",
        )
        _add_tag(
            in_memory_backend,
            entity_type="resource",
            entity_id=_topic_resource_id("cluster-other", "orders-other"),
            tag_key="other",
            tag_value="value",
        )

        response = app_with_backend.get(
            _topic_attribution_url(),
            params=_comparison_params(group_by="topic", **filters),
        )

        assert response.status_code == 200
        body = response.json()
        assert body["summary"]["baseline_amount"] == "1"
        assert body["summary"]["comparison_amount"] == "2"
        assert [row["key"] for row in body["rows"]] == ["cluster-match:topic:orders-match"]


class TestComparisonPeriodAndCoverageContract:
    def test_expected_source_slots_are_resolved_once_per_period(self, app_with_backend: TestClient) -> None:
        from core.api.routes import cost_comparison

        original = cost_comparison._expected_source_timestamps
        with patch.object(cost_comparison, "_expected_source_timestamps", wraps=original) as expected_slots:
            response = app_with_backend.get(_chargeback_url(), params=_comparison_params())

        assert response.status_code == 200
        assert expected_slots.call_count == 2

    def test_non_utc_dst_period_returns_complete_bounds_duration_and_coverage_contract(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from core.api.routes import cost_comparison

        evaluated_at = datetime(2026, 3, 11, tzinfo=UTC)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz: object | None = None) -> datetime:
                assert tz is UTC
                return evaluated_at

        monkeypatch.setattr(cost_comparison, "datetime", FixedDateTime)
        baseline_day = date(2026, 3, 8)
        comparison_day = date(2026, 3, 9)
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert_batch(
                [
                    _chargeback(
                        baseline_day,
                        "dst",
                        "1",
                        timestamp=datetime(2026, 3, 8, 12, tzinfo=UTC),
                    ),
                    _chargeback(
                        comparison_day,
                        "dst",
                        "2",
                        timestamp=datetime(2026, 3, 9, 12, tzinfo=UTC),
                    ),
                ]
            )
            # Daily source slots are UTC based even when the selected calendar
            # dates are resolved in a user's local timezone.
            uow.pipeline_state.upsert(_state(date(2026, 3, 9)))
            uow.pipeline_state.upsert(_state(date(2026, 3, 10)))
            uow.commit()

        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(
                baseline_start=baseline_day.isoformat(),
                baseline_end=baseline_day.isoformat(),
                comparison_start=comparison_day.isoformat(),
                comparison_end=comparison_day.isoformat(),
                timezone="America/Los_Angeles",
            ),
        )

        assert response.status_code == 200
        body = response.json()
        evaluated_at = datetime.fromisoformat(body["coverage_evaluated_at"].replace("Z", "+00:00"))
        cutoff_at = f"{(evaluated_at - timedelta(days=250)).date().isoformat()}T00:00:00Z"
        assert body["timezone"] == "America/Los_Angeles"
        assert body["coverage_evaluated_at"].endswith("Z")
        assert body["baseline"] == {
            "start_date": "2026-03-08",
            "end_date": "2026-03-08",
            "start_at": "2026-03-08T08:00:00Z",
            "end_at": "2026-03-09T07:00:00Z",
            "duration_seconds": 82800,
            "coverage": {
                "status": "complete",
                "expected_dates": ["2026-03-09"],
                "unknown_dates": [],
                "incomplete_dates": [],
                "retention_qualified_dates": [],
                "availability_cutoff_at": cutoff_at,
            },
        }
        assert body["comparison"] == {
            "start_date": "2026-03-09",
            "end_date": "2026-03-09",
            "start_at": "2026-03-09T07:00:00Z",
            "end_at": "2026-03-10T07:00:00Z",
            "duration_seconds": 86400,
            "coverage": {
                "status": "complete",
                "expected_dates": ["2026-03-10"],
                "unknown_dates": [],
                "incomplete_dates": [],
                "retention_qualified_dates": [],
                "availability_cutoff_at": cutoff_at,
            },
        }
        assert body["unequal_durations"] is True
        assert body["summary"] == {
            "baseline_amount": "1",
            "comparison_amount": "2",
            "increases": "1",
            "decreases": "0",
            "net_change": "1",
            "percentage_change": "100",
        }

    def test_non_utc_fall_back_prior_day_reports_25_hour_bounds_and_coverage(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from core.api.routes import cost_comparison

        evaluated_at = datetime(2026, 11, 3, tzinfo=UTC)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz: object | None = None) -> datetime:
                assert tz is UTC
                return evaluated_at

        monkeypatch.setattr(cost_comparison, "datetime", FixedDateTime)
        baseline_day = date(2026, 11, 1)
        comparison_day = date(2026, 10, 31)
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert_batch(
                [
                    _chargeback(
                        baseline_day,
                        "dst",
                        "2",
                        timestamp=datetime(2026, 11, 1, 12, tzinfo=UTC),
                    ),
                    _chargeback(
                        comparison_day,
                        "dst",
                        "1",
                        timestamp=datetime(2026, 10, 31, 12, tzinfo=UTC),
                    ),
                ]
            )
            uow.pipeline_state.upsert(_state(date(2026, 11, 2)))
            uow.pipeline_state.upsert(_state(date(2026, 11, 1)))
            uow.commit()

        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(
                baseline_start=baseline_day.isoformat(),
                baseline_end=baseline_day.isoformat(),
                comparison_start=comparison_day.isoformat(),
                comparison_end=comparison_day.isoformat(),
                timezone="America/Los_Angeles",
            ),
        )

        assert response.status_code == 200
        body = response.json()
        evaluated_at = datetime.fromisoformat(body["coverage_evaluated_at"].replace("Z", "+00:00"))
        cutoff_at = f"{(evaluated_at - timedelta(days=250)).date().isoformat()}T00:00:00Z"
        assert body["baseline"] == {
            "start_date": "2026-11-01",
            "end_date": "2026-11-01",
            "start_at": "2026-11-01T07:00:00Z",
            "end_at": "2026-11-02T08:00:00Z",
            "duration_seconds": 90000,
            "coverage": {
                "status": "complete",
                "expected_dates": ["2026-11-02"],
                "unknown_dates": [],
                "incomplete_dates": [],
                "retention_qualified_dates": [],
                "availability_cutoff_at": cutoff_at,
            },
        }
        assert body["comparison"] == {
            "start_date": "2026-10-31",
            "end_date": "2026-10-31",
            "start_at": "2026-10-31T07:00:00Z",
            "end_at": "2026-11-01T07:00:00Z",
            "duration_seconds": 86400,
            "coverage": {
                "status": "complete",
                "expected_dates": ["2026-11-01"],
                "unknown_dates": [],
                "incomplete_dates": [],
                "retention_qualified_dates": [],
                "availability_cutoff_at": cutoff_at,
            },
        }
        assert body["unequal_durations"] is True
        assert body["summary"] == {
            "baseline_amount": "2",
            "comparison_amount": "1",
            "increases": "0",
            "decreases": "-1",
            "net_change": "-1",
            "percentage_change": "-50",
        }

    @pytest.mark.parametrize(
        "case",
        _DST_WEEK_CASES,
        ids=("spring-forward-167-hours", "fall-back-169-hours"),
    )
    def test_non_utc_monday_to_sunday_dst_week_reports_exact_bounds_and_coverage(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        case: _DstWeekCase,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from core.api.routes import cost_comparison

        evaluated_at = datetime.combine(case.comparison_end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz: object | None = None) -> datetime:
                assert tz is UTC
                return evaluated_at

        monkeypatch.setattr(cost_comparison, "datetime", FixedDateTime)
        with in_memory_backend.create_unit_of_work() as uow:
            for tracking_date in case.baseline_expected_dates + case.comparison_expected_dates:
                uow.pipeline_state.upsert(_state(date.fromisoformat(tracking_date)))
            uow.commit()

        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(
                baseline_start=case.baseline_start.isoformat(),
                baseline_end=case.baseline_end.isoformat(),
                comparison_start=case.comparison_start.isoformat(),
                comparison_end=case.comparison_end.isoformat(),
                timezone="America/Los_Angeles",
            ),
        )

        assert response.status_code == 200
        body = response.json()
        evaluated_at = datetime.fromisoformat(body["coverage_evaluated_at"].replace("Z", "+00:00"))
        cutoff_at = f"{(evaluated_at - timedelta(days=250)).date().isoformat()}T00:00:00Z"
        assert body["timezone"] == "America/Los_Angeles"
        assert body["baseline"]["start_at"] == case.baseline_start_at
        assert body["baseline"]["end_at"] == case.baseline_end_at
        assert body["baseline"]["duration_seconds"] == case.baseline_duration_seconds
        assert body["baseline"]["coverage"] == {
            "status": "complete",
            "expected_dates": list(case.baseline_expected_dates),
            "unknown_dates": [],
            "incomplete_dates": [],
            "retention_qualified_dates": [],
            "availability_cutoff_at": cutoff_at,
        }
        assert body["comparison"]["start_at"] == case.comparison_start_at
        assert body["comparison"]["end_at"] == case.comparison_end_at
        assert body["comparison"]["duration_seconds"] == case.comparison_duration_seconds
        assert body["comparison"]["coverage"] == {
            "status": "complete",
            "expected_dates": list(case.comparison_expected_dates),
            "unknown_dates": [],
            "incomplete_dates": [],
            "retention_qualified_dates": [],
            "availability_cutoff_at": cutoff_at,
        }
        assert body["coverage_evaluated_at"].endswith("Z")
        assert body["unequal_durations"] is True
        assert body["summary"] == {
            "baseline_amount": "0",
            "comparison_amount": "0",
            "increases": "0",
            "decreases": "0",
            "net_change": "0",
            "percentage_change": None,
        }

    def test_chargeback_range_includes_both_requested_calendar_dates(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert_batch(
                [
                    _chargeback(_BASELINE_DAY, "window", "1"),
                    _chargeback(
                        _BASELINE_DAY,
                        "window",
                        "2",
                        timestamp=datetime(2026, 1, 10, 23, 59, 59, tzinfo=UTC),
                    ),
                    _chargeback(_COMPARISON_DAY, "window", "3"),
                    _chargeback(
                        _COMPARISON_DAY,
                        "window",
                        "4",
                        timestamp=datetime(2026, 1, 11, 23, 59, 59, tzinfo=UTC),
                    ),
                    _chargeback(date(2026, 1, 9), "window", "100"),
                    _chargeback(date(2026, 1, 12), "window", "200"),
                ]
            )
            uow.pipeline_state.upsert(_state(_BASELINE_DAY))
            uow.pipeline_state.upsert(_state(_COMPARISON_DAY))
            uow.commit()

        response = app_with_backend.get(_chargeback_url(), params=_comparison_params(group_by="principal"))

        assert response.status_code == 200
        assert response.json()["summary"]["baseline_amount"] == "3"
        assert response.json()["summary"]["comparison_amount"] == "7"

    def test_topic_attribution_range_includes_both_requested_calendar_dates(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        with in_memory_backend.create_unit_of_work() as uow:
            uow.topic_attributions.upsert_batch(
                [
                    _topic(_BASELINE_DAY, "cluster", "window", "1"),
                    _topic(
                        _BASELINE_DAY,
                        "cluster",
                        "window",
                        "2",
                        timestamp=datetime(2026, 1, 10, 23, 59, 59, tzinfo=UTC),
                    ),
                    _topic(_COMPARISON_DAY, "cluster", "window", "3"),
                    _topic(
                        _COMPARISON_DAY,
                        "cluster",
                        "window",
                        "4",
                        timestamp=datetime(2026, 1, 11, 23, 59, 59, tzinfo=UTC),
                    ),
                    _topic(date(2026, 1, 9), "cluster", "window", "100"),
                    _topic(date(2026, 1, 12), "cluster", "window", "200"),
                ]
            )
            uow.pipeline_state.upsert(_state(_BASELINE_DAY, topic_attribution=True))
            uow.pipeline_state.upsert(_state(_COMPARISON_DAY, topic_attribution=True))
            uow.commit()

        response = app_with_backend.get(_topic_attribution_url(), params=_comparison_params(group_by="topic"))

        assert response.status_code == 200
        assert response.json()["summary"]["baseline_amount"] == "3"
        assert response.json()["summary"]["comparison_amount"] == "7"

    def test_topic_attribution_filtered_zero_is_complete_when_unfiltered_slots_exist(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        baseline_day = date.today() - timedelta(days=2)
        comparison_day = date.today() - timedelta(days=1)
        _set_plugin_settings(
            app_with_backend,
            topic_attribution={"enabled": True, "retention_days": 90},
        )
        with in_memory_backend.create_unit_of_work() as uow:
            uow.topic_attributions.upsert_batch(
                [
                    _topic(baseline_day, "cluster-present", "orders", "1"),
                    _topic(comparison_day, "cluster-present", "orders", "2"),
                ]
            )
            uow.pipeline_state.upsert(_state(baseline_day, topic_attribution=True))
            uow.pipeline_state.upsert(_state(comparison_day, topic_attribution=True))
            uow.commit()

        response = app_with_backend.get(
            _topic_attribution_url(),
            params=_comparison_params(
                baseline_start=baseline_day.isoformat(),
                baseline_end=baseline_day.isoformat(),
                comparison_start=comparison_day.isoformat(),
                comparison_end=comparison_day.isoformat(),
                cluster_resource_id="cluster-with-no-facts",
            ),
        )

        assert response.status_code == 200
        body = response.json()
        assert body["rows"] == []
        assert body["summary"]["baseline_amount"] == "0"
        assert body["baseline"]["coverage"]["status"] == "complete"
        assert body["comparison"]["coverage"]["status"] == "complete"

    def test_topic_attribution_source_wide_zero_remains_retention_qualified(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        baseline_day = date.today() - timedelta(days=2)
        comparison_day = date.today() - timedelta(days=1)
        _set_plugin_settings(
            app_with_backend,
            topic_attribution={"enabled": True, "retention_days": 90},
        )
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(_state(baseline_day, topic_attribution=True))
            uow.pipeline_state.upsert(_state(comparison_day, topic_attribution=True))
            uow.commit()

        response = app_with_backend.get(
            _topic_attribution_url(),
            params=_comparison_params(
                baseline_start=baseline_day.isoformat(),
                baseline_end=baseline_day.isoformat(),
                comparison_start=comparison_day.isoformat(),
                comparison_end=comparison_day.isoformat(),
            ),
        )

        assert response.status_code == 200
        body = response.json()
        assert body["baseline"]["coverage"] == {
            "status": "unknown",
            "expected_dates": [baseline_day.isoformat()],
            "unknown_dates": [baseline_day.isoformat()],
            "incomplete_dates": [],
            "retention_qualified_dates": [baseline_day.isoformat()],
            "availability_cutoff_at": body["baseline"]["coverage"]["availability_cutoff_at"],
        }

    def test_monthly_comparison_accepts_complete_utc_calendar_months(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        baseline_day = date(2026, 1, 1)
        comparison_day = date(2026, 2, 1)
        _set_plugin_settings(app_with_backend, chargeback_granularity="monthly")
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert_batch(
                [
                    _chargeback(baseline_day, "monthly", "1"),
                    _chargeback(comparison_day, "monthly", "2"),
                ]
            )
            uow.pipeline_state.upsert(_state(baseline_day))
            uow.pipeline_state.upsert(_state(comparison_day))
            uow.commit()

        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(
                baseline_start="2026-01-01",
                baseline_end="2026-01-31",
                comparison_start="2026-02-01",
                comparison_end="2026-02-28",
            ),
        )

        assert response.status_code == 200
        assert response.json()["granularity"] == "monthly"
        assert response.json()["summary"]["net_change"] == "1"

    @pytest.mark.parametrize(
        ("params", "detail"),
        [
            pytest.param(
                {
                    "baseline_start": "2026-01-02",
                    "baseline_end": "2026-01-31",
                    "comparison_start": "2026-02-01",
                    "comparison_end": "2026-02-28",
                },
                "baseline period must contain complete UTC calendar months for monthly comparison data",
                id="baseline-alignment",
            ),
            pytest.param(
                {
                    "baseline_start": "2026-01-01",
                    "baseline_end": "2026-01-31",
                    "comparison_start": "2026-02-01",
                    "comparison_end": "2026-02-27",
                },
                "comparison period must contain complete UTC calendar months for monthly comparison data",
                id="comparison-alignment",
            ),
        ],
    )
    def test_monthly_comparison_reports_the_misaligned_period(
        self,
        app_with_backend: TestClient,
        params: dict[str, str],
        detail: str,
    ) -> None:
        _set_plugin_settings(app_with_backend, chargeback_granularity="monthly")

        response = app_with_backend.get(_chargeback_url(), params=_comparison_params(**params))

        assert response.status_code == 400
        assert response.json() == {"detail": detail}

    def test_unknown_timezone_returns_the_existing_client_error_before_storage(
        self, app_with_backend: TestClient
    ) -> None:
        provider = app_with_backend.app.state.backend_provider

        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(timezone="Moon/Tranquility"),
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Unknown timezone: 'Moon/Tranquility'"}
        assert provider.acquisitions == []

    def test_unknown_timezone_precedes_monthly_utc_policy_error(self, app_with_backend: TestClient) -> None:
        _set_plugin_settings(app_with_backend, chargeback_granularity="monthly")

        response = app_with_backend.get(
            _chargeback_url(),
            params=_comparison_params(
                baseline_start="2026-01-01",
                baseline_end="2026-01-31",
                comparison_start="2026-02-01",
                comparison_end="2026-02-28",
                timezone="Moon/Tranquility",
            ),
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Unknown timezone: 'Moon/Tranquility'"}


class TestComparisonRetentionAndRuntimeFailures:
    def test_topic_retention_policy_is_resolved_before_provider_lease(
        self, app_with_backend: TestClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from core.api.routes import cost_comparison

        _set_plugin_settings(
            app_with_backend,
            topic_attribution={"enabled": True, "retention_days": 90},
        )
        provider = app_with_backend.app.state.backend_provider

        def resolve_before_lease(*args: object, **kwargs: object) -> int:
            del args, kwargs
            assert provider.acquisitions == []
            return 90

        monkeypatch.setattr(
            cost_comparison,
            "resolve_topic_attribution_retention_days",
            resolve_before_lease,
        )

        response = app_with_backend.get(_topic_attribution_url(), params=_comparison_params())

        assert response.status_code == 200
        assert provider.lease_events == [("enter", _TENANT_NAME), ("exit", _TENANT_NAME)]

    def test_lazy_fact_stream_failure_returns_sanitized_error_and_releases_lease(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository

        _seed_chargeback_comparison(in_memory_backend)
        original_iter = SQLModelChargebackRepository.iter_by_filters

        def failing_iter(
            self: SQLModelChargebackRepository,
            *args: Any,
            **kwargs: Any,
        ) -> Iterator[ChargebackRow]:
            yield from original_iter(self, *args, **kwargs)
            raise RuntimeError("late stream failure")

        monkeypatch.setattr(SQLModelChargebackRepository, "iter_by_filters", failing_iter)
        provider = app_with_backend.app.state.backend_provider
        client = TestClient(app_with_backend.app, raise_server_exceptions=False)
        try:
            response = client.get(_chargeback_url(), params=_comparison_params())
        finally:
            client.close()

        assert response.status_code == 500
        assert response.json()["detail"] == "Internal server error"
        assert provider.lease_events[-2:] == [("enter", _TENANT_NAME), ("exit", _TENANT_NAME)]

    def test_topic_retention_window_qualifies_before_cutoff_and_confirms_at_and_after_cutoff(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from core.api.routes import cost_comparison

        evaluated_at = datetime(2026, 4, 15, tzinfo=UTC)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz: object | None = None) -> datetime:
                assert tz is UTC
                return evaluated_at

        monkeypatch.setattr(cost_comparison, "datetime", FixedDateTime)
        _set_plugin_settings(
            app_with_backend,
            chargeback_granularity="hourly",
            topic_attribution={"enabled": True, "retention_days": 90},
        )
        cutoff_day = (evaluated_at - timedelta(days=90)).date()
        before_cutoff = cutoff_day - timedelta(days=1)
        after_cutoff = cutoff_day + timedelta(days=1)
        with in_memory_backend.create_unit_of_work() as uow:
            uow.topic_attributions.upsert_batch(
                [
                    _topic(
                        day,
                        "cluster-retention",
                        "orders",
                        "1",
                        timestamp=datetime(day.year, day.month, day.day, hour, tzinfo=UTC),
                    )
                    for day in (before_cutoff, cutoff_day, after_cutoff)
                    for hour in range(24)
                ]
            )
            uow.chargebacks.upsert_batch(
                [
                    _chargeback(
                        day,
                        "chargeback-retention",
                        "1",
                        timestamp=datetime(day.year, day.month, day.day, hour, tzinfo=UTC),
                    )
                    for day in (before_cutoff, cutoff_day)
                    for hour in range(24)
                ]
            )
            for day in (before_cutoff, cutoff_day, after_cutoff):
                uow.pipeline_state.upsert(_state(day, topic_attribution=True))
            uow.commit()

        before_and_at = _comparison_params(
            baseline_start=before_cutoff.isoformat(),
            baseline_end=before_cutoff.isoformat(),
            comparison_start=cutoff_day.isoformat(),
            comparison_end=cutoff_day.isoformat(),
        )
        at_and_after = _comparison_params(
            baseline_start=cutoff_day.isoformat(),
            baseline_end=cutoff_day.isoformat(),
            comparison_start=after_cutoff.isoformat(),
            comparison_end=after_cutoff.isoformat(),
        )

        before_response = app_with_backend.get(_topic_attribution_url(), params=before_and_at)
        at_response = app_with_backend.get(_topic_attribution_url(), params=at_and_after)
        chargeback_response = app_with_backend.get(_chargeback_url(), params=before_and_at)

        assert before_response.status_code == 200
        before_body = before_response.json()
        assert before_body["baseline"]["coverage"]["status"] == "unknown"
        assert before_body["baseline"]["coverage"]["retention_qualified_dates"] == [before_cutoff.isoformat()]
        assert before_body["comparison"]["coverage"]["availability_cutoff_at"] == "2026-01-15T00:00:00Z"
        assert before_body["comparison"]["coverage"]["status"] == "complete"
        assert at_response.status_code == 200
        assert at_response.json()["baseline"]["coverage"]["status"] == "complete"
        assert at_response.json()["comparison"]["coverage"]["status"] == "complete"
        assert chargeback_response.status_code == 200
        assert chargeback_response.json()["baseline"]["coverage"]["status"] == "complete"
        assert chargeback_response.json()["baseline"]["coverage"]["availability_cutoff_at"] == "2025-08-08T00:00:00Z"

    def test_topic_retention_expansion_keeps_unknown_slots_without_source_timestamps(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from core.api.routes import cost_comparison

        evaluated_at = datetime(2026, 9, 1, tzinfo=UTC)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz: object | None = None) -> datetime:
                assert tz is UTC
                return evaluated_at

        monkeypatch.setattr(cost_comparison, "datetime", FixedDateTime)
        older_day = (evaluated_at - timedelta(days=200)).date()
        comparison_day = older_day + timedelta(days=1)
        with in_memory_backend.create_unit_of_work() as uow:
            uow.pipeline_state.upsert(_state(older_day, topic_attribution=True))
            uow.pipeline_state.upsert(_state(comparison_day, topic_attribution=True))
            uow.commit()

        params = _comparison_params(
            baseline_start=older_day.isoformat(),
            baseline_end=older_day.isoformat(),
            comparison_start=comparison_day.isoformat(),
            comparison_end=comparison_day.isoformat(),
        )
        tenant_config = app_with_backend.app.state.settings.tenants[_TENANT_NAME]
        assert tenant_config.retention_days == 250

        _set_plugin_settings(app_with_backend, topic_attribution={"enabled": True, "retention_days": 90})
        short_policy_response = app_with_backend.get(_topic_attribution_url(), params=params)
        _set_plugin_settings(app_with_backend, topic_attribution={"enabled": True, "retention_days": 250})
        expanded_policy_response = app_with_backend.get(_topic_attribution_url(), params=params)

        assert short_policy_response.status_code == 200
        assert expanded_policy_response.status_code == 200
        short_body = short_policy_response.json()
        expanded_body = expanded_policy_response.json()
        short_cutoff = f"{(evaluated_at - timedelta(days=90)).isoformat().replace('+00:00', 'Z')}"
        expanded_cutoff = f"{(evaluated_at - timedelta(days=250)).isoformat().replace('+00:00', 'Z')}"
        for body, cutoff in ((short_body, short_cutoff), (expanded_body, expanded_cutoff)):
            assert body["baseline"]["coverage"] == {
                "status": "unknown",
                "expected_dates": [older_day.isoformat()],
                "unknown_dates": [older_day.isoformat()],
                "incomplete_dates": [],
                "retention_qualified_dates": [older_day.isoformat()],
                "availability_cutoff_at": cutoff,
            }
            assert body["comparison"]["coverage"] == {
                "status": "unknown",
                "expected_dates": [comparison_day.isoformat()],
                "unknown_dates": [comparison_day.isoformat()],
                "incomplete_dates": [],
                "retention_qualified_dates": [comparison_day.isoformat()],
                "availability_cutoff_at": cutoff,
            }
        assert (
            short_body["baseline"]["coverage"]["availability_cutoff_at"]
            != expanded_body["baseline"]["coverage"]["availability_cutoff_at"]
        )

    def test_repository_failure_returns_sanitized_error_id(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargeback_comparison(in_memory_backend)

        with (
            patch(
                "core.storage.backends.sqlmodel.repositories.SQLModelChargebackRepository.iter_by_filters",
                side_effect=RuntimeError("connection credentials must not reach the client"),
            ),
            TestClient(app_with_backend.app, raise_server_exceptions=False) as client,
        ):
            response = client.get(
                _chargeback_url(),
                params=_comparison_params(group_by="principal"),
            )

        assert response.status_code == 500
        body = response.json()
        assert body["detail"] == "Internal server error"
        UUID(body["error_id"])
        assert "credentials" not in str(body)

    def test_invalid_provider_initialization_returns_sanitized_error_without_comparison_fallback(
        self, tmp_path: Path
    ) -> None:
        settings = AppSettings(
            tenants={
                "broken": TenantConfig(
                    ecosystem="confluent_cloud",
                    tenant_id="broken-tenant",
                    storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'broken.db'}"),
                    plugin_settings={"topic_attribution": {"enabled": True, "retention_days": 90}},
                )
            }
        )
        app = create_app(settings, mode="api")

        assert "/api/v1/tenants/{tenant_name}/chargebacks/comparison" in {route.path for route in app.routes}

        with (
            patch(
                "core.storage.backends.sqlmodel.unit_of_work.SQLModelBackend.create_consistent_read_unit_of_work",
                side_effect=AssertionError("comparison storage must not be reached"),
                create=True,
            ) as create_consistent_read,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.get(
                "/api/v1/tenants/broken/chargebacks/comparison",
                params=_comparison_params(),
            )

        assert response.status_code == 500
        body = response.json()
        assert body["detail"] == "Internal server error"
        UUID(body["error_id"])
        create_consistent_read.assert_not_called()


class TestComparisonProductionWiring:
    @pytest.mark.parametrize("ecosystem", ["confluent_cloud", "self_managed_kafka"])
    @pytest.mark.parametrize(
        ("retention", "evidence", "baseline_status", "comparison_status"),
        [
            (None, "complete", "complete", "complete"),
            (180, "complete", "complete", "complete"),
            (None, "filtered_zero", "complete", "complete"),
            (None, "missing_slot", "unknown", "complete"),
            (None, "unfinished", "incomplete", "complete"),
            (None, "missing_state", "unknown", "complete"),
            (10, "complete", "unknown", "unknown"),
        ],
    )
    def test_builtin_effective_retention_preserves_source_coverage(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        ecosystem: str,
        retention: int | None,
        evidence: str,
        baseline_status: str,
        comparison_status: str,
    ) -> None:
        from core.api.routes import cost_comparison

        evaluated_at = datetime(2026, 9, 7, 12, tzinfo=UTC)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz: object | None = None) -> datetime:
                assert tz is UTC
                return evaluated_at

        monkeypatch.setattr(cost_comparison, "datetime", FixedDateTime)
        topic_settings: dict[str, object] = {"enabled": True}
        if retention is not None:
            topic_settings["retention_days"] = retention
        plugin_values: dict[str, object] = {
            "metrics": {"url": "http://prometheus.invalid"},
            "topic_attribution": topic_settings,
        }
        if ecosystem == "confluent_cloud":
            plugin_values["ccloud_api"] = {"key": "test-key", "secret": "test-secret"}  # pragma: allowlist secret
        else:
            plugin_values.update(
                {
                    "cluster_id": "cluster",
                    "metrics_identifier": "cluster",
                    "broker_count": 3,
                    "cost_model": {
                        "compute_hourly_rate": "1",
                        "storage_per_gib_hourly": "0.01",
                        "network_ingress_per_gib": "0.01",
                        "network_egress_per_gib": "0.01",
                    },
                }
            )
        config = TenantConfig(
            ecosystem=ecosystem,
            tenant_id="retention-tenant",
            storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'comparison.db'}"),
            plugin_settings=PluginSettingsBase.model_validate(plugin_values),
        )
        app = create_app(AppSettings(tenants={"retention": config}), mode="api")
        with TestClient(app) as client:
            with (
                app.state.backend_provider.acquire_backend("retention", config) as backend,
                backend.create_unit_of_work() as uow,
            ):
                for day_number in range(18, 22):
                    day = date(2026, 8, day_number)
                    if not (evidence == "missing_slot" and day_number == 18):
                        uow.topic_attributions.upsert_batch(
                            [_topic(day, "cluster", "orders", "1", ecosystem=ecosystem, tenant_id=config.tenant_id)]
                        )
                    if not (evidence == "missing_state" and day_number == 18):
                        state = _state(day, topic_attribution=True)
                        state.ecosystem = ecosystem
                        state.tenant_id = config.tenant_id
                        state.topic_attribution_calculated = not (evidence == "unfinished" and day_number == 18)
                        uow.pipeline_state.upsert(state)
                uow.commit()
            params = _comparison_params(
                baseline_start="2026-08-17",
                baseline_end="2026-08-18",
                comparison_start="2026-08-19",
                comparison_end="2026-08-20",
                timezone="America/Los_Angeles",
            )
            if evidence == "filtered_zero":
                params["cluster_resource_id"] = "no-matching-cluster"
            response = client.get("/api/v1/tenants/retention/topic-attributions/comparison", params=params)

        assert response.status_code == 200
        body = response.json()
        cutoff = (evaluated_at - timedelta(days=retention or 90)).isoformat().replace("+00:00", "Z")
        for period, status in (("baseline", baseline_status), ("comparison", comparison_status)):
            assert body[period]["coverage"]["status"] == status
            assert body[period]["coverage"]["availability_cutoff_at"] == cutoff
        if evidence == "filtered_zero":
            assert body["rows"] == []
            assert body["summary"]["baseline_amount"] == "0"
            assert body["summary"]["comparison_amount"] == "0"
        else:
            assert body["summary"]["baseline_amount"] == ("1" if evidence == "missing_slot" else "2")
            assert body["summary"]["comparison_amount"] == "2"

    def test_real_provider_serves_two_tenants_with_each_retention_policy_shape(self, tmp_path: Path) -> None:
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        def tenant_config(name: str, topic_policy: dict[str, object] | None) -> TenantConfig:
            plugin_values: dict[str, object] = {
                "cluster_id": f"cluster-{name}",
                "metrics": {"url": "http://prometheus.invalid"},
                "identity_source": {"source": "static", "static_identities": []},
                "cost_types": [
                    {
                        "name": "KAFKA_BASE",
                        "product_category": "kafka",
                        "rate": "1",
                        "cost_quantity": {"type": "fixed", "count": 1},
                        "allocation_strategy": "even_split",
                    }
                ],
            }
            if topic_policy is not None:
                plugin_values["topic_attribution"] = topic_policy
            return TenantConfig(
                ecosystem="generic_metrics_only",
                tenant_id=f"{name}-id",
                storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / f'{name}.db'}"),
                plugin_settings=PluginSettingsBase.model_validate(plugin_values),
            )

        settings = AppSettings(
            tenants={
                "enabled": tenant_config("enabled", {"enabled": True, "retention_days": 365}),
                "disabled": tenant_config("disabled", {"enabled": False, "retention_days": 120}),
                "absent": tenant_config("absent", None),
            }
        )
        app = create_app(settings, mode="api")

        with TestClient(app) as client:
            provider = app.state.backend_provider
            for name, config in settings.tenants.items():
                with provider.acquire_backend(name, config) as backend:
                    assert isinstance(backend, SQLModelBackend)
                    with backend.create_unit_of_work() as uow:
                        uow.chargebacks.upsert(
                            _chargeback(
                                _BASELINE_DAY,
                                "principal",
                                "1",
                                tenant_id=config.tenant_id,
                                ecosystem=config.ecosystem,
                            )
                        )
                        uow.chargebacks.upsert(
                            _chargeback(
                                _COMPARISON_DAY,
                                "principal",
                                "2",
                                tenant_id=config.tenant_id,
                                ecosystem=config.ecosystem,
                            )
                        )
                        uow.topic_attributions.upsert_batch(
                            [
                                _topic(
                                    _BASELINE_DAY,
                                    "cluster",
                                    "orders",
                                    "1",
                                    tenant_id=config.tenant_id,
                                    ecosystem=config.ecosystem,
                                ),
                                _topic(
                                    _COMPARISON_DAY,
                                    "cluster",
                                    "orders",
                                    "2",
                                    tenant_id=config.tenant_id,
                                    ecosystem=config.ecosystem,
                                ),
                            ]
                        )
                        for day in (_BASELINE_DAY, _COMPARISON_DAY):
                            uow.pipeline_state.upsert(
                                PipelineState(
                                    ecosystem=config.ecosystem,
                                    tenant_id=config.tenant_id,
                                    tracking_date=day,
                                    chargeback_calculated=True,
                                    calculation_id=f"calculation-{day.isoformat()}",
                                    calculation_completed_at=datetime(day.year, day.month, day.day, 12, tzinfo=UTC),
                                    topic_overlay_gathered=True,
                                    topic_attribution_calculated=True,
                                )
                            )
                        uow.commit()

            for name in settings.tenants:
                params = _comparison_params()
                chargeback_response = client.get(f"/api/v1/tenants/{name}/chargebacks/comparison", params=params)
                topic_response = client.get(f"/api/v1/tenants/{name}/topic-attributions/comparison", params=params)
                assert chargeback_response.status_code == 200
                assert topic_response.status_code == 200
                assert chargeback_response.json()["summary"]["net_change"] == "1"
                assert topic_response.json()["summary"]["net_change"] == "1"

                cutoff = topic_response.json()["baseline"]["coverage"]["availability_cutoff_at"]
                if name == "absent":
                    assert cutoff is None
                else:
                    assert cutoff is not None

    def test_real_provider_confirms_chargeback_zero_and_filtered_topic_zero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from core.api.routes import cost_comparison
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        evaluated_at = datetime(2026, 1, 12, tzinfo=UTC)

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz: object | None = None) -> datetime:
                assert tz is UTC
                return evaluated_at

        monkeypatch.setattr(cost_comparison, "datetime", FixedDateTime)
        plugin_settings = PluginSettingsBase.model_validate(
            {
                "cluster_id": "cluster-production",
                "metrics": {"url": "http://prometheus.invalid"},
                "identity_source": {"source": "static", "static_identities": []},
                "cost_types": [
                    {
                        "name": "KAFKA_BASE",
                        "product_category": "kafka",
                        "rate": "1",
                        "cost_quantity": {"type": "fixed", "count": 1},
                        "allocation_strategy": "even_split",
                    }
                ],
                "topic_attribution": {"enabled": True, "retention_days": 365},
            }
        )
        settings = AppSettings(
            tenants={
                "production": TenantConfig(
                    ecosystem="generic_metrics_only",
                    tenant_id="production-tenant",
                    retention_days=365,
                    storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'zero-comparison.db'}"),
                    plugin_settings=plugin_settings,
                )
            }
        )
        app = create_app(settings, mode="api")

        with TestClient(app) as client:
            provider = app.state.backend_provider
            config = settings.tenants["production"]
            with provider.acquire_backend("production", config) as backend:
                assert isinstance(backend, SQLModelBackend)
                with backend.create_unit_of_work() as uow:
                    uow.topic_attributions.upsert_batch(
                        [
                            _topic(
                                _BASELINE_DAY,
                                "cluster-with-facts",
                                "orders",
                                "1",
                                tenant_id=config.tenant_id,
                                ecosystem=config.ecosystem,
                            ),
                            _topic(
                                _COMPARISON_DAY,
                                "cluster-with-facts",
                                "orders",
                                "2",
                                tenant_id=config.tenant_id,
                                ecosystem=config.ecosystem,
                            ),
                        ]
                    )
                    for day in (_BASELINE_DAY, _COMPARISON_DAY):
                        uow.pipeline_state.upsert(
                            PipelineState(
                                ecosystem=config.ecosystem,
                                tenant_id=config.tenant_id,
                                tracking_date=day,
                                chargeback_calculated=True,
                                calculation_id=f"calculation-{day.isoformat()}",
                                calculation_completed_at=datetime(day.year, day.month, day.day, 12, tzinfo=UTC),
                                topic_overlay_gathered=True,
                                topic_attribution_calculated=True,
                            )
                        )
                    uow.commit()

            params = _comparison_params()
            chargeback_response = client.get(
                "/api/v1/tenants/production/chargebacks/comparison",
                params=params,
            )
            topic_response = client.get(
                "/api/v1/tenants/production/topic-attributions/comparison",
                params={**params, "cluster_resource_id": "cluster-without-facts"},
            )

        assert chargeback_response.status_code == 200
        chargeback_body = chargeback_response.json()
        assert chargeback_body["rows"] == []
        assert chargeback_body["summary"] == {
            "baseline_amount": "0",
            "comparison_amount": "0",
            "increases": "0",
            "decreases": "0",
            "net_change": "0",
            "percentage_change": None,
        }
        assert chargeback_body["baseline"]["coverage"]["status"] == "complete"
        assert chargeback_body["comparison"]["coverage"]["status"] == "complete"

        assert topic_response.status_code == 200
        topic_body = topic_response.json()
        assert topic_body["rows"] == []
        assert topic_body["summary"] == {
            "baseline_amount": "0",
            "comparison_amount": "0",
            "increases": "0",
            "decreases": "0",
            "net_change": "0",
            "percentage_change": None,
        }
        assert topic_body["baseline"]["coverage"]["status"] == "complete"
        assert topic_body["comparison"]["coverage"]["status"] == "complete"

    def test_create_app_provider_constructs_real_backend_and_serves_both_sources(self, tmp_path: Path) -> None:
        from core.metrics.config import MetricsConnectionConfig
        from plugins.confluent_cloud.config import CCloudCredentials, CCloudPluginConfig, TopicAttributionConfig

        database_url = f"sqlite:///{tmp_path / 'comparison.db'}"
        plugin_settings = CCloudPluginConfig(
            ccloud_api=CCloudCredentials(key="test-key", secret="test-secret"),  # type: ignore[arg-type]
            metrics=MetricsConnectionConfig(url="http://prometheus.invalid"),
            topic_attribution=TopicAttributionConfig(enabled=True, retention_days=365),
        )
        settings = AppSettings(
            tenants={
                "production": TenantConfig(
                    ecosystem="confluent_cloud",
                    tenant_id="production-tenant",
                    storage=StorageConfig(connection_string=database_url),
                    plugin_settings=plugin_settings,
                )
            }
        )
        app = create_app(settings, mode="api")

        with TestClient(app) as client:
            provider = app.state.backend_provider
            with provider.acquire_backend("production", settings.tenants["production"]) as backend:  # noqa: SIM117
                with backend.create_unit_of_work() as uow:
                    uow.chargebacks.upsert(
                        _chargeback(
                            _BASELINE_DAY,
                            "alice",
                            "2",
                            tenant_id="production-tenant",
                            ecosystem="confluent_cloud",
                        )
                    )
                    uow.chargebacks.upsert(
                        _chargeback(
                            _COMPARISON_DAY,
                            "alice",
                            "3",
                            tenant_id="production-tenant",
                            ecosystem="confluent_cloud",
                        )
                    )
                    uow.topic_attributions.upsert_batch(
                        [
                            _topic(
                                _BASELINE_DAY,
                                "cluster-a",
                                "orders",
                                "1",
                                tenant_id="production-tenant",
                                ecosystem="confluent_cloud",
                            ),
                            _topic(
                                _COMPARISON_DAY,
                                "cluster-a",
                                "orders",
                                "2",
                                tenant_id="production-tenant",
                                ecosystem="confluent_cloud",
                            ),
                        ]
                    )
                    for day in (_BASELINE_DAY, _COMPARISON_DAY):
                        uow.pipeline_state.upsert(
                            PipelineState(
                                ecosystem="confluent_cloud",
                                tenant_id="production-tenant",
                                tracking_date=day,
                                chargeback_calculated=True,
                                calculation_id=f"calculation-{day.isoformat()}",
                                calculation_completed_at=datetime(day.year, day.month, day.day, 12, tzinfo=UTC),
                                topic_overlay_gathered=True,
                                topic_attribution_calculated=True,
                            )
                        )
                    uow.commit()

            chargeback_response = client.get(
                "/api/v1/tenants/production/chargebacks/comparison",
                params=_comparison_params(group_by="principal"),
            )
            topic_response = client.get(
                "/api/v1/tenants/production/topic-attributions/comparison",
                params=_comparison_params(group_by="topic"),
            )

        assert chargeback_response.status_code == 200
        assert chargeback_response.json()["summary"]["net_change"] == "1"
        assert topic_response.status_code == 200
        assert topic_response.json()["rows"][0]["key"] == "cluster-a:topic:orders"
