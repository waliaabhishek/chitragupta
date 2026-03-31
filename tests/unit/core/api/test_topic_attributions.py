from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.topic_attribution import (
    TopicAttributionAggregationBucket,
    TopicAttributionAggregationResult,
    TopicAttributionRow,
)


def _make_settings() -> AppSettings:
    return AppSettings(
        api=ApiConfig(host="127.0.0.1", port=8080),
        logging=LoggingConfig(),
        tenants={
            "prod": TenantConfig(
                tenant_id="prod",
                ecosystem="eco",
                storage=StorageConfig(connection_string="sqlite:///:memory:"),
            )
        },
    )


@contextmanager
def _app_with_mock_uow(mock_uow: MagicMock) -> Iterator[TestClient]:
    settings = _make_settings()
    app = create_app(settings)

    def _uow_override() -> Iterator[MagicMock]:
        yield mock_uow

    app.dependency_overrides[get_unit_of_work] = _uow_override
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def _make_ta_row(
    topic_name: str = "orders",
    cluster_resource_id: str = "lkc-abc",
    amount: Decimal = Decimal("10.00"),
) -> TopicAttributionRow:
    return TopicAttributionRow(
        ecosystem="eco",
        tenant_id="prod",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        env_id="env-1",
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_WRITE",
        attribution_method="bytes_ratio",
        amount=amount,
        dimension_id=1,
    )


class TestListTopicAttributionsEndpoint:
    def test_list_returns_200_with_rows(self) -> None:
        """GET /api/v1/tenants/prod/topic-attributions → 200."""
        mock_uow = MagicMock()
        mock_uow.topic_attributions.find_by_filters.return_value = (
            [_make_ta_row(topic_name="orders"), _make_ta_row(topic_name="payments")],
            2,
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/topic-attributions")

        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["items"]) == 2

    def test_list_filters_by_cluster_resource_id(self) -> None:
        """GET /api/v1/tenants/prod/topic-attributions?cluster_resource_id=lkc-abc → only that cluster's rows."""
        mock_uow = MagicMock()
        mock_uow.topic_attributions.find_by_filters.return_value = (
            [_make_ta_row(cluster_resource_id="lkc-abc")],
            1,
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/prod/topic-attributions",
                params={"cluster_resource_id": "lkc-abc"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["items"][0]["cluster_resource_id"] == "lkc-abc"

        # Verify repository was called with correct filter
        call_kwargs = mock_uow.topic_attributions.find_by_filters.call_args.kwargs
        assert call_kwargs.get("cluster_resource_id") == "lkc-abc"

    def test_list_passes_ecosystem_and_tenant_id(self) -> None:
        mock_uow = MagicMock()
        mock_uow.topic_attributions.find_by_filters.return_value = ([], 0)
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/topic-attributions")

        call_kwargs = mock_uow.topic_attributions.find_by_filters.call_args.kwargs
        assert call_kwargs["ecosystem"] == "eco"
        assert call_kwargs["tenant_id"] == "prod"

    def test_list_returns_empty_when_no_rows(self) -> None:
        mock_uow = MagicMock()
        mock_uow.topic_attributions.find_by_filters.return_value = ([], 0)
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/topic-attributions")

        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_list_unknown_tenant_returns_404(self) -> None:
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/nonexistent/topic-attributions")
        assert resp.status_code == 404


class TestAggregateTopicAttributionsEndpoint:
    def test_aggregate_by_topic_name_returns_buckets(self) -> None:
        """GET .../aggregate?group_by=topic_name → one bucket per topic, summed amounts."""
        mock_uow = MagicMock()
        mock_uow.topic_attributions.aggregate.return_value = TopicAttributionAggregationResult(
            buckets=[
                TopicAttributionAggregationBucket(
                    dimensions={"topic_name": "orders"},
                    time_bucket="2026-01-01",
                    total_amount=Decimal("20.00"),
                    row_count=2,
                ),
                TopicAttributionAggregationBucket(
                    dimensions={"topic_name": "payments"},
                    time_bucket="2026-01-01",
                    total_amount=Decimal("10.00"),
                    row_count=2,
                ),
            ],
            total_amount=Decimal("30.00"),
            total_rows=4,
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get(
                "/api/v1/tenants/prod/topic-attributions/aggregate",
                params={"group_by": "topic_name"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["buckets"]) == 2
        assert data["total_amount"] == "20.00" or float(data["total_amount"]) == pytest.approx(30.0, rel=1e-2)

    def test_aggregate_passes_group_by_to_repo(self) -> None:
        mock_uow = MagicMock()
        mock_uow.topic_attributions.aggregate.return_value = TopicAttributionAggregationResult(
            buckets=[], total_amount=Decimal(0), total_rows=0
        )
        with _app_with_mock_uow(mock_uow) as client:
            client.get(
                "/api/v1/tenants/prod/topic-attributions/aggregate",
                params={"group_by": "topic_name"},
            )

        call_kwargs = mock_uow.topic_attributions.aggregate.call_args.kwargs
        assert "topic_name" in call_kwargs.get("group_by", [])

    def test_aggregate_returns_200_empty_result(self) -> None:
        mock_uow = MagicMock()
        mock_uow.topic_attributions.aggregate.return_value = TopicAttributionAggregationResult(
            buckets=[], total_amount=Decimal(0), total_rows=0
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/topic-attributions/aggregate")

        assert resp.status_code == 200
        data = resp.json()
        assert data["buckets"] == []
        assert data["total_rows"] == 0


class TestListTopicAttributionDatesEndpoint:
    def test_dates_returns_200_with_dates(self) -> None:
        """GET /api/v1/tenants/prod/topic-attributions/dates → 200 with date list."""
        from datetime import date

        mock_uow = MagicMock()
        mock_uow.topic_attributions.get_distinct_dates.return_value = [
            date(2026, 1, 1),
            date(2026, 1, 2),
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/topic-attributions/dates")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["dates"]) == 2
        assert data["dates"][0] == "2026-01-01"

    def test_dates_returns_empty_when_no_data(self) -> None:
        mock_uow = MagicMock()
        mock_uow.topic_attributions.get_distinct_dates.return_value = []
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/topic-attributions/dates")

        assert resp.status_code == 200
        assert resp.json()["dates"] == []

    def test_dates_unknown_tenant_returns_404(self) -> None:
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/nonexistent/topic-attributions/dates")
        assert resp.status_code == 404


class TestExportTopicAttributionsEndpoint:
    def test_export_returns_csv_response(self) -> None:
        """POST .../export → CSV content-type, header row + data rows."""
        mock_uow = MagicMock()
        mock_uow.topic_attributions.iter_by_filters.return_value = iter(
            [_make_ta_row(topic_name="orders", amount=Decimal("5.00"))]
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.post("/api/v1/tenants/prod/topic-attributions/export")

        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        content = resp.text
        assert "ecosystem" in content
        assert "orders" in content
        assert "5.00" in content

    def test_export_includes_all_columns(self) -> None:
        mock_uow = MagicMock()
        mock_uow.topic_attributions.iter_by_filters.return_value = iter([_make_ta_row()])
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.post("/api/v1/tenants/prod/topic-attributions/export")

        lines = resp.text.strip().split("\n")
        header = lines[0]
        for col_name in ["ecosystem", "tenant_id", "timestamp", "cluster_resource_id", "topic_name", "amount"]:
            assert col_name in header

    def test_export_empty_result_returns_header_only(self) -> None:
        mock_uow = MagicMock()
        mock_uow.topic_attributions.iter_by_filters.return_value = iter([])
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.post("/api/v1/tenants/prod/topic-attributions/export")

        assert resp.status_code == 200
        lines = [ln for ln in resp.text.strip().split("\n") if ln]
        assert len(lines) == 1  # header only

    def test_export_uses_iter_by_filters_not_find_by_filters(self) -> None:
        """Export endpoint calls iter_by_filters(), never find_by_filters()."""
        mock_uow = MagicMock()
        mock_uow.topic_attributions.iter_by_filters.return_value = iter([_make_ta_row()])
        with _app_with_mock_uow(mock_uow) as client:
            client.post("/api/v1/tenants/prod/topic-attributions/export")

        mock_uow.topic_attributions.iter_by_filters.assert_called_once()
        mock_uow.topic_attributions.find_by_filters.assert_not_called()

    def test_export_passes_ecosystem_and_tenant_to_iter(self) -> None:
        """iter_by_filters is called with ecosystem and tenant_id from path."""
        mock_uow = MagicMock()
        mock_uow.topic_attributions.iter_by_filters.return_value = iter([])
        with _app_with_mock_uow(mock_uow) as client:
            client.post("/api/v1/tenants/prod/topic-attributions/export")

        call_kwargs = mock_uow.topic_attributions.iter_by_filters.call_args.kwargs
        assert call_kwargs["ecosystem"] == "eco"
        assert call_kwargs["tenant_id"] == "prod"

    def test_export_passes_date_range_to_iter(self) -> None:
        """iter_by_filters receives start and end datetimes from query params."""
        mock_uow = MagicMock()
        mock_uow.topic_attributions.iter_by_filters.return_value = iter([])
        with _app_with_mock_uow(mock_uow) as client:
            client.post(
                "/api/v1/tenants/prod/topic-attributions/export",
                params={"start_date": "2026-01-01", "end_date": "2026-01-31"},
            )

        call_kwargs = mock_uow.topic_attributions.iter_by_filters.call_args.kwargs
        assert call_kwargs["start"] == datetime(2026, 1, 1, tzinfo=UTC)
        assert call_kwargs["end"] == datetime(2026, 2, 1, tzinfo=UTC)  # exclusive: midnight of day after end_date

    def test_export_streams_all_rows_from_iterator(self) -> None:
        """All rows yielded by iter_by_filters appear in the CSV output."""
        mock_uow = MagicMock()
        mock_uow.topic_attributions.iter_by_filters.return_value = iter(
            [
                _make_ta_row(topic_name="alpha"),
                _make_ta_row(topic_name="beta"),
                _make_ta_row(topic_name="gamma"),
            ]
        )
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.post("/api/v1/tenants/prod/topic-attributions/export")

        assert resp.status_code == 200
        lines = [ln for ln in resp.text.strip().split("\n") if ln]
        assert len(lines) == 4  # 1 header + 3 data rows
        content = resp.text
        assert "alpha" in content
        assert "beta" in content
        assert "gamma" in content

    def test_export_unknown_tenant_returns_404(self) -> None:
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.post("/api/v1/tenants/nonexistent/topic-attributions/export")
        assert resp.status_code == 404
