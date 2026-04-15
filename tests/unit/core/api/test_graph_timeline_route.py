from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.graph import GraphTimelineData


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


_TIMELINE_PARAMS = {
    "entity_id": "lkc-abc123",
    "start": "2026-04-01",
    "end": "2026-04-13",
}


def _make_timeline_points(start: date, days: int) -> list[GraphTimelineData]:
    return [GraphTimelineData(date=start + timedelta(days=i), cost=Decimal("10.00")) for i in range(days)]


class TestGraphTimelineRouteBasic:
    def test_timeline_returns_points_http_200(self) -> None:
        """V13: GET /graph/timeline returns points array, HTTP 200."""
        mock_uow = MagicMock()
        mock_uow.graph.get_timeline.return_value = _make_timeline_points(date(2026, 4, 1), 13)
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/timeline", params=_TIMELINE_PARAMS)

        assert resp.status_code == 200
        data = resp.json()
        assert "entity_id" in data
        assert "points" in data
        assert data["entity_id"] == "lkc-abc123"

    def test_timeline_returns_one_point_per_day(self) -> None:
        """V13: 13-day range → 13 points returned."""
        mock_uow = MagicMock()
        mock_uow.graph.get_timeline.return_value = _make_timeline_points(date(2026, 4, 1), 13)
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/timeline", params=_TIMELINE_PARAMS)

        data = resp.json()
        assert len(data["points"]) == 13

    def test_timeline_point_has_date_and_cost_fields(self) -> None:
        """V13: each point has date and cost fields."""
        mock_uow = MagicMock()
        mock_uow.graph.get_timeline.return_value = [
            GraphTimelineData(date=date(2026, 4, 1), cost=Decimal("42.50")),
        ]
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/timeline", params=_TIMELINE_PARAMS)

        point = resp.json()["points"][0]
        assert "date" in point
        assert "cost" in point
        assert point["date"] == "2026-04-01"

    def test_timeline_passes_entity_id_to_repo(self) -> None:
        """Route forwards entity_id param to get_timeline."""
        mock_uow = MagicMock()
        mock_uow.graph.get_timeline.return_value = []
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph/timeline", params=_TIMELINE_PARAMS)

        kwargs = mock_uow.graph.get_timeline.call_args.kwargs
        assert kwargs["entity_id"] == "lkc-abc123"

    def test_timeline_passes_ecosystem_and_tenant_id_to_repo(self) -> None:
        """Route forwards ecosystem and tenant_id from TenantConfig."""
        mock_uow = MagicMock()
        mock_uow.graph.get_timeline.return_value = []
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph/timeline", params=_TIMELINE_PARAMS)

        kwargs = mock_uow.graph.get_timeline.call_args.kwargs
        assert kwargs["ecosystem"] == "eco"
        assert kwargs["tenant_id"] == "prod"

    def test_timeline_passes_start_and_end_as_datetimes_to_repo(self) -> None:
        """Route converts date params to UTC datetimes before passing to repo."""
        mock_uow = MagicMock()
        mock_uow.graph.get_timeline.return_value = []
        with _app_with_mock_uow(mock_uow) as client:
            client.get("/api/v1/tenants/prod/graph/timeline", params=_TIMELINE_PARAMS)

        kwargs = mock_uow.graph.get_timeline.call_args.kwargs
        assert kwargs["start"] == datetime(2026, 4, 1, tzinfo=UTC)
        assert kwargs["end"] == datetime(2026, 4, 14, tzinfo=UTC)  # exclusive end (end+1 day)


class TestGraphTimelineRouteGapFilling:
    def test_timeline_gap_filled_days_have_cost_zero(self) -> None:
        """V17: days with no billing data are included with cost=0."""
        start = date(2026, 4, 1)
        # Only 3 days have data; rest are gap-filled with 0
        mock_points = [
            GraphTimelineData(
                date=start + timedelta(days=i), cost=Decimal("0") if i not in {0, 5, 10} else Decimal("50.00")
            )
            for i in range(13)
        ]
        mock_uow = MagicMock()
        mock_uow.graph.get_timeline.return_value = mock_points
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/timeline", params=_TIMELINE_PARAMS)

        points = resp.json()["points"]
        assert len(points) == 13
        # Days 0, 5, 10 have data; others are 0
        zero_points = [p for p in points if p["cost"] == "0"]
        assert len(zero_points) == 10


class TestGraphTimelineRouteErrorHandling:
    def test_timeline_missing_entity_returns_404(self) -> None:
        """V18: KeyError from repo → HTTP 404."""
        mock_uow = MagicMock()
        mock_uow.graph.get_timeline.side_effect = KeyError("nonexistent")
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/timeline", params=_TIMELINE_PARAMS)

        assert resp.status_code == 404
        assert "nonexistent" in resp.json()["detail"]

    def test_timeline_missing_params_returns_422(self) -> None:
        """V20: missing entity_id/start/end → HTTP 422."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/prod/graph/timeline")

        assert resp.status_code == 422

    def test_timeline_unknown_tenant_returns_404(self) -> None:
        """V19: unknown tenant name → 404."""
        mock_uow = MagicMock()
        with _app_with_mock_uow(mock_uow) as client:
            resp = client.get("/api/v1/tenants/nonexistent/graph/timeline", params=_TIMELINE_PARAMS)

        assert resp.status_code == 404
