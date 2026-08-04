from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.dependencies import get_unit_of_work
from core.config.models import ApiConfig, AppSettings, LoggingConfig, StorageConfig, TenantConfig
from core.models.chargeback import AggregationRow


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
    app = create_app(_make_settings())

    def _uow_override() -> Iterator[MagicMock]:
        yield mock_uow

    app.dependency_overrides[get_unit_of_work] = _uow_override
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


def test_aggregate_chargebacks_logs_request_window_grouping_in_message_prefix_and_canonical_suffix(
    caplog,
) -> None:
    mock_uow = MagicMock()
    mock_uow.chargebacks.aggregate.return_value = [
        AggregationRow(
            dimensions={"identity_id": "user-1"},
            time_bucket="2026-02-15",
            total_amount=Decimal("30.00"),
            usage_amount=Decimal("20.00"),
            shared_amount=Decimal("10.00"),
            row_count=2,
        )
    ]

    with _app_with_mock_uow(mock_uow) as client, caplog.at_level(logging.DEBUG, logger="core.api.routes.aggregation"):
        response = client.get(
            "/api/v1/tenants/prod/chargebacks/aggregate",
            params={
                "group_by": "identity_id",
                "time_bucket": "day",
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
            },
        )

    assert response.status_code == 200

    route_messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "core.api.routes.aggregation"
        and record.getMessage().startswith(("aggregation_requested", "aggregation_completed"))
    ]
    assert len(route_messages) == 2
    requested_message, completed_message = route_messages

    for message, outcome in (
        (requested_message, "started"),
        (completed_message, "completed"),
    ):
        suffix_start = message.index(" tenant_id=")
        prefix = message[:suffix_start]
        suffix = message[suffix_start:]

        assert "start_date=2026-02-01 00:00:00+00:00" in prefix
        assert "end_date=2026-03-01 00:00:00+00:00" in prefix
        assert "group_by=identity_id" in prefix
        assert "time_bucket=day" in prefix
        assert re.search(
            " tenant_id=prod ecosystem=eco request_id=[^ ]+"
            rf" stage=aggregation operation=aggregate_chargebacks outcome={outcome}$",
            suffix,
        )
