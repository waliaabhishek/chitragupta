from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import responses
from pydantic import SecretStr


class TestDateWindowGenerator:
    """Tests for _generate_date_windows()."""

    def test_single_window_within_days_per_query(self):
        from plugins.confluent_cloud.cost_input import _generate_date_windows

        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 10, tzinfo=UTC)
        windows = list(_generate_date_windows(start, end, days_per_query=15))
        assert windows == [(start, end)]

    def test_multiple_windows(self):
        from plugins.confluent_cloud.cost_input import _generate_date_windows

        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 2, 1, tzinfo=UTC)  # 31 days
        windows = list(_generate_date_windows(start, end, days_per_query=15))
        assert len(windows) == 3
        assert windows[0] == (
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 16, tzinfo=UTC),
        )
        assert windows[1] == (
            datetime(2024, 1, 16, tzinfo=UTC),
            datetime(2024, 1, 31, tzinfo=UTC),
        )
        assert windows[2] == (
            datetime(2024, 1, 31, tzinfo=UTC),
            datetime(2024, 2, 1, tzinfo=UTC),
        )

    def test_start_equals_end(self):
        from plugins.confluent_cloud.cost_input import _generate_date_windows

        start = datetime(2024, 1, 1, tzinfo=UTC)
        windows = list(_generate_date_windows(start, start, days_per_query=15))
        assert windows == []

    def test_start_after_end(self):
        from plugins.confluent_cloud.cost_input import _generate_date_windows

        start = datetime(2024, 2, 1, tzinfo=UTC)
        end = datetime(2024, 1, 1, tzinfo=UTC)
        windows = list(_generate_date_windows(start, end, days_per_query=15))
        assert windows == []

    def test_exact_multiple_of_days(self):
        from plugins.confluent_cloud.cost_input import _generate_date_windows

        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 31, tzinfo=UTC)  # exactly 30 days
        windows = list(_generate_date_windows(start, end, days_per_query=15))
        assert len(windows) == 2
        assert windows[0][1] == windows[1][0]  # contiguous
        assert windows[1][1] == end


class TestBillingItemMapping:
    """Tests for _map_billing_item() and helpers."""

    def test_map_billing_item_standard(self):
        from plugins.confluent_cloud.cost_input import _map_billing_item

        raw = {
            "start_date": "2024-01-15",
            "resource": {
                "id": "lkc-abc123",
                "display_name": "my-cluster",
                "environment": {"id": "env-xyz"},
            },
            "product": "KAFKA",
            "line_type": "KAFKA_NUM_CKU",
            "quantity": 2.0,
            "price": 1.50,
            "amount": 3.00,
            "original_amount": 3.50,
        }
        item = _map_billing_item(raw, "confluent_cloud", "org-123")

        assert item.ecosystem == "confluent_cloud"
        assert item.tenant_id == "org-123"
        assert item.timestamp == datetime(2024, 1, 15, tzinfo=UTC)
        assert item.resource_id == "lkc-abc123"
        assert item.product_category == "KAFKA"
        assert item.product_type == "KAFKA_NUM_CKU"
        assert item.quantity == Decimal("2.0")
        assert item.unit_price == Decimal("1.50")
        assert item.total_cost == Decimal("3.00")
        assert item.granularity == "daily"
        assert item.currency == "USD"
        assert item.metadata["original_amount"] == Decimal("3.50")
        assert item.metadata["env_id"] == "env-xyz"
        assert item.metadata["resource_name"] == "my-cluster"

    def test_map_billing_item_missing_resource_id(self):
        from plugins.confluent_cloud.cost_input import _map_billing_item

        raw = {
            "start_date": "2024-01-15",
            "resource": {},
            "product": "KAFKA",
            "line_type": "KAFKA_NUM_CKU",
            "quantity": 0,
            "price": 0,
            "amount": 0,
        }
        item = _map_billing_item(raw, "confluent_cloud", "org-123")
        assert item.resource_id == "unresolved_billing_resource"

    def test_map_billing_item_zero_amount(self):
        from plugins.confluent_cloud.cost_input import _map_billing_item

        raw = {
            "start_date": "2024-01-15",
            "resource": {"id": "lkc-abc"},
            "product": "KAFKA",
            "line_type": "KAFKA_BASE",
            "quantity": 0,
            "price": 0,
            "amount": 0,
        }
        item = _map_billing_item(raw, "confluent_cloud", "org-123")
        assert item.total_cost == Decimal("0")

    def test_map_billing_item_negative_amount(self):
        """Negative amounts (credits/discounts) should be preserved."""
        from plugins.confluent_cloud.cost_input import _map_billing_item

        raw = {
            "start_date": "2024-01-15",
            "resource": {"id": "lkc-abc"},
            "product": "KAFKA",
            "line_type": "KAFKA_CREDIT",
            "quantity": 1,
            "price": -100,
            "amount": -100,
        }
        item = _map_billing_item(raw, "confluent_cloud", "org-123")
        assert item.total_cost == Decimal("-100")

    def test_parse_billing_date(self):
        from plugins.confluent_cloud.cost_input import _parse_billing_date

        result = _parse_billing_date("2024-01-15")
        assert result == datetime(2024, 1, 15, tzinfo=UTC)

    def test_parse_billing_date_invalid(self):
        import pytest
        from plugins.confluent_cloud.cost_input import _parse_billing_date

        with pytest.raises(ValueError, match="Invalid billing date"):
            _parse_billing_date("not-a-date")

    def test_safe_decimal_valid(self):
        from plugins.confluent_cloud.cost_input import _safe_decimal

        assert _safe_decimal(123.45) == Decimal("123.45")
        assert _safe_decimal("123.45") == Decimal("123.45")
        assert _safe_decimal(0) == Decimal("0")

    def test_safe_decimal_none(self):
        from plugins.confluent_cloud.cost_input import _safe_decimal

        assert _safe_decimal(None) == Decimal("0")

    def test_safe_decimal_invalid(self):
        from plugins.confluent_cloud.cost_input import _safe_decimal

        # Invalid values should return 0 and log warning
        assert _safe_decimal("not-a-number") == Decimal("0")


class TestCCloudBillingCostInput:
    """Tests for CCloudBillingCostInput.gather()."""

    @responses.activate
    def test_gather_single_window(self):
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        responses.add(
            responses.GET,
            "https://api.confluent.cloud/billing/v1/costs",
            json={
                "data": [
                    {
                        "start_date": "2024-01-15",
                        "resource": {
                            "id": "lkc-abc",
                            "display_name": "cl",
                            "environment": {"id": "env-1"},
                        },
                        "product": "KAFKA",
                        "line_type": "KAFKA_NUM_CKU",
                        "quantity": 1,
                        "price": 100,
                        "amount": 100,
                    }
                ],
                "metadata": {},
            },
            status=200,
        )

        conn = CCloudConnection(
            api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0
        )
        config = CCloudPluginConfig.from_plugin_settings(
            {"ccloud_api": {"key": "k", "secret": "s"}}
        )
        cost_input = CCloudBillingCostInput(conn, config)

        items = list(
            cost_input.gather(
                "org-123",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 16, tzinfo=UTC),
                uow=None,  # Not used by billing API CostInput
            )
        )

        assert len(items) == 1
        assert items[0].resource_id == "lkc-abc"
        assert items[0].total_cost == Decimal("100")

    @responses.activate
    def test_gather_multiple_windows(self):
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        # days_per_query=5, range=12 days → 3 API calls
        for _ in range(3):
            responses.add(
                responses.GET,
                "https://api.confluent.cloud/billing/v1/costs",
                json={
                    "data": [
                        {
                            "start_date": "2024-01-01",
                            "resource": {"id": "lkc-1"},
                            "product": "KAFKA",
                            "line_type": "KAFKA_BASE",
                            "quantity": 1,
                            "price": 10,
                            "amount": 10,
                        }
                    ],
                    "metadata": {},
                },
                status=200,
            )

        conn = CCloudConnection(
            api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0
        )
        config = CCloudPluginConfig.from_plugin_settings(
            {
                "ccloud_api": {"key": "k", "secret": "s"},
                "billing_api": {"days_per_query": 5},
            }
        )
        cost_input = CCloudBillingCostInput(conn, config)

        items = list(
            cost_input.gather(
                "org-123",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 13, tzinfo=UTC),
                uow=None,
            )
        )

        assert len(items) == 3
        assert len(responses.calls) == 3

    @responses.activate
    def test_gather_empty_range(self):
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        conn = CCloudConnection(
            api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0
        )
        config = CCloudPluginConfig.from_plugin_settings(
            {"ccloud_api": {"key": "k", "secret": "s"}}
        )
        cost_input = CCloudBillingCostInput(conn, config)

        items = list(
            cost_input.gather(
                "org-123",
                datetime(2024, 2, 1, tzinfo=UTC),
                datetime(2024, 1, 1, tzinfo=UTC),  # start > end
                uow=None,
            )
        )

        assert items == []
        assert len(responses.calls) == 0

    @responses.activate
    def test_gather_skips_malformed_items(self):
        """Malformed billing items should be skipped with warning, not fail gather."""
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        # Use date range within single window (<=15 days)
        responses.add(
            responses.GET,
            "https://api.confluent.cloud/billing/v1/costs",
            json={
                "data": [
                    # Valid item
                    {
                        "start_date": "2024-01-15",
                        "resource": {"id": "lkc-good"},
                        "product": "KAFKA",
                        "line_type": "KAFKA_BASE",
                        "quantity": 1,
                        "price": 10,
                        "amount": 10,
                    },
                    # Invalid item - missing start_date
                    {
                        "resource": {"id": "lkc-bad"},
                        "product": "KAFKA",
                        "line_type": "KAFKA_BASE",
                        "quantity": 1,
                        "price": 10,
                        "amount": 10,
                    },
                ],
                "metadata": {},
            },
            status=200,
        )

        conn = CCloudConnection(
            api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0
        )
        config = CCloudPluginConfig.from_plugin_settings(
            {"ccloud_api": {"key": "k", "secret": "s"}}
        )
        cost_input = CCloudBillingCostInput(conn, config)

        items = list(
            cost_input.gather(
                "org-123",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 16, tzinfo=UTC),  # 15 days = single window
                uow=None,
            )
        )

        # Should have 1 valid item, malformed item skipped
        assert len(items) == 1
        assert items[0].resource_id == "lkc-good"
