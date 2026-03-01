from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import respx
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
        assert item.resource_id == "unresolved_billing_0"

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

    @respx.mock
    def test_gather_single_window(self):
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        respx.get("https://api.confluent.cloud/billing/v1/costs").mock(
            return_value=httpx.Response(
                200,
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
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        config = CCloudPluginConfig.from_plugin_settings({"ccloud_api": {"key": "k", "secret": "s"}})
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

    @respx.mock
    def test_gather_multiple_windows(self):
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        # days_per_query=5, range=12 days → 3 API calls
        billing_response = {
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
        }
        route = respx.get("https://api.confluent.cloud/billing/v1/costs")
        route.side_effect = [
            httpx.Response(200, json=billing_response),
            httpx.Response(200, json=billing_response),
            httpx.Response(200, json=billing_response),
        ]

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
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
        assert len(respx.calls) == 3

    @respx.mock
    def test_gather_empty_range(self):
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        config = CCloudPluginConfig.from_plugin_settings({"ccloud_api": {"key": "k", "secret": "s"}})
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
        assert len(respx.calls) == 0

    @respx.mock
    def test_gather_preserves_malformed_items(self):
        """Malformed billing items should be preserved with malformed metadata, not dropped."""
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        # Use date range within single window (<=15 days)
        respx.get("https://api.confluent.cloud/billing/v1/costs").mock(
            return_value=httpx.Response(
                200,
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
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        config = CCloudPluginConfig.from_plugin_settings({"ccloud_api": {"key": "k", "secret": "s"}})
        cost_input = CCloudBillingCostInput(conn, config)

        items = list(
            cost_input.gather(
                "org-123",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 16, tzinfo=UTC),  # 15 days = single window
                uow=None,
            )
        )

        # Both items preserved — valid item unchanged, malformed item has metadata flag
        assert len(items) == 2
        valid = next(i for i in items if i.resource_id == "lkc-good")
        malformed = next(i for i in items if i.resource_id != "lkc-good")
        assert valid.total_cost == Decimal("10")
        assert malformed.metadata["malformed"] is True
        assert "parse_error" in malformed.metadata


class TestMalformedBillingHandling:
    """Tests for GAP-17: malformed billing items preserved, not dropped."""

    def test_map_malformed_item_sets_malformed_flag(self) -> None:
        """_map_malformed_item sets metadata['malformed']=True and parse_error."""
        from plugins.confluent_cloud.cost_input import _map_malformed_item

        raw = {
            "resource": {"id": "lkc-bad"},
            "product": "KAFKA",
            "line_type": "KAFKA_BASE",
            "amount": 10,
        }
        exc = ValueError("Invalid billing date format 'None': expected YYYY-MM-DD")
        item = _map_malformed_item(raw, "confluent_cloud", "org-123", idx=0, exc=exc)

        assert item.metadata["malformed"] is True
        assert "parse_error" in item.metadata
        assert str(exc) in item.metadata["parse_error"]

    def test_map_malformed_item_resource_id_uses_index(self) -> None:
        """_map_malformed_item resource_id is f'malformed_billing_{idx}'."""
        from plugins.confluent_cloud.cost_input import _map_malformed_item

        raw = {"product": "KAFKA", "line_type": "KAFKA_BASE", "amount": 5}
        exc = ValueError("missing start_date")
        item = _map_malformed_item(raw, "confluent_cloud", "org-123", idx=7, exc=exc)

        assert item.resource_id == "malformed_billing_7"

    def test_map_malformed_item_missing_start_date_uses_epoch(self) -> None:
        """_map_malformed_item uses epoch timestamp when start_date is absent."""
        from plugins.confluent_cloud.cost_input import _map_malformed_item

        raw = {"resource": {}, "amount": 0}
        exc = KeyError("start_date")
        item = _map_malformed_item(raw, "confluent_cloud", "org-123", idx=0, exc=exc)

        assert item.timestamp == datetime(1970, 1, 1, tzinfo=UTC)

    @respx.mock
    def test_two_malformed_rows_same_date_product_distinct_resource_ids(self) -> None:
        """Two malformed rows on same date/product → both in output with distinct resource_ids."""
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        respx.get("https://api.confluent.cloud/billing/v1/costs").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            # Missing start_date → malformed, idx=0
                            "resource": {"id": "lkc-bad-1"},
                            "product": "KAFKA",
                            "line_type": "KAFKA_BASE",
                            "quantity": 1,
                            "price": 10,
                            "amount": 10,
                        },
                        {
                            # Also missing start_date → malformed, idx=1
                            "resource": {"id": "lkc-bad-2"},
                            "product": "KAFKA",
                            "line_type": "KAFKA_BASE",
                            "quantity": 1,
                            "price": 20,
                            "amount": 20,
                        },
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        config = CCloudPluginConfig.from_plugin_settings({"ccloud_api": {"key": "k", "secret": "s"}})
        cost_input = CCloudBillingCostInput(conn, config)

        items = list(
            cost_input.gather(
                "org-123",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 16, tzinfo=UTC),
                uow=None,
            )
        )

        assert len(items) == 2
        resource_ids = {item.resource_id for item in items}
        assert "malformed_billing_0" in resource_ids
        assert "malformed_billing_1" in resource_ids

    @respx.mock
    def test_missing_start_date_preserved_not_dropped(self) -> None:
        """Row with missing start_date is preserved via _map_malformed_item, not dropped."""
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        respx.get("https://api.confluent.cloud/billing/v1/costs").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "start_date": "2024-01-15",
                            "resource": {"id": "lkc-good"},
                            "product": "KAFKA",
                            "line_type": "KAFKA_BASE",
                            "quantity": 1,
                            "price": 50,
                            "amount": 50,
                        },
                        {
                            # Missing start_date → previously dropped, now preserved
                            "resource": {"id": "lkc-no-date"},
                            "product": "KAFKA",
                            "line_type": "KAFKA_BASE",
                            "quantity": 1,
                            "price": 30,
                            "amount": 30,
                        },
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        config = CCloudPluginConfig.from_plugin_settings({"ccloud_api": {"key": "k", "secret": "s"}})
        cost_input = CCloudBillingCostInput(conn, config)

        items = list(
            cost_input.gather(
                "org-123",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 16, tzinfo=UTC),
                uow=None,
            )
        )

        assert len(items) == 2
        resource_ids = {item.resource_id for item in items}
        assert "lkc-good" in resource_ids
        assert any(item.resource_id.startswith("malformed_billing_") for item in items)

    @respx.mock
    def test_aggregate_daily_total_no_loss(self) -> None:
        """Sum of output costs equals sum of all source API amounts (no loss)."""
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        respx.get("https://api.confluent.cloud/billing/v1/costs").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "start_date": "2024-01-15",
                            "resource": {"id": "lkc-valid"},
                            "product": "KAFKA",
                            "line_type": "KAFKA_BASE",
                            "quantity": 1,
                            "price": 100,
                            "amount": 100,
                        },
                        {
                            # Malformed — no start_date; amount must still appear in output
                            "resource": {"id": "lkc-malformed"},
                            "product": "KAFKA",
                            "line_type": "KAFKA_BASE",
                            "quantity": 1,
                            "price": 50,
                            "amount": 50,
                        },
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        config = CCloudPluginConfig.from_plugin_settings({"ccloud_api": {"key": "k", "secret": "s"}})
        cost_input = CCloudBillingCostInput(conn, config)

        items = list(
            cost_input.gather(
                "org-123",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 16, tzinfo=UTC),
                uow=None,
            )
        )

        assert len(items) == 2
        total = sum(item.total_cost for item in items)
        assert total == Decimal("150")

    @respx.mock
    def test_malformed_metadata_flag_set_on_error_rows(self) -> None:
        """Malformed billing items have metadata['malformed']=True and parse_error key."""
        from plugins.confluent_cloud.config import CCloudPluginConfig
        from plugins.confluent_cloud.connections import CCloudConnection
        from plugins.confluent_cloud.cost_input import CCloudBillingCostInput

        respx.get("https://api.confluent.cloud/billing/v1/costs").mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "start_date": "2024-01-15",
                            "resource": {"id": "lkc-good"},
                            "product": "KAFKA",
                            "line_type": "KAFKA_BASE",
                            "quantity": 1,
                            "price": 100,
                            "amount": 100,
                        },
                        {
                            # No start_date → malformed
                            "resource": {"id": "lkc-bad"},
                            "product": "KAFKA",
                            "line_type": "KAFKA_BASE",
                            "quantity": 1,
                            "price": 75,
                            "amount": 75,
                        },
                    ],
                    "metadata": {},
                },
            )
        )

        conn = CCloudConnection(api_key="k", api_secret=SecretStr("s"), request_interval_seconds=0)
        config = CCloudPluginConfig.from_plugin_settings({"ccloud_api": {"key": "k", "secret": "s"}})
        cost_input = CCloudBillingCostInput(conn, config)

        items = list(
            cost_input.gather(
                "org-123",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 16, tzinfo=UTC),
                uow=None,
            )
        )

        assert len(items) == 2
        malformed_items = [item for item in items if item.metadata.get("malformed")]
        assert len(malformed_items) == 1
        assert malformed_items[0].metadata["malformed"] is True
        assert "parse_error" in malformed_items[0].metadata
