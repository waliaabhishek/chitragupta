from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.chargeback import ChargebackRow, CostType
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001


def _seed_chargebacks(backend: SQLModelBackend) -> None:
    """Insert multiple chargeback rows for aggregation tests."""
    rows = [
        # (identity_id, product_type, resource_id, cost_type, amount)
        ("user-1", "kafka", "r-0", CostType.USAGE, Decimal("10.00")),
        ("user-1", "kafka", "r-1", CostType.USAGE, Decimal("10.00")),
        ("user-2", "connect", "r-2", CostType.SHARED, Decimal("10.00")),
    ]
    with backend.create_unit_of_work() as uow:
        for i, (uid, ptype, rid, ctype, amount) in enumerate(rows):
            uow.chargebacks.upsert(
                ChargebackRow(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    timestamp=datetime(2026, 2, 15, i, tzinfo=UTC),
                    resource_id=rid,
                    product_category="compute",
                    product_type=ptype,
                    identity_id=uid,
                    cost_type=ctype,
                    amount=amount,
                    allocation_method="direct",
                    allocation_detail=None,
                    tags=[],
                    metadata={},
                )
            )
        uow.commit()


class TestAggregateChargebacks:
    def test_aggregate_empty(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={"group_by": "identity_id"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["buckets"] == []
        assert data["total_amount"] == "0"
        assert data["total_rows"] == 0

    def test_aggregate_by_identity(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": "identity_id",
                "time_bucket": "day",
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
            },
        )
        assert response.status_code == 200
        data = response.json()
        # 2 identities × 1 day = 2 buckets; total_rows = 3 (2 for user-1, 1 for user-2)
        assert data["total_rows"] == 3
        buckets = data["buckets"]
        # Check dimensions dict structure
        for b in buckets:
            assert "identity_id" in b["dimensions"]
        keys = {b["dimensions"]["identity_id"] for b in buckets}
        assert "user-1" in keys
        assert "user-2" in keys

    def test_aggregate_by_multiple_dimensions(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": ["identity_id", "product_type"],
                "time_bucket": "day",
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
            },
        )
        assert response.status_code == 200
        data = response.json()
        for b in data["buckets"]:
            assert "identity_id" in b["dimensions"]
            assert "product_type" in b["dimensions"]

    def test_aggregate_invalid_group_by(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={"group_by": "invalid_column"},
        )
        assert response.status_code == 400
        assert "group_by" in response.json()["detail"]

    def test_aggregate_invalid_time_bucket(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={"group_by": "identity_id", "time_bucket": "year"},
        )
        assert response.status_code == 400
        assert "time_bucket" in response.json()["detail"]

    def test_aggregate_invalid_date_range(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": "identity_id",
                "start_date": "2026-02-28",
                "end_date": "2026-02-01",
            },
        )
        assert response.status_code == 400

    def test_aggregate_nonexistent_tenant(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/no-such-tenant/chargebacks/aggregate",
            params={"group_by": "identity_id"},
        )
        assert response.status_code == 404

    def test_aggregate_by_product_type(self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": "product_type",
                "time_bucket": "month",
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
            },
        )
        assert response.status_code == 200
        data = response.json()
        # 2 product types × 1 month bucket = 2 buckets; total_rows = 3
        assert data["total_rows"] == 3
        product_types = {b["dimensions"]["product_type"] for b in data["buckets"]}
        assert "kafka" in product_types

    def test_aggregate_total_amount_sums(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={
                "group_by": "product_type",
                "time_bucket": "day",
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
            },
        )
        assert response.status_code == 200
        data = response.json()
        # 3 rows of 10.00 each
        assert Decimal(data["total_amount"]) == Decimal("30.0")
        assert data["total_rows"] == 3


class TestAggregateCostTypeSplit:
    _COMMON_PARAMS = {
        "group_by": "identity_id",
        "time_bucket": "day",
        "start_date": "2026-02-01",
        "end_date": "2026-02-28",
    }

    def _seed_mixed(self, backend: SQLModelBackend) -> None:
        """Seed two rows per bucket: one usage, one shared."""
        rows = [
            ("user-a", "kafka", "r-a0", CostType.USAGE, Decimal("15.00")),
            ("user-a", "kafka", "r-a1", CostType.SHARED, Decimal("5.00")),
            ("user-b", "connect", "r-b0", CostType.USAGE, Decimal("8.00")),
            ("user-b", "connect", "r-b1", CostType.SHARED, Decimal("2.00")),
        ]
        with backend.create_unit_of_work() as uow:
            for i, (uid, ptype, rid, ctype, amount) in enumerate(rows):
                uow.chargebacks.upsert(
                    ChargebackRow(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=datetime(2026, 2, 15, i, tzinfo=UTC),
                        resource_id=rid,
                        product_category="compute",
                        product_type=ptype,
                        identity_id=uid,
                        cost_type=ctype,
                        amount=amount,
                        allocation_method="direct",
                        allocation_detail=None,
                        tags=[],
                        metadata={},
                    )
                )
            uow.commit()

    def _seed_usage_only(self, backend: SQLModelBackend) -> None:
        """Seed only usage rows, no shared."""
        rows = [
            ("user-c", "kafka", "r-c0", CostType.USAGE, Decimal("12.00")),
            ("user-c", "kafka", "r-c1", CostType.USAGE, Decimal("8.00")),
        ]
        with backend.create_unit_of_work() as uow:
            for i, (uid, ptype, rid, ctype, amount) in enumerate(rows):
                uow.chargebacks.upsert(
                    ChargebackRow(
                        ecosystem="test-eco",
                        tenant_id="test-tenant",
                        timestamp=datetime(2026, 2, 15, i, tzinfo=UTC),
                        resource_id=rid,
                        product_category="compute",
                        product_type=ptype,
                        identity_id=uid,
                        cost_type=ctype,
                        amount=amount,
                        allocation_method="direct",
                        allocation_detail=None,
                        tags=[],
                        metadata={},
                    )
                )
            uow.commit()

    def test_mixed_cost_types_bucket_amounts_sum_to_total(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        self._seed_mixed(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params=self._COMMON_PARAMS,
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["buckets"]) > 0
        for bucket in data["buckets"]:
            usage = Decimal(bucket["usage_amount"])
            shared = Decimal(bucket["shared_amount"])
            total = Decimal(bucket["total_amount"])
            assert usage + shared == total

    def test_top_level_totals_sum_to_total_amount(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        self._seed_mixed(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params=self._COMMON_PARAMS,
        )
        assert response.status_code == 200
        data = response.json()
        usage = Decimal(data["usage_amount"])
        shared = Decimal(data["shared_amount"])
        total = Decimal(data["total_amount"])
        assert usage + shared == total
        assert usage == Decimal("23.00")
        assert shared == Decimal("7.00")
        assert total == Decimal("30.00")

    def test_usage_only_shared_amount_is_zero(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        self._seed_usage_only(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params=self._COMMON_PARAMS,
        )
        assert response.status_code == 200
        data = response.json()
        assert Decimal(data["shared_amount"]) == Decimal("0")
        assert Decimal(data["usage_amount"]) == Decimal(data["total_amount"])
        assert Decimal(data["total_amount"]) == Decimal("20.00")

    def test_empty_result_all_amounts_are_zero(self, app_with_backend: TestClient) -> None:
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params=self._COMMON_PARAMS,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["buckets"] == []
        assert Decimal(data["total_amount"]) == Decimal("0")
        assert Decimal(data["usage_amount"]) == Decimal("0")
        assert Decimal(data["shared_amount"]) == Decimal("0")

    def test_backward_compatibility_total_amount_still_present(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params=self._COMMON_PARAMS,
        )
        assert response.status_code == 200
        data = response.json()
        assert "total_amount" in data
        assert Decimal(data["total_amount"]) == Decimal("30.00")
        assert "total_rows" in data
        assert data["total_rows"] == 3
        for bucket in data["buckets"]:
            assert "total_amount" in bucket


class TestAggregateWithFilters:
    _COMMON_PARAMS = {
        "group_by": "identity_id",
        "time_bucket": "day",
        "start_date": "2026-02-01",
        "end_date": "2026-02-28",
    }

    def test_aggregate_with_identity_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "identity_id": "user-1"},
        )
        assert response.status_code == 200
        data = response.json()
        identity_ids = {b["dimensions"]["identity_id"] for b in data["buckets"]}
        assert identity_ids == {"user-1"}
        assert "user-2" not in identity_ids

    def test_aggregate_with_product_type_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "product_type": "connect"},
        )
        assert response.status_code == 200
        data = response.json()
        # Only user-2 row has product_type="connect"
        assert data["total_rows"] == 1
        assert Decimal(data["total_amount"]) == Decimal("10.00")

    def test_aggregate_with_resource_id_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "resource_id": "r-0"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 1
        assert Decimal(data["total_amount"]) == Decimal("10.00")

    def test_aggregate_with_cost_type_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "cost_type": "shared"},
        )
        assert response.status_code == 200
        data = response.json()
        # Only user-2 row has cost_type=SHARED
        assert data["total_rows"] == 1

    def test_aggregate_with_multiple_filters(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "identity_id": "user-1", "product_type": "kafka"},
        )
        assert response.status_code == 200
        data = response.json()
        # user-1 has 2 kafka rows
        assert data["total_rows"] == 2
        assert Decimal(data["total_amount"]) == Decimal("20.00")

    def test_aggregate_with_no_matching_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        _seed_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._COMMON_PARAMS, "identity_id": "nonexistent-user"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["buckets"] == []
        assert data["total_rows"] == 0


def _make_resource(resource_id: str, parent_id: str | None) -> CoreResource:
    return CoreResource(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        resource_id=resource_id,
        resource_type="kafka_cluster",
        display_name=resource_id,
        parent_id=parent_id,
        owner_id=None,
        status=ResourceStatus.ACTIVE,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        deleted_at=None,
        last_seen_at=datetime(2026, 2, 15, tzinfo=UTC),
        metadata={},
    )


def _make_chargeback(
    resource_id: str | None,
    identity_id: str,
    product_type: str,
    cost_type: CostType,
    amount: Decimal,
    hour: int = 0,
    metadata: dict[str, Any] | None = None,
) -> ChargebackRow:
    return ChargebackRow(
        ecosystem="test-eco",
        tenant_id="test-tenant",
        timestamp=datetime(2026, 2, 15, hour, tzinfo=UTC),
        resource_id=resource_id,
        product_category="compute",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=cost_type,
        amount=amount,
        allocation_method="direct",
        allocation_detail=None,
        tags=[],
        metadata=metadata or {},
    )


class TestAggregateByEnvironment:
    _BASE_PARAMS = {
        "time_bucket": "day",
        "start_date": "2026-02-01",
        "end_date": "2026-02-28",
    }

    def _seed_env_chargebacks(self, backend: SQLModelBackend) -> None:
        """Seed two CCloud chargebacks with env_id populated via metadata (production path).

        Deliberately does NOT seed the resources table — env_id must come from
        metadata, not resource.parent_id, to match production behaviour.
        """
        with backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(
                _make_chargeback(
                    "res-env-a",
                    "user-1",
                    "kafka",
                    CostType.USAGE,
                    Decimal("20.00"),
                    hour=0,
                    metadata={"env_id": "env-alpha"},
                )
            )
            uow.chargebacks.upsert(
                _make_chargeback(
                    "res-env-b",
                    "user-2",
                    "connect",
                    CostType.USAGE,
                    Decimal("15.00"),
                    hour=1,
                    metadata={"env_id": "env-beta"},
                )
            )
            uow.commit()

    def test_aggregate_group_by_environment_id_returns_buckets(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """group_by=environment_id returns distinct buckets per environment."""
        self._seed_env_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": "environment_id"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) == 2
        env_ids = {b["dimensions"]["environment_id"] for b in buckets}
        assert "env-alpha" in env_ids
        assert "env-beta" in env_ids

    def test_aggregate_org_wide_cost_has_empty_environment_id(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Chargeback with resource_id=NULL produces environment_id="" (not crash)."""
        with in_memory_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(
                _make_chargeback(None, "user-org", "kafka", CostType.USAGE, Decimal("50.00"), hour=0)
            )
            uow.commit()
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": "environment_id"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) == 1
        assert buckets[0]["dimensions"]["environment_id"] == ""

    def test_aggregate_existing_dimensions_unaffected_by_environment_feature(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """group_by=identity_id and group_by=cost_type still produce correct results (regression)."""
        self._seed_env_chargebacks(in_memory_backend)

        # Test identity_id grouping
        resp_identity = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": "identity_id"},
        )
        assert resp_identity.status_code == 200
        identity_data = resp_identity.json()
        identity_keys = {b["dimensions"]["identity_id"] for b in identity_data["buckets"]}
        assert "user-1" in identity_keys
        assert "user-2" in identity_keys
        assert identity_data["total_rows"] == 2

        # Test cost_type grouping
        resp_cost = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": "cost_type"},
        )
        assert resp_cost.status_code == 200
        cost_data = resp_cost.json()
        cost_keys = {b["dimensions"]["cost_type"] for b in cost_data["buckets"]}
        assert "usage" in cost_keys

    def test_aggregate_invalid_group_by_returns_400(self, app_with_backend: TestClient) -> None:
        """group_by=bad_col → HTTP 400."""
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": "bad_col"},
        )
        assert response.status_code == 400
        assert "group_by" in response.json()["detail"]

    def test_aggregate_multi_dimension_with_environment_id(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """group_by=environment_id&group_by=product_type → buckets have both dimension keys."""
        self._seed_env_chargebacks(in_memory_backend)
        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": ["environment_id", "product_type"]},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) >= 1
        for bucket in buckets:
            assert "environment_id" in bucket["dimensions"]
            assert "product_type" in bucket["dimensions"]
        env_ids = {b["dimensions"]["environment_id"] for b in buckets}
        assert "env-alpha" in env_ids
        assert "env-beta" in env_ids

    def test_aggregate_env_id_from_metadata_no_resources_table_required(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Item 4: aggregate(group_by=["environment_id"]) returns correct env grouping without seeding resources.

        env_id comes from chargeback_dimensions.env_id (written from metadata), not from
        a join to the resources table.
        """
        # Seed chargebacks without seeding resources — verify precondition via resource get
        # (any known ID should return None since nothing is seeded)
        with in_memory_backend.create_unit_of_work() as uow:
            no_resource = uow.resources.get("test-eco", "test-tenant", "res-env-a")
        assert no_resource is None, "Test precondition: resources table must be empty"

        self._seed_env_chargebacks(in_memory_backend)

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": "environment_id"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        assert len(buckets) == 2
        env_ids = {b["dimensions"]["environment_id"] for b in buckets}
        assert "env-alpha" in env_ids
        assert "env-beta" in env_ids

    def test_aggregate_env_id_with_zero_resources_table_rows_returns_non_null(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Item 5: tenant with no resources table rows returns correct non-NULL env grouping.

        This is the production scenario: resources snapshot is partial/stale, but
        chargeback_dimensions.env_id is populated from billing metadata.
        """
        self._seed_env_chargebacks(in_memory_backend)

        response = app_with_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": "environment_id"},
        )
        assert response.status_code == 200
        data = response.json()
        buckets = data["buckets"]
        # Must not return NULL grouping — both env buckets must be non-empty strings
        for bucket in buckets:
            env_val = bucket["dimensions"]["environment_id"]
            assert env_val != "", f"Expected non-empty env_id, got {env_val!r}"
        env_ids = {b["dimensions"]["environment_id"] for b in buckets}
        assert "env-alpha" in env_ids
        assert "env-beta" in env_ids


class TestAggregateByEnvironmentCCloud:
    """Verify environment_id aggregation path with CCloudStorageModule.

    Exercises CCloudChargebackRepository._get_or_create_dimension() end-to-end
    through the HTTP aggregate API, confirming env_id is stored and returned
    as distinct buckets.
    """

    _BASE_PARAMS = {
        "time_bucket": "day",
        "start_date": "2026-02-01",
        "end_date": "2026-02-28",
    }

    def _seed_env_chargebacks(self, backend: SQLModelBackend) -> None:
        with backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(
                _make_chargeback(
                    "res-env-a",
                    "user-1",
                    "kafka",
                    CostType.USAGE,
                    Decimal("20.00"),
                    hour=0,
                    metadata={"env_id": "env-alpha"},
                )
            )
            uow.chargebacks.upsert(
                _make_chargeback(
                    "res-env-b",
                    "user-2",
                    "connect",
                    CostType.USAGE,
                    Decimal("15.00"),
                    hour=1,
                    metadata={"env_id": "env-beta"},
                )
            )
            uow.commit()

    def test_aggregate_group_by_environment_id_returns_distinct_buckets(
        self, app_with_ccloud_backend: TestClient, in_memory_ccloud_backend: SQLModelBackend
    ) -> None:
        """CCloudChargebackRepository stores env_id on dimension; API returns distinct env buckets."""
        self._seed_env_chargebacks(in_memory_ccloud_backend)
        response = app_with_ccloud_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": "environment_id"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 2
        env_ids = {b["dimensions"]["environment_id"] for b in data["buckets"]}
        assert "env-alpha" in env_ids
        assert "env-beta" in env_ids

    def test_two_rows_same_dims_different_env_id_produce_distinct_dimension_rows(
        self, app_with_ccloud_backend: TestClient, in_memory_ccloud_backend: SQLModelBackend
    ) -> None:
        """Same resource/identity/product but different env_id → two dimension rows → two env buckets."""
        with in_memory_ccloud_backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(
                _make_chargeback(
                    "res-shared",
                    "user-x",
                    "kafka",
                    CostType.USAGE,
                    Decimal("10.00"),
                    hour=0,
                    metadata={"env_id": "env-x"},
                )
            )
            uow.chargebacks.upsert(
                _make_chargeback(
                    "res-shared",
                    "user-x",
                    "kafka",
                    CostType.USAGE,
                    Decimal("20.00"),
                    hour=1,
                    metadata={"env_id": "env-y"},
                )
            )
            uow.commit()

        response = app_with_ccloud_backend.get(
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            params={**self._BASE_PARAMS, "group_by": "environment_id"},
        )
        assert response.status_code == 200
        data = response.json()
        env_ids = {b["dimensions"]["environment_id"] for b in data["buckets"]}
        assert "env-x" in env_ids
        assert "env-y" in env_ids
        assert len(data["buckets"]) == 2
