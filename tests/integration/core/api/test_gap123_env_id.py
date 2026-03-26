from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient  # noqa: TC002
from sqlmodel import Session  # noqa: TC002

from core.models.chargeback import AllocationDetail, CostType
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable, ChargebackFactTable
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001

_TS = datetime(2026, 3, 10, tzinfo=UTC)


def _insert_dimension_with_env(
    session: Session,
    *,
    env_id: str = "",
    ecosystem: str = "test-eco",
    tenant_id: str = "test-tenant",
    resource_id: str | None = "r1",
    product_type: str = "kafka",
    identity_id: str = "sa-1",
    cost_type: str = CostType.USAGE.value,
    allocation_detail: str | None,
    timestamp: datetime = _TS,
    amount: str = "10.00",
) -> int:
    """Insert a dimension+fact row with explicit env_id; returns dimension_id."""
    dim = ChargebackDimensionTable(
        ecosystem=ecosystem,
        env_id=env_id,
        tenant_id=tenant_id,
        resource_id=resource_id,
        product_category="compute",
        product_type=product_type,
        identity_id=identity_id,
        cost_type=cost_type,
        allocation_method="direct",
        allocation_detail=allocation_detail,
    )
    session.add(dim)
    session.flush()
    fact = ChargebackFactTable(
        dimension_id=dim.dimension_id,
        timestamp=timestamp,
        amount=amount,
        tags_json="[]",
    )
    session.add(fact)
    session.flush()
    return dim.dimension_id  # type: ignore[return-value]


class TestGetDimensionEnvId:
    def test_get_dimension_returns_env_id(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """GET /chargebacks/{dim_id} includes env_id field from the stored dimension."""
        with in_memory_backend.create_unit_of_work() as uow:
            dim_id = _insert_dimension_with_env(
                uow._session,  # type: ignore[attr-defined]
                env_id="env-test",
                allocation_detail=AllocationDetail.USAGE_RATIO_ALLOCATION.value,
            )
            uow.commit()

        response = app_with_backend.get(f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}")
        assert response.status_code == 200
        assert response.json()["env_id"] == "env-test"

    def test_get_dimension_env_id_after_entity_tag_add(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """GET /chargebacks/{dim_id} still includes env_id after entity tags are added to the resource."""
        with in_memory_backend.create_unit_of_work() as uow:
            dim_id = _insert_dimension_with_env(
                uow._session,  # type: ignore[attr-defined]
                env_id="env-test",
                allocation_detail=AllocationDetail.USAGE_RATIO_ALLOCATION.value,
            )
            uow.commit()

        response = app_with_backend.get(
            f"/api/v1/tenants/test-tenant/chargebacks/{dim_id}",
        )
        assert response.status_code == 200
        assert response.json()["env_id"] == "env-test"


class TestAllocationIssuesEnvId:
    def test_allocation_issues_distinct_env_ids_not_collapsed(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Two dimensions with identical keys but different env_id must produce two rows."""
        shared_kwargs = {
            "resource_id": "r-shared",
            "product_type": "kafka",
            "identity_id": "sa-1",
            "allocation_detail": AllocationDetail.NO_IDENTITIES_LOCATED.value,
        }
        with in_memory_backend.create_unit_of_work() as uow:
            _insert_dimension_with_env(
                uow._session,  # type: ignore[attr-defined]
                env_id="env-alpha",
                **shared_kwargs,
            )
            _insert_dimension_with_env(
                uow._session,  # type: ignore[attr-defined]
                env_id="env-beta",
                timestamp=datetime(2026, 3, 11, tzinfo=UTC),
                **shared_kwargs,
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/allocation-issues")
        assert response.status_code == 200
        items = response.json()["items"]
        assert len(items) == 2
        env_ids = {item["env_id"] for item in items}
        assert env_ids == {"env-alpha", "env-beta"}

    def test_allocation_issues_response_contains_env_id_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        """Every allocation issue item in the response has an env_id key."""
        with in_memory_backend.create_unit_of_work() as uow:
            _insert_dimension_with_env(
                uow._session,  # type: ignore[attr-defined]
                env_id="env-check",
                allocation_detail=AllocationDetail.NO_IDENTITIES_LOCATED.value,
            )
            uow.commit()

        response = app_with_backend.get("/api/v1/tenants/test-tenant/chargebacks/allocation-issues")
        assert response.status_code == 200
        items = response.json()["items"]
        assert len(items) == 1
        assert "env_id" in items[0]
        assert items[0]["env_id"] == "env-check"
