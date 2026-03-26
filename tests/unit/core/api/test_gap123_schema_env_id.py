from __future__ import annotations

from decimal import Decimal

from core.api.schemas import AllocationIssueResponse, ChargebackDimensionResponse


class TestChargebackDimensionResponseEnvId:
    def test_schema_field_env_id_present(self) -> None:
        response = ChargebackDimensionResponse(
            dimension_id=42,
            ecosystem="ccloud",
            env_id="env-abc",
            tenant_id="t-001",
            resource_id="r-001",
            product_category="KAFKA",
            product_type="KAFKA_NUM_BYTES",
            identity_id="user@example.com",
            cost_type="usage",
            allocation_method="ratio",
            allocation_detail=None,
            tags={},
        )
        assert response.env_id == "env-abc"


class TestAllocationIssueResponseEnvId:
    def test_schema_field_env_id_present(self) -> None:
        response = AllocationIssueResponse(
            ecosystem="ccloud",
            env_id="env-xyz",
            resource_id="r-001",
            product_type="kafka",
            identity_id="sa-1",
            allocation_detail="no_identities_located",
            row_count=3,
            usage_cost=Decimal("15.00"),
            shared_cost=Decimal("5.00"),
            total_cost=Decimal("20.00"),
        )
        assert response.env_id == "env-xyz"
