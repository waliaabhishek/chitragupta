from __future__ import annotations

from decimal import Decimal

from core.models.chargeback import AllocationIssueRow


class TestAllocationIssueRowEnvId:
    def test_domain_model_field_env_id_present(self) -> None:
        row = AllocationIssueRow(
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
        assert row.env_id == "env-xyz"
