from __future__ import annotations

import inspect


def test_historical_repair_billing_writer_is_narrow_and_runtime_checkable() -> None:
    from core.storage.interface import HistoricalRepairBillingWriter

    method = HistoricalRepairBillingWriter.replace_for_date
    signature = inspect.signature(method)

    assert getattr(HistoricalRepairBillingWriter, "_is_runtime_protocol", False) is True
    assert list(signature.parameters) == [
        "self",
        "ecosystem",
        "tenant_id",
        "tracking_date",
        "lines",
    ]
    assert signature.parameters["tracking_date"].annotation == "date"
    assert signature.parameters["lines"].annotation == "Sequence[BillingLineItem]"
    assert signature.return_annotation == "int"


def test_generic_billing_repository_is_not_widened_with_historical_mutation() -> None:
    from core.storage.interface import BillingRepository

    assert not hasattr(BillingRepository, "replace_for_date")
