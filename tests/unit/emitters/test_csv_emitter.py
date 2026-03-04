from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


def _make_row(
    *,
    identity_id: str = "user-1",
    amount: Decimal = Decimal("10.00"),
    allocation_detail: str | None = "even_split",
    resource_id: str | None = "cluster-1",
) -> Any:
    from core.models.chargeback import ChargebackRow, CostType

    return ChargebackRow(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
        resource_id=resource_id,
        product_category="kafka",
        product_type="KAFKA_CKU",
        identity_id=identity_id,
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method="even",
        allocation_detail=allocation_detail,
    )


class TestCsvEmitterEndToEnd:
    def test_writes_header_and_data_rows(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        rows = [
            _make_row(identity_id="user-1", amount=Decimal("10.00")),
            _make_row(identity_id="user-2", amount=Decimal("20.00")),
            _make_row(identity_id="user-3", amount=Decimal("30.00")),
        ]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)

        out_file = tmp_path / f"{TENANT_ID}_2024-01-15.csv"
        assert out_file.exists()

        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            data_rows = list(reader)

        assert len(data_rows) == 3

    def test_csv_has_expected_header_fields(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), [_make_row()])

        out_file = tmp_path / f"{TENANT_ID}_2024-01-15.csv"
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            fieldnames = reader.fieldnames or []

        expected = {
            "ecosystem",
            "tenant_id",
            "timestamp",
            "resource_id",
            "product_category",
            "product_type",
            "identity_id",
            "cost_type",
            "amount",
            "allocation_method",
            "allocation_detail",
        }
        assert expected.issubset(set(fieldnames))

    def test_output_filename_uses_tenant_and_date(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter("my-tenant", date(2024, 3, 5), [_make_row()])

        out_file = tmp_path / "my-tenant_2024-03-05.csv"
        assert out_file.exists()

    def test_creates_output_dir_if_not_exists(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        nested = tmp_path / "deep" / "nested"
        emitter = CsvEmitter(output_dir=str(nested))
        emitter(TENANT_ID, date(2024, 1, 15), [_make_row()])

        assert nested.exists()
        out_file = nested / f"{TENANT_ID}_2024-01-15.csv"
        assert out_file.exists()


class TestCsvEmitterIdempotent:
    def test_second_call_overwrites_first(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        emitter = CsvEmitter(output_dir=str(tmp_path))
        rows_first = [_make_row(identity_id="user-1")]
        rows_second = [_make_row(identity_id="user-1")]

        emitter(TENANT_ID, date(2024, 1, 15), rows_first)
        emitter(TENANT_ID, date(2024, 1, 15), rows_second)

        out_file = tmp_path / f"{TENANT_ID}_2024-01-15.csv"
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            data_rows = list(reader)

        # Second call overwrites — same 1 row, not 2
        assert len(data_rows) == 1

    def test_idempotent_preserves_row_count(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        emitter = CsvEmitter(output_dir=str(tmp_path))
        rows = [_make_row(identity_id=f"user-{i}") for i in range(5)]

        emitter(TENANT_ID, date(2024, 1, 15), rows)
        emitter(TENANT_ID, date(2024, 1, 15), rows)

        out_file = tmp_path / f"{TENANT_ID}_2024-01-15.csv"
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            data_rows = list(reader)

        assert len(data_rows) == 5


class TestCsvEmitterProtocol:
    def test_csv_emitter_is_instance_of_emitter_protocol(self) -> None:
        from core.plugin.protocols import Emitter
        from emitters.csv_emitter import CsvEmitter

        instance = CsvEmitter(output_dir="/tmp")
        assert isinstance(instance, Emitter)


class TestMakeCsvEmitterFactory:
    def test_make_csv_emitter_returns_csv_emitter(self) -> None:
        from emitters.csv_emitter import CsvEmitter, make_csv_emitter

        result = make_csv_emitter(output_dir="/tmp")
        assert isinstance(result, CsvEmitter)
