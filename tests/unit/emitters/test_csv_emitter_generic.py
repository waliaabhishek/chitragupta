from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

TENANT_ID = "t1"
ECOSYSTEM = "aws"


# ---------------------------------------------------------------------------
# Helpers — row factories
# ---------------------------------------------------------------------------


def _make_chargeback_row(
    *,
    ecosystem: str = ECOSYSTEM,
    resource_id: str | None = None,
    amount: Decimal = Decimal("10.50"),
) -> Any:
    from core.models.chargeback import ChargebackRow, CostType

    return ChargebackRow(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        timestamp=datetime(2024, 1, 15, 0, 0, 0),
        resource_id=resource_id,
        product_category="compute",
        product_type="ec2",
        identity_id="u1",
        cost_type=CostType.USAGE,
        amount=amount,
        allocation_method=None,
        allocation_detail=None,
    )


def _make_ta_row(
    *,
    ecosystem: str = ECOSYSTEM,
    amount: Decimal = Decimal("5.00"),
) -> Any:
    from core.models.topic_attribution import TopicAttributionRow

    return TopicAttributionRow(
        ecosystem=ecosystem,
        tenant_id=TENANT_ID,
        timestamp=datetime(2024, 1, 15, 0, 0, 0),
        env_id="env1",
        cluster_resource_id="lkc-abc",
        topic_name="orders",
        product_category="kafka",
        product_type="throughput",
        attribution_method="proportional",
        amount=amount,
    )


# ---------------------------------------------------------------------------
# Verification 1: CSV behavioral parity — ChargebackRow
# ---------------------------------------------------------------------------


class TestCsvEmitterChargebackParity:
    """Verification test 1 from design doc."""

    def test_chargeback_column_order_exact(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        rows = [_make_chargeback_row()]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)

        content = (tmp_path / "t1_2024-01-15.csv").read_text()
        header_line = content.splitlines()[0]
        assert header_line == (
            "ecosystem,tenant_id,timestamp,resource_id,product_category,"
            "product_type,identity_id,cost_type,amount,allocation_method,allocation_detail"
        )

    def test_chargeback_none_serializes_to_empty_string(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        rows = [_make_chargeback_row(resource_id=None)]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)

        content = (tmp_path / "t1_2024-01-15.csv").read_text()
        # resource_id column should be empty string
        assert "aws,t1,2024-01-15T00:00:00,,compute,ec2,u1,usage,10.50,," in content

    def test_chargeback_strenum_serializes_via_str(self, tmp_path: Path) -> None:
        from core.models.chargeback import CostType
        from emitters.csv_emitter import CsvEmitter

        rows = [_make_chargeback_row()]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)

        with (tmp_path / "t1_2024-01-15.csv").open() as fh:
            data = list(csv.DictReader(fh))

        assert data[0]["cost_type"] == str(CostType.USAGE)

    def test_chargeback_datetime_serializes_to_isoformat(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        rows = [_make_chargeback_row()]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)

        with (tmp_path / "t1_2024-01-15.csv").open() as fh:
            data = list(csv.DictReader(fh))

        assert data[0]["timestamp"] == "2024-01-15T00:00:00"

    def test_chargeback_decimal_serializes_to_str(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        rows = [_make_chargeback_row(amount=Decimal("10.50"))]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)

        with (tmp_path / "t1_2024-01-15.csv").open() as fh:
            data = list(csv.DictReader(fh))

        assert data[0]["amount"] == "10.50"

    def test_chargeback_full_row_content(self, tmp_path: Path) -> None:
        """Verification test 1 verbatim from design doc."""
        from core.models.chargeback import ChargebackRow, CostType
        from emitters.csv_emitter import CsvEmitter

        rows = [
            ChargebackRow(
                ecosystem="aws",
                tenant_id="t1",
                timestamp=datetime(2024, 1, 15, 0, 0, 0),
                resource_id=None,
                product_category="compute",
                product_type="ec2",
                identity_id="u1",
                cost_type=CostType.USAGE,
                amount=Decimal("10.50"),
                allocation_method=None,
                allocation_detail=None,
            )
        ]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter("t1", date(2024, 1, 15), rows)
        content = (tmp_path / "t1_2024-01-15.csv").read_text()
        assert (
            "ecosystem,tenant_id,timestamp,resource_id,product_category,product_type,"
            "identity_id,cost_type,amount,allocation_method,allocation_detail"
        ) in content
        assert "aws,t1,2024-01-15T00:00:00,,compute,ec2,u1,usage,10.50,," in content


# ---------------------------------------------------------------------------
# Verification 2: CSV behavioral parity — TopicAttributionRow
# ---------------------------------------------------------------------------


class TestCsvEmitterTopicAttributionParity:
    """Verification test 2 verbatim from design doc."""

    def test_topic_attribution_with_ta_filename_template(self, tmp_path: Path) -> None:
        from core.models.topic_attribution import TopicAttributionRow
        from emitters.csv_emitter import CsvEmitter

        rows = [
            TopicAttributionRow(
                ecosystem="aws",
                tenant_id="t1",
                timestamp=datetime(2024, 1, 15, 0, 0, 0),
                env_id="env1",
                cluster_resource_id="lkc-abc",
                topic_name="orders",
                product_category="kafka",
                product_type="throughput",
                attribution_method="proportional",
                amount=Decimal("5.00"),
            )
        ]
        emitter = CsvEmitter(
            output_dir=str(tmp_path),
            filename_template="topic_attr_{tenant_id}_{date}.csv",
        )
        emitter("t1", date(2024, 1, 15), rows)
        content = (tmp_path / "topic_attr_t1_2024-01-15.csv").read_text()
        assert (
            "ecosystem,tenant_id,timestamp,env_id,cluster_resource_id,topic_name,"
            "product_category,product_type,attribution_method,amount"
        ) in content
        assert "aws,t1,2024-01-15T00:00:00,env1,lkc-abc,orders,kafka,throughput,proportional,5.00" in content

    def test_ta_column_count_is_ten(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        emitter = CsvEmitter(
            output_dir=str(tmp_path),
            filename_template="topic_attr_{tenant_id}_{date}.csv",
        )
        emitter(TENANT_ID, date(2024, 1, 15), [_make_ta_row()])

        with (tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv").open() as fh:
            reader = csv.DictReader(fh)
            fieldnames = list(reader.fieldnames or [])

        assert len(fieldnames) == 10


# ---------------------------------------------------------------------------
# Verification 4: CSV no-op for emit rows with empty __csv_fields__
# ---------------------------------------------------------------------------


class TestCsvEmitterNoOpForEmitRows:
    """Verification test 4 from design doc."""

    def test_billing_emit_row_produces_no_file(self, tmp_path: Path) -> None:
        from core.emitters.emit_rows import BillingEmitRow
        from emitters.csv_emitter import CsvEmitter

        rows = [
            BillingEmitRow(
                tenant_id=TENANT_ID,
                ecosystem=ECOSYSTEM,
                resource_id="r1",
                product_type="ec2",
                product_category="compute",
                amount=Decimal("20.00"),
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            )
        ]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)
        assert not any(tmp_path.iterdir()), "No file must be written for rows with empty __csv_fields__"

    def test_resource_emit_row_produces_no_file(self, tmp_path: Path) -> None:
        from core.emitters.emit_rows import ResourceEmitRow
        from emitters.csv_emitter import CsvEmitter

        rows = [
            ResourceEmitRow(
                tenant_id=TENANT_ID,
                ecosystem=ECOSYSTEM,
                resource_id="r1",
                resource_type="kafka_cluster",
                amount=Decimal(1),
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            )
        ]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)
        assert not any(tmp_path.iterdir())

    def test_identity_emit_row_produces_no_file(self, tmp_path: Path) -> None:
        from core.emitters.emit_rows import IdentityEmitRow
        from emitters.csv_emitter import CsvEmitter

        rows = [
            IdentityEmitRow(
                tenant_id=TENANT_ID,
                ecosystem=ECOSYSTEM,
                identity_id="u1",
                identity_type="user",
                amount=Decimal(1),
                timestamp=datetime(2024, 1, 15, tzinfo=UTC),
            )
        ]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)
        assert not any(tmp_path.iterdir())


# ---------------------------------------------------------------------------
# Zero-code extensibility: new pipeline row type works without emitter changes
# ---------------------------------------------------------------------------


class _SomeKind(StrEnum):
    WIDGET = "widget"


class TestCsvEmitterZeroCodeExtensibility:
    """Verification test 11 (CSV side): new row type works with CsvEmitter unchanged."""

    def test_new_pipeline_row_type_works_with_csv_emitter(self, tmp_path: Path) -> None:
        from dataclasses import dataclass

        from core.models.emit_descriptors import MetricDescriptor
        from emitters.csv_emitter import CsvEmitter

        @dataclass
        class NewPipelineRow:
            ecosystem: str
            tenant_id: str
            timestamp: datetime
            amount: Decimal
            some_field: str
            __csv_fields__: ClassVar[tuple[str, ...]] = (
                "ecosystem",
                "tenant_id",
                "timestamp",
                "some_field",
                "amount",
            )
            __prometheus_metrics__: ClassVar[tuple[MetricDescriptor, ...]] = (
                MetricDescriptor(
                    name="chitragupta_new_metric",
                    value_field="amount",
                    label_fields=("tenant_id", "ecosystem", "some_field"),
                ),
            )

        rows = [
            NewPipelineRow(
                ecosystem=ECOSYSTEM,
                tenant_id=TENANT_ID,
                timestamp=datetime(2024, 1, 15, 0, 0, 0),
                amount=Decimal("99.99"),
                some_field="widget",
            )
        ]
        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)

        out_file = tmp_path / f"{TENANT_ID}_2024-01-15.csv"
        assert out_file.exists(), "CsvEmitter must write file for new row type without code changes"

        with out_file.open() as fh:
            reader = csv.DictReader(fh)
            data = list(reader)

        assert len(data) == 1
        assert data[0]["ecosystem"] == ECOSYSTEM
        assert data[0]["some_field"] == "widget"
        assert data[0]["amount"] == "99.99"


# ---------------------------------------------------------------------------
# Edge case: empty rows list
# ---------------------------------------------------------------------------


class TestCsvEmitterEdgeCases:
    def test_empty_rows_list_does_not_write_file(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), [])
        assert not any(tmp_path.iterdir()), "Empty rows must not write any file"

    def test_default_filename_template_uses_chargeback_pattern(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 3, 5), [_make_chargeback_row()])
        assert (tmp_path / f"{TENANT_ID}_2024-03-05.csv").exists()

    def test_creates_output_dir_if_not_exists(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        nested = tmp_path / "deep" / "nested"
        emitter = CsvEmitter(output_dir=str(nested))
        emitter(TENANT_ID, date(2024, 1, 15), [_make_chargeback_row()])
        assert nested.exists()

    def test_second_call_overwrites_first(self, tmp_path: Path) -> None:
        from emitters.csv_emitter import CsvEmitter

        emitter = CsvEmitter(output_dir=str(tmp_path))
        emitter(
            TENANT_ID,
            date(2024, 1, 15),
            [_make_chargeback_row(amount=Decimal("1.00")), _make_chargeback_row(amount=Decimal("2.00"))],
        )
        emitter(TENANT_ID, date(2024, 1, 15), [_make_chargeback_row(amount=Decimal("3.00"))])

        with (tmp_path / f"{TENANT_ID}_2024-01-15.csv").open() as fh:
            rows = list(csv.DictReader(fh))

        assert len(rows) == 1, "Second call must overwrite first — idempotent"
