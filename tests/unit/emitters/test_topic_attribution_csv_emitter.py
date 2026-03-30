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
    topic_name: str = "orders-events",
    cluster_resource_id: str = "lkc-abc123",
    product_type: str = "KAFKA_NETWORK_WRITE",
    amount: Decimal = Decimal("5.50"),
    attribution_method: str = "bytes_ratio",
) -> Any:
    from core.models.topic_attribution import TopicAttributionRow

    return TopicAttributionRow(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC),
        env_id="env-001",
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_category="KAFKA",
        product_type=product_type,
        attribution_method=attribution_method,
        amount=amount,
    )


class TestTopicAttributionCsvEmitterHeader:
    def test_writes_10_column_header(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        emitter = TopicAttributionCsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), [_make_row()])

        out_file = tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv"
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            fieldnames = list(reader.fieldnames or [])

        assert len(fieldnames) == 10

    def test_header_contains_all_expected_columns(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        emitter = TopicAttributionCsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), [_make_row()])

        out_file = tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv"
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            fieldnames = set(reader.fieldnames or [])

        expected = {
            "ecosystem",
            "tenant_id",
            "timestamp",
            "env_id",
            "cluster_resource_id",
            "topic_name",
            "product_category",
            "product_type",
            "attribution_method",
            "amount",
        }
        assert expected == fieldnames


class TestTopicAttributionCsvEmitterData:
    def test_writes_correct_row_count(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        rows = [_make_row(topic_name=f"topic-{i}") for i in range(3)]
        emitter = TopicAttributionCsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), rows)

        out_file = tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv"
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            data_rows = list(reader)

        assert len(data_rows) == 3

    def test_writes_row_fields_correctly(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        row = _make_row(
            topic_name="orders-events",
            cluster_resource_id="lkc-abc123",
            product_type="KAFKA_NETWORK_WRITE",
            amount=Decimal("7.25"),
            attribution_method="bytes_ratio",
        )
        emitter = TopicAttributionCsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), [row])

        out_file = tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv"
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            data = list(reader)[0]

        assert data["ecosystem"] == ECOSYSTEM
        assert data["tenant_id"] == TENANT_ID
        assert data["env_id"] == "env-001"
        assert data["cluster_resource_id"] == "lkc-abc123"
        assert data["topic_name"] == "orders-events"
        assert data["product_category"] == "KAFKA"
        assert data["product_type"] == "KAFKA_NETWORK_WRITE"
        assert data["attribution_method"] == "bytes_ratio"
        assert data["amount"] == "7.25"

    def test_amount_serialized_as_string(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        emitter = TopicAttributionCsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), [_make_row(amount=Decimal("12.345678"))])

        out_file = tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv"
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            data = list(reader)[0]

        assert data["amount"] == "12.345678"

    def test_empty_rows_writes_header_only(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        emitter = TopicAttributionCsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), [])

        out_file = tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv"
        assert out_file.exists()
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            data_rows = list(reader)

        assert len(data_rows) == 0


class TestTopicAttributionCsvEmitterFileCreation:
    def test_default_filename_template(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        emitter = TopicAttributionCsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 3, 5), [_make_row()])

        expected = tmp_path / f"topic_attr_{TENANT_ID}_2024-03-05.csv"
        assert expected.exists()

    def test_creates_output_dir_if_not_exists(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        nested = tmp_path / "deep" / "nested"
        emitter = TopicAttributionCsvEmitter(output_dir=str(nested))
        emitter(TENANT_ID, date(2024, 1, 15), [_make_row()])

        assert nested.exists()
        assert (nested / f"topic_attr_{TENANT_ID}_2024-01-15.csv").exists()

    def test_output_dir_stored_as_pathlib_path(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        emitter = TopicAttributionCsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), [_make_row()])
        assert (tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv").is_file()

    def test_second_call_overwrites_first(self, tmp_path: Path) -> None:
        from emitters.topic_attribution_csv_emitter import TopicAttributionCsvEmitter

        emitter = TopicAttributionCsvEmitter(output_dir=str(tmp_path))
        emitter(TENANT_ID, date(2024, 1, 15), [_make_row(topic_name="topic-a"), _make_row(topic_name="topic-b")])
        emitter(TENANT_ID, date(2024, 1, 15), [_make_row(topic_name="topic-a")])

        out_file = tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv"
        with out_file.open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            data_rows = list(reader)

        assert len(data_rows) == 1
