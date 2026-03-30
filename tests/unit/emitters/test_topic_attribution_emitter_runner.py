from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

ECOSYSTEM = "test-eco"
TENANT_ID = "tenant-1"


def _make_ta_row(
    topic_name: str = "orders-events",
    dt: date = date(2024, 1, 15),
    amount: Decimal = Decimal("5.00"),
) -> Any:
    from core.models.topic_attribution import TopicAttributionRow

    return TopicAttributionRow(
        ecosystem=ECOSYSTEM,
        tenant_id=TENANT_ID,
        timestamp=datetime(dt.year, dt.month, dt.day, 0, 0, 0, tzinfo=UTC),
        env_id="env-001",
        cluster_resource_id="lkc-abc123",
        topic_name=topic_name,
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_WRITE",
        attribution_method="bytes_ratio",
        amount=amount,
    )


def _make_emitter_spec(type_: str, **params: Any) -> Any:
    from core.config.models import EmitterSpec

    return EmitterSpec(type=type_, name=type_, params=params)


def _make_mock_storage(dates: list[date], rows_per_date: list[Any] | None = None) -> MagicMock:
    storage = MagicMock()
    uow = MagicMock()

    uow.topic_attributions.get_distinct_dates.return_value = dates
    if rows_per_date is not None:
        uow.topic_attributions.find_by_date.return_value = rows_per_date
    else:
        uow.topic_attributions.find_by_date.return_value = []

    storage.create_read_only_unit_of_work.return_value.__enter__ = MagicMock(return_value=uow)
    storage.create_read_only_unit_of_work.return_value.__exit__ = MagicMock(return_value=False)
    return storage, uow


class TestTopicAttributionEmitterRunnerReadsConfig:
    def test_runner_accepts_emitter_specs_from_config(self, tmp_path: Any) -> None:
        from workflow_runner import TopicAttributionEmitterRunner

        dates = [date(2024, 1, 15)]
        rows = [_make_ta_row()]
        storage, uow = _make_mock_storage(dates, rows)
        specs = [_make_emitter_spec("csv", output_dir=str(tmp_path))]
        runner = TopicAttributionEmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=specs,
        )
        runner.run(TENANT_ID)
        assert (tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv").exists()

    def test_runner_queries_distinct_dates(self) -> None:
        from workflow_runner import TopicAttributionEmitterRunner

        storage, uow = _make_mock_storage([date(2024, 1, 15)])
        specs = [_make_emitter_spec("csv", output_dir="/tmp/ta")]

        with patch("emitters.topic_attribution_csv_emitter.TopicAttributionCsvEmitter.__call__"):
            runner = TopicAttributionEmitterRunner(
                ecosystem=ECOSYSTEM,
                storage_backend=storage,
                emitter_specs=specs,
            )
            runner.run(TENANT_ID)

        uow.topic_attributions.get_distinct_dates.assert_called_once_with(ECOSYSTEM, TENANT_ID)

    def test_runner_no_specs_does_not_query(self) -> None:
        from workflow_runner import TopicAttributionEmitterRunner

        storage, uow = _make_mock_storage([date(2024, 1, 15)])
        runner = TopicAttributionEmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[],
        )
        runner.run(TENANT_ID)

        # With no specs, get_distinct_dates is still called (dates fetched before spec loop)
        # but no find_by_date calls happen
        uow.topic_attributions.find_by_date.assert_not_called()


class TestTopicAttributionEmitterRunnerCsvDispatch:
    def test_dispatches_to_csv_emitter_for_each_date(self, tmp_path: Any) -> None:
        from workflow_runner import TopicAttributionEmitterRunner

        dates = [date(2024, 1, 15), date(2024, 1, 16)]
        rows = [_make_ta_row()]
        storage, uow = _make_mock_storage(dates, rows)
        uow.topic_attributions.find_by_date.return_value = rows

        spec = _make_emitter_spec("csv", output_dir=str(tmp_path))
        runner = TopicAttributionEmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[spec],
        )
        runner.run(TENANT_ID)

        # find_by_date called once per date
        assert uow.topic_attributions.find_by_date.call_count == 2

    def test_csv_emitter_creates_files_for_each_date(self, tmp_path: Any) -> None:
        from workflow_runner import TopicAttributionEmitterRunner

        dates = [date(2024, 1, 15), date(2024, 1, 16)]
        rows = [_make_ta_row()]
        storage, uow = _make_mock_storage(dates, rows)
        uow.topic_attributions.find_by_date.return_value = rows

        spec = _make_emitter_spec("csv", output_dir=str(tmp_path))
        runner = TopicAttributionEmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[spec],
        )
        runner.run(TENANT_ID)

        assert (tmp_path / f"topic_attr_{TENANT_ID}_2024-01-15.csv").exists()
        assert (tmp_path / f"topic_attr_{TENANT_ID}_2024-01-16.csv").exists()

    def test_no_dates_skips_csv_emission(self, tmp_path: Any) -> None:
        from workflow_runner import TopicAttributionEmitterRunner

        storage, uow = _make_mock_storage([])
        spec = _make_emitter_spec("csv", output_dir=str(tmp_path))
        runner = TopicAttributionEmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[spec],
        )
        runner.run(TENANT_ID)

        uow.topic_attributions.find_by_date.assert_not_called()


class TestTopicAttributionEmitterRunnerPrometheusDispatch:
    def test_dispatches_to_prometheus_emit_topic_attributions(self) -> None:
        from workflow_runner import TopicAttributionEmitterRunner

        dates = [date(2024, 1, 15)]
        rows = [_make_ta_row()]
        storage, uow = _make_mock_storage(dates, rows)
        uow.topic_attributions.find_by_date.return_value = rows

        spec = _make_emitter_spec("prometheus", port=9090)

        mock_emitter = MagicMock()
        with patch("emitters.prometheus_emitter.PrometheusEmitter", return_value=mock_emitter):
            runner = TopicAttributionEmitterRunner(
                ecosystem=ECOSYSTEM,
                storage_backend=storage,
                emitter_specs=[spec],
            )
            runner.run(TENANT_ID)

        mock_emitter.emit_topic_attributions.assert_called_once_with(TENANT_ID, date(2024, 1, 15), rows)

    def test_prometheus_called_once_per_date(self) -> None:
        from workflow_runner import TopicAttributionEmitterRunner

        dates = [date(2024, 1, 15), date(2024, 1, 16)]
        rows = [_make_ta_row()]
        storage, uow = _make_mock_storage(dates, rows)
        uow.topic_attributions.find_by_date.return_value = rows

        spec = _make_emitter_spec("prometheus", port=9090)

        mock_emitter = MagicMock()
        with patch("emitters.prometheus_emitter.PrometheusEmitter", return_value=mock_emitter):
            runner = TopicAttributionEmitterRunner(
                ecosystem=ECOSYSTEM,
                storage_backend=storage,
                emitter_specs=[spec],
            )
            runner.run(TENANT_ID)

        assert mock_emitter.emit_topic_attributions.call_count == 2


class TestTopicAttributionEmitterRunnerUnknownType:
    def test_unknown_emitter_type_is_skipped(self, caplog: Any) -> None:
        import logging

        from workflow_runner import TopicAttributionEmitterRunner

        dates = [date(2024, 1, 15)]
        storage, uow = _make_mock_storage(dates, [])
        spec = _make_emitter_spec("unknown-type")
        runner = TopicAttributionEmitterRunner(
            ecosystem=ECOSYSTEM,
            storage_backend=storage,
            emitter_specs=[spec],
        )

        with caplog.at_level(logging.WARNING):
            runner.run(TENANT_ID)

        assert any("unknown" in r.message.lower() or "unknown-type" in r.message for r in caplog.records)
