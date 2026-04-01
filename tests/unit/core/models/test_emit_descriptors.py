from __future__ import annotations

import dataclasses

import pytest


class TestMetricDescriptorStructure:
    def test_metric_descriptor_importable(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor  # noqa: F401

    def test_metric_descriptor_is_dataclass(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor

        assert dataclasses.is_dataclass(MetricDescriptor)

    def test_metric_descriptor_has_required_fields(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor

        field_names = {f.name for f in dataclasses.fields(MetricDescriptor)}
        assert {"name", "value_field", "label_fields", "documentation", "metric_type"} <= field_names

    def test_metric_descriptor_is_frozen(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor

        d = MetricDescriptor(name="test_metric", value_field="amount", label_fields=("a", "b"))
        with pytest.raises((AttributeError, TypeError)):
            d.name = "other"  # type: ignore[misc]

    def test_metric_descriptor_default_documentation_empty(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor

        d = MetricDescriptor(name="test_metric", value_field="amount", label_fields=())
        assert d.documentation == ""

    def test_metric_descriptor_default_metric_type_gauge(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor

        d = MetricDescriptor(name="test_metric", value_field="amount", label_fields=())
        assert d.metric_type == "gauge"

    def test_label_fields_is_tuple(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor

        d = MetricDescriptor(name="m", value_field="v", label_fields=("a", "b", "c"))
        assert isinstance(d.label_fields, tuple)

    def test_equality(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor

        d1 = MetricDescriptor(name="m", value_field="v", label_fields=("a",))
        d2 = MetricDescriptor(name="m", value_field="v", label_fields=("a",))
        assert d1 == d2

    def test_inequality_on_name(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor

        d1 = MetricDescriptor(name="m1", value_field="v", label_fields=("a",))
        d2 = MetricDescriptor(name="m2", value_field="v", label_fields=("a",))
        assert d1 != d2

    def test_full_construction(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor

        d = MetricDescriptor(
            name="chitragupta_chargeback_amount",
            value_field="amount",
            label_fields=("tenant_id", "ecosystem", "identity_id"),
            documentation="Chargeback cost",
            metric_type="gauge",
        )
        assert d.name == "chitragupta_chargeback_amount"
        assert d.value_field == "amount"
        assert d.label_fields == ("tenant_id", "ecosystem", "identity_id")
        assert d.documentation == "Chargeback cost"
        assert d.metric_type == "gauge"


class TestMetricDescriptorOnRowModels:
    def test_chargeback_row_has_prometheus_metrics_classvar(self) -> None:
        from core.models.chargeback import ChargebackRow
        from core.models.emit_descriptors import MetricDescriptor

        assert hasattr(ChargebackRow, "__prometheus_metrics__")
        assert isinstance(ChargebackRow.__prometheus_metrics__, tuple)
        assert len(ChargebackRow.__prometheus_metrics__) >= 1
        assert all(isinstance(d, MetricDescriptor) for d in ChargebackRow.__prometheus_metrics__)

    def test_chargeback_row_has_csv_fields_classvar(self) -> None:
        from core.models.chargeback import ChargebackRow

        assert hasattr(ChargebackRow, "__csv_fields__")
        assert isinstance(ChargebackRow.__csv_fields__, tuple)
        assert len(ChargebackRow.__csv_fields__) > 0

    def test_chargeback_row_csv_fields_exact_order(self) -> None:
        from core.models.chargeback import ChargebackRow

        expected = (
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
        )
        assert ChargebackRow.__csv_fields__ == expected

    def test_chargeback_prometheus_metric_name(self) -> None:
        from core.models.chargeback import ChargebackRow

        names = [d.name for d in ChargebackRow.__prometheus_metrics__]
        assert "chitragupta_chargeback_amount" in names

    def test_topic_attribution_row_has_prometheus_metrics_classvar(self) -> None:
        from core.models.emit_descriptors import MetricDescriptor
        from core.models.topic_attribution import TopicAttributionRow

        assert hasattr(TopicAttributionRow, "__prometheus_metrics__")
        assert all(isinstance(d, MetricDescriptor) for d in TopicAttributionRow.__prometheus_metrics__)

    def test_topic_attribution_row_has_csv_fields_classvar(self) -> None:
        from core.models.topic_attribution import TopicAttributionRow

        assert hasattr(TopicAttributionRow, "__csv_fields__")
        expected = (
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
        )
        assert TopicAttributionRow.__csv_fields__ == expected

    def test_topic_attribution_prometheus_metric_name(self) -> None:
        from core.models.topic_attribution import TopicAttributionRow

        names = [d.name for d in TopicAttributionRow.__prometheus_metrics__]
        assert "chitragupta_topic_attribution_amount" in names
