from __future__ import annotations

import inspect
from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

ECOSYSTEM = "aws"
DATE = date(2024, 1, 15)


@pytest.fixture()
def fake_storage() -> Any:
    uow = MagicMock()
    uow.__enter__ = MagicMock(return_value=uow)
    uow.__exit__ = MagicMock(return_value=False)
    uow.chargebacks.get_distinct_dates.return_value = [DATE]

    storage = MagicMock()
    storage.create_unit_of_work.return_value = uow
    return storage


# ---------------------------------------------------------------------------
# create_auxiliary_prometheus_runners
# ---------------------------------------------------------------------------


class TestCreateAuxiliaryPrometheusRunners:
    """Verification test 9 from design doc."""

    def test_importable(self) -> None:
        from core.emitters.wiring import create_auxiliary_prometheus_runners  # noqa: F401

    def test_returns_three_runners(self, fake_storage: Any) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.sources import ChargebackDateSource
        from core.emitters.wiring import create_auxiliary_prometheus_runners

        prometheus_specs = [EmitterSpec(type="prometheus", params={"port": 8000})]
        runners = create_auxiliary_prometheus_runners(
            ecosystem=ECOSYSTEM,
            storage_backend=fake_storage,
            prometheus_specs=prometheus_specs,
            date_source=ChargebackDateSource(fake_storage),
            resource_types=["kafka_cluster"],
        )
        assert len(runners) == 3

    def test_runners_have_billing_resource_identity_pipelines(self, fake_storage: Any) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.sources import ChargebackDateSource
        from core.emitters.wiring import create_auxiliary_prometheus_runners

        prometheus_specs = [EmitterSpec(type="prometheus", params={"port": 8000})]
        runners = create_auxiliary_prometheus_runners(
            ecosystem=ECOSYSTEM,
            storage_backend=fake_storage,
            prometheus_specs=prometheus_specs,
            date_source=ChargebackDateSource(fake_storage),
            resource_types=["kafka_cluster"],
        )
        pipelines = [r._pipeline for r in runners]
        assert "billing" in pipelines
        assert "resource" in pipelines
        assert "identity" in pipelines

    def test_runners_receive_only_prometheus_specs(self, fake_storage: Any) -> None:
        """Verification test 9 verbatim from design doc."""
        from core.config.models import EmitterSpec
        from core.emitters.sources import ChargebackDateSource
        from core.emitters.wiring import create_auxiliary_prometheus_runners

        prometheus_specs = [EmitterSpec(type="prometheus", params={"port": 8000})]
        runners = create_auxiliary_prometheus_runners(
            ecosystem=ECOSYSTEM,
            storage_backend=fake_storage,
            prometheus_specs=prometheus_specs,
            date_source=ChargebackDateSource(fake_storage),
            resource_types=["kafka_cluster"],
        )
        for runner in runners:
            assert runner._emitter_specs == prometheus_specs

    def test_runners_share_provided_date_source(self, fake_storage: Any) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.sources import ChargebackDateSource
        from core.emitters.wiring import create_auxiliary_prometheus_runners

        prometheus_specs = [EmitterSpec(type="prometheus", params={"port": 8000})]
        date_source = ChargebackDateSource(fake_storage)
        runners = create_auxiliary_prometheus_runners(
            ecosystem=ECOSYSTEM,
            storage_backend=fake_storage,
            prometheus_specs=prometheus_specs,
            date_source=date_source,
            resource_types=["kafka_cluster"],
        )
        for runner in runners:
            assert runner._date_source is date_source

    def test_multiple_prometheus_specs_all_forwarded(self, fake_storage: Any) -> None:
        from core.config.models import EmitterSpec
        from core.emitters.sources import ChargebackDateSource
        from core.emitters.wiring import create_auxiliary_prometheus_runners

        prometheus_specs = [
            EmitterSpec(type="prometheus", params={"port": 8000}),
            EmitterSpec(type="prometheus", params={"port": 8001}),
        ]
        runners = create_auxiliary_prometheus_runners(
            ecosystem=ECOSYSTEM,
            storage_backend=fake_storage,
            prometheus_specs=prometheus_specs,
            date_source=ChargebackDateSource(fake_storage),
            resource_types=["kafka_cluster"],
        )
        for runner in runners:
            assert len(runner._emitter_specs) == 2


# ---------------------------------------------------------------------------
# Verification 10: RegistryEmitterBuilder no longer takes storage_backend
# ---------------------------------------------------------------------------


class TestRegistryEmitterBuilderNoStorageBackend:
    """Verification test 10 from design doc."""

    def test_storage_backend_not_in_init_signature(self) -> None:
        from core.emitters.sources import RegistryEmitterBuilder

        sig = inspect.signature(RegistryEmitterBuilder.__init__)
        assert "storage_backend" not in sig.parameters

    def test_registry_emitter_builder_constructable_without_args(self) -> None:
        from core.emitters.sources import RegistryEmitterBuilder

        # Must not raise — no required constructor args
        builder = RegistryEmitterBuilder()
        assert builder is not None

    def test_build_delegates_to_registry(self) -> None:
        from core.config.models import EmitterSpec
        from core.emitters import registry
        from core.emitters.sources import RegistryEmitterBuilder

        sentinel = object()

        def _factory(**kwargs: Any) -> Any:
            return sentinel

        original_registry = registry._REGISTRY.copy()
        try:
            registry.register("_test_builder_type", _factory)
            builder = RegistryEmitterBuilder()
            spec = EmitterSpec(type="_test_builder_type", params={})
            result = builder.build(spec)
            assert result is sentinel
        finally:
            registry._REGISTRY.clear()
            registry._REGISTRY.update(original_registry)


# ---------------------------------------------------------------------------
# TopicAttributionEmitterBuilder deleted from workflow_runner
# ---------------------------------------------------------------------------


class TestTopicAttributionEmitterBuilderDeleted:
    def test_topic_attribution_emitter_builder_not_importable(self) -> None:
        with pytest.raises((ImportError, AttributeError)):
            from workflow_runner import TopicAttributionEmitterBuilder  # type: ignore[attr-defined]  # noqa: F401
