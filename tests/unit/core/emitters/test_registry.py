from __future__ import annotations

import pytest


def _clear_registry() -> None:
    """Clear the emitter registry between tests."""
    from core.emitters import registry

    registry._REGISTRY.clear()


class TestEmitterRegistryGet:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_get_returns_csv_emitter_instance(self) -> None:
        from core.emitters.registry import get, register
        from emitters.csv_emitter import CsvEmitter, make_csv_emitter

        register("csv", make_csv_emitter)
        result = get("csv", {"output_dir": "/tmp"})
        assert isinstance(result, CsvEmitter)

    def test_get_unknown_type_raises_value_error(self) -> None:
        from core.emitters.registry import get

        with pytest.raises(ValueError, match="unknown_type"):
            get("unknown_type", {})

    def test_get_unknown_type_error_lists_available(self) -> None:
        from core.emitters.registry import get, register
        from emitters.csv_emitter import make_csv_emitter

        register("csv", make_csv_emitter)

        with pytest.raises(ValueError, match="Available: csv"):
            get("unknown_type", {})

    def test_get_unknown_when_empty_mentions_none(self) -> None:
        from core.emitters.registry import get

        with pytest.raises(ValueError):
            get("nonexistent", {})

    def test_register_and_get_custom_emitter(self) -> None:
        from core.emitters.registry import get, register

        class _FakeEmitter:
            def __call__(self, tenant_id: str, date: object, rows: object) -> None:
                pass

        def _fake_factory(**kwargs: object) -> _FakeEmitter:
            return _FakeEmitter()

        register("fake", _fake_factory)
        result = get("fake", {})
        assert isinstance(result, _FakeEmitter)

    def test_register_overwrites_existing(self) -> None:
        from core.emitters.registry import get, register

        call_log: list[str] = []

        class _Emitter1:
            def __call__(self, tenant_id: str, date: object, rows: object) -> None:
                call_log.append("v1")

        class _Emitter2:
            def __call__(self, tenant_id: str, date: object, rows: object) -> None:
                call_log.append("v2")

        register("mytype", lambda **_: _Emitter1())
        register("mytype", lambda **_: _Emitter2())
        result = get("mytype", {})
        assert isinstance(result, _Emitter2)


class TestEmitterProtocolCompliance:
    def setup_method(self) -> None:
        _clear_registry()

    def teardown_method(self) -> None:
        _clear_registry()

    def test_csv_emitter_satisfies_emitter_protocol(self) -> None:
        from core.plugin.protocols import Emitter
        from emitters.csv_emitter import CsvEmitter

        instance = CsvEmitter(output_dir="/tmp")
        assert isinstance(instance, Emitter)

    def test_emitter_protocol_importable(self) -> None:
        from core.plugin.protocols import Emitter

        assert Emitter is not None
