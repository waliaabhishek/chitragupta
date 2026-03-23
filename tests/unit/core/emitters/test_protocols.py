from __future__ import annotations

from collections.abc import Sequence
from datetime import date


class TestLifecycleEmitterProtocol:
    def test_lifecycle_emitter_importable(self) -> None:
        from core.emitters.protocols import LifecycleEmitter

        assert LifecycleEmitter is not None

    def test_lifecycle_emitter_is_runtime_checkable(self) -> None:
        from core.emitters.protocols import LifecycleEmitter

        class _Impl:
            def open(self, tenant_id: str, manifest: object) -> None: ...

            def emit(self, tenant_id: str, dt: date, rows: Sequence[object]) -> None: ...

            def close(self, tenant_id: str) -> object: ...

        assert isinstance(_Impl(), LifecycleEmitter)

    def test_incomplete_class_not_lifecycle_emitter(self) -> None:
        from core.emitters.protocols import LifecycleEmitter

        class _Incomplete:
            def open(self, tenant_id: str, manifest: object) -> None: ...

        assert not isinstance(_Incomplete(), LifecycleEmitter)

    def test_missing_all_methods_not_lifecycle_emitter(self) -> None:
        from core.emitters.protocols import LifecycleEmitter

        class _Empty:
            pass

        assert not isinstance(_Empty(), LifecycleEmitter)


class TestExpositionEmitterProtocol:
    def test_exposition_emitter_importable(self) -> None:
        from core.emitters.protocols import ExpositionEmitter

        assert ExpositionEmitter is not None

    def test_exposition_emitter_is_runtime_checkable(self) -> None:
        from core.emitters.protocols import ExpositionEmitter

        class _Impl:
            def load(self, tenant_id: str, manifest: object, rows: object) -> None: ...

            def get_consumed(self, tenant_id: str) -> set[date]:
                return set()

        assert isinstance(_Impl(), ExpositionEmitter)

    def test_missing_get_consumed_not_exposition_emitter(self) -> None:
        from core.emitters.protocols import ExpositionEmitter

        class _Incomplete:
            def load(self, tenant_id: str, manifest: object, rows: object) -> None: ...

        assert not isinstance(_Incomplete(), ExpositionEmitter)

    def test_missing_load_not_exposition_emitter(self) -> None:
        from core.emitters.protocols import ExpositionEmitter

        class _Incomplete:
            def get_consumed(self, tenant_id: str) -> set[date]:
                return set()

        assert not isinstance(_Incomplete(), ExpositionEmitter)


class TestRowProvider:
    def test_row_provider_importable(self) -> None:
        from core.emitters.protocols import RowProvider

        assert RowProvider is not None
