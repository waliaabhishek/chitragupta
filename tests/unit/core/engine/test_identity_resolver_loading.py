from __future__ import annotations

import inspect
import sys
import types

import pytest

from core.engine.orchestrator import _load_identity_resolver


def _make_six_param_resolver():
    """Return a plain function with exactly 6 positional params (matching resolve_identities)."""

    def resolver(tenant_id, resource_id, billing_timestamp, billing_duration, metrics_data, uow):
        pass

    return resolver


class TestLoadIdentityResolverValidPath:
    def test_valid_callable_six_params_loads_successfully(self) -> None:
        """Valid callable with 6 positional params loads and is stored-able in _identity_overrides."""
        fn = _make_six_param_resolver()
        mod = types.ModuleType("_test_ir_valid_mod")
        mod.valid_resolver = fn  # type: ignore[attr-defined]
        sys.modules["_test_ir_valid_mod"] = mod
        try:
            result = _load_identity_resolver("_test_ir_valid_mod:valid_resolver")
            assert callable(result)
            assert result is fn
        finally:
            del sys.modules["_test_ir_valid_mod"]


class TestLoadIdentityResolverErrors:
    def test_bad_module_path_raises_import_error_with_message(self) -> None:
        """Bad module path raises ImportError with 'Could not import module' message."""
        with pytest.raises(ImportError, match="Could not import module"):
            _load_identity_resolver("nonexistent_module_xyz_ir:func")

    def test_missing_attribute_raises_attribute_error_with_message(self) -> None:
        """Valid module, missing attribute raises AttributeError with 'Module.*has no attribute'."""
        # load_protocol_callable wraps with uppercase "Module" — current impl uses lowercase python default
        with pytest.raises(AttributeError, match="Module.*has no attribute"):
            _load_identity_resolver("core.engine.orchestrator:no_such_resolver_xyz")

    def test_wrong_param_count_five_raises_signature_mismatch(self) -> None:
        """Callable with 5 positional params raises TypeError with 'Signature mismatch'."""
        mod = types.ModuleType("_test_ir_five_params_mod")

        def five_params(a, b, c, d, e):
            pass

        mod.five_params = five_params  # type: ignore[attr-defined]
        sys.modules["_test_ir_five_params_mod"] = mod
        try:
            with pytest.raises(TypeError, match="Signature mismatch"):
                _load_identity_resolver("_test_ir_five_params_mod:five_params")
        finally:
            del sys.modules["_test_ir_five_params_mod"]

    def test_wrong_param_count_seven_raises_signature_mismatch(self) -> None:
        """Callable with 7 positional params raises TypeError with 'Signature mismatch'."""
        mod = types.ModuleType("_test_ir_seven_params_mod")

        def seven_params(a, b, c, d, e, f, g):
            pass

        mod.seven_params = seven_params  # type: ignore[attr-defined]
        sys.modules["_test_ir_seven_params_mod"] = mod
        try:
            with pytest.raises(TypeError, match="Signature mismatch"):
                _load_identity_resolver("_test_ir_seven_params_mod:seven_params")
        finally:
            del sys.modules["_test_ir_seven_params_mod"]

    def test_uninstantiated_class_raises_type_error_is_a_class(self) -> None:
        """Uninstantiated class raises TypeError with 'is a class' message."""
        mod = types.ModuleType("_test_ir_bare_class_mod")

        class SomeResolver:
            def __call__(self, tenant_id, resource_id, billing_timestamp, billing_duration, metrics_data, uow):
                pass

        mod.SomeResolver = SomeResolver  # type: ignore[attr-defined]
        sys.modules["_test_ir_bare_class_mod"] = mod
        try:
            with pytest.raises(TypeError, match="is a class"):
                _load_identity_resolver("_test_ir_bare_class_mod:SomeResolver")
        finally:
            del sys.modules["_test_ir_bare_class_mod"]

    def test_non_callable_raises_type_error_does_not_satisfy_protocol(self) -> None:
        """Non-callable attribute raises TypeError with 'does not satisfy protocol'."""
        mod = types.ModuleType("_test_ir_noncallable_mod")
        mod.not_a_function = 42  # type: ignore[attr-defined]
        sys.modules["_test_ir_noncallable_mod"] = mod
        try:
            with pytest.raises(TypeError, match="does not satisfy protocol"):
                _load_identity_resolver("_test_ir_noncallable_mod:not_a_function")
        finally:
            del sys.modules["_test_ir_noncallable_mod"]


class TestLoadIdentityResolverStructural:
    def test_body_does_not_contain_importlib_or_inspect(self) -> None:
        """Body must not use importlib/inspect directly — delegates to load_protocol_callable."""
        import core.engine.orchestrator as orch_mod

        src = inspect.getsource(orch_mod._load_identity_resolver)
        assert "importlib" not in src, (
            "_load_identity_resolver must not use importlib directly; delegate to load_protocol_callable"
        )
        assert "inspect" not in src, (
            "_load_identity_resolver must not use inspect directly; delegate to load_protocol_callable"
        )

    def test_orchestrator_top_level_does_not_import_inspect(self) -> None:
        """'import inspect' must be absent from orchestrator.py top-level imports."""
        import pathlib

        import core.engine.orchestrator as orch_mod

        src = pathlib.Path(orch_mod.__file__).read_text()
        lines = src.splitlines()

        # Collect top-level lines (before first def/class/decorator)
        top_level_lines: list[str] = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith(("def ", "class ", "@")):
                break
            top_level_lines.append(line)

        top_level_src = "\n".join(top_level_lines)
        assert "import inspect" not in top_level_src, (
            "orchestrator.py must not have 'import inspect' at top-level after TASK-019 fix"
        )
