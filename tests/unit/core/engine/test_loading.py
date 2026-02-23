from __future__ import annotations

import inspect
import sys
import types
from unittest.mock import patch

import pytest

from core.engine.loading import _validate_signature, load_protocol_callable
from core.plugin.protocols import CostAllocator


class TestLoadProtocolCallableValidPath:
    def test_loads_correct_callable(self) -> None:
        fn = load_protocol_callable(
            "core.engine.helpers:allocate_to_resource",
            CostAllocator,
        )
        from core.engine.helpers import allocate_to_resource

        assert fn is allocate_to_resource

    def test_callable_satisfying_protocol(self) -> None:
        fn = load_protocol_callable(
            "core.engine.helpers:allocate_to_resource",
            CostAllocator,
        )
        assert callable(fn)


class TestLoadProtocolCallableErrors:
    def test_missing_module(self) -> None:
        with pytest.raises(ImportError, match="nonexistent_module_xyz"):
            load_protocol_callable("nonexistent_module_xyz:func", CostAllocator)

    def test_missing_attribute(self) -> None:
        with pytest.raises(AttributeError, match="no_such_func_xyz"):
            load_protocol_callable("core.engine.helpers:no_such_func_xyz", CostAllocator)

    def test_malformed_path_no_colon(self) -> None:
        with pytest.raises(ValueError, match="module:attribute"):
            load_protocol_callable("core.engine.helpers.func", CostAllocator)

    def test_empty_string(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            load_protocol_callable("", CostAllocator)

    def test_non_callable_object(self) -> None:
        # _CENT is a Decimal, not callable
        with pytest.raises(TypeError, match="does not satisfy"):
            load_protocol_callable("core.engine.helpers:_CENT", CostAllocator)

    def test_wrong_parameter_count(self) -> None:
        # split_amount_evenly takes 2 params, CostAllocator expects 1
        with pytest.raises(TypeError, match="Signature mismatch"):
            load_protocol_callable(
                "core.engine.helpers:split_amount_evenly",
                CostAllocator,
            )


class TestLoadProtocolCallableEdgeCases:
    def test_callable_with_args_kwargs_passes(self) -> None:
        """A callable accepting *args, **kwargs should pass validation."""
        mod = types.ModuleType("_test_permissive_mod")
        mod.permissive_fn = lambda *args, **kwargs: None  # type: ignore[attr-defined]
        sys.modules["_test_permissive_mod"] = mod
        try:
            fn = load_protocol_callable("_test_permissive_mod:permissive_fn", CostAllocator)
            assert fn is mod.permissive_fn  # type: ignore[attr-defined]
        finally:
            del sys.modules["_test_permissive_mod"]

    def test_class_instance_with_call(self) -> None:
        """A callable instance with correct __call__ signature passes."""
        mod = types.ModuleType("_test_callable_instance_mod")

        class MyAllocator:
            def __call__(self, ctx):
                return None

        mod.my_alloc = MyAllocator()  # type: ignore[attr-defined]
        sys.modules["_test_callable_instance_mod"] = mod
        try:
            fn = load_protocol_callable("_test_callable_instance_mod:my_alloc", CostAllocator)
            assert fn is mod.my_alloc  # type: ignore[attr-defined]
        finally:
            del sys.modules["_test_callable_instance_mod"]

    def test_bare_class_rejected(self) -> None:
        """An uninstantiated class that doesn't satisfy protocol -> TypeError."""
        mod = types.ModuleType("_test_bare_class_mod")

        class NotAnAllocator:
            pass

        mod.NotAnAllocator = NotAnAllocator  # type: ignore[attr-defined]
        sys.modules["_test_bare_class_mod"] = mod
        try:
            with pytest.raises(TypeError, match="class"):
                load_protocol_callable("_test_bare_class_mod:NotAnAllocator", CostAllocator)
        finally:
            del sys.modules["_test_bare_class_mod"]

    def test_non_introspectable_callable(self) -> None:
        """If inspect.signature raises ValueError for the target, we get TypeError."""
        mod = types.ModuleType("_test_nonintrospectable_mod")

        class OpaqueCallable:
            def __call__(self, ctx):
                return None

        instance = OpaqueCallable()
        mod.opaque_fn = instance  # type: ignore[attr-defined]
        sys.modules["_test_nonintrospectable_mod"] = mod
        try:
            original_sig = inspect.signature
            call_count = 0

            def patched_sig(obj, **kwargs):
                nonlocal call_count
                call_count += 1
                # First call is for the protocol's __call__, let it through
                # Second call is for the loaded object's __call__, raise
                if call_count >= 2:
                    raise ValueError("non-introspectable")
                return original_sig(obj, **kwargs)

            with (
                patch("core.engine.loading.inspect.signature", side_effect=patched_sig),
                pytest.raises(TypeError, match="Cannot validate signature"),
            ):
                load_protocol_callable("_test_nonintrospectable_mod:opaque_fn", CostAllocator)
        finally:
            del sys.modules["_test_nonintrospectable_mod"]


class TestValidateSignatureDefensiveBranches:
    """CT-002: Exercise _validate_signature defensive branches."""

    def test_non_callable_protocol_skips_validation(self) -> None:
        """If protocol is not callable, _validate_signature returns early."""
        # An integer is not callable — exercises the `if not callable(protocol)` guard
        _validate_signature(lambda ctx: None, 42)  # type: ignore[arg-type]

    def test_protocol_signature_introspection_failure(self) -> None:
        """If inspect.signature raises on protocol.__call__, skip validation."""

        def patched_sig(obj, **kwargs):
            # Raise on the first call (protocol introspection)
            raise ValueError("cannot introspect")

        with patch("core.engine.loading.inspect.signature", side_effect=patched_sig):
            # Should not raise — returns early from except branch
            _validate_signature(lambda ctx: None, CostAllocator)


class TestInitExports:
    """CT-004: Verify __init__.py exports correct types."""

    def test_exports_resolve_correctly(self) -> None:
        from core.engine import AllocationContext, AllocationResult, AllocatorRegistry, load_protocol_callable

        assert AllocationContext is not None
        assert AllocationResult is not None
        assert AllocatorRegistry is not None
        assert callable(load_protocol_callable)
