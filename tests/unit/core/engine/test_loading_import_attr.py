from __future__ import annotations

import sys
import types

import pytest


class TestImportAttrEmptyPath:
    def test_raises_value_error_for_empty_string(self) -> None:
        from core.engine.loading import import_attr

        with pytest.raises(ValueError, match="must not be empty"):
            import_attr("")


class TestImportAttrMissingColon:
    def test_raises_value_error_for_no_colon(self) -> None:
        from core.engine.loading import import_attr

        with pytest.raises(ValueError, match="module:attribute"):
            import_attr("core.engine.helpers.allocate_to_resource")

    def test_raises_value_error_for_plain_name(self) -> None:
        from core.engine.loading import import_attr

        with pytest.raises(ValueError, match="module:attribute"):
            import_attr("nocotonhere")


class TestImportAttrBadModule:
    def test_raises_import_error_for_nonexistent_module(self) -> None:
        from core.engine.loading import import_attr

        with pytest.raises(ImportError):
            import_attr("nonexistent_module_xyz_abc:some_attr")


class TestImportAttrMissingAttr:
    def test_raises_attribute_error_for_missing_attr(self) -> None:
        from core.engine.loading import import_attr

        with pytest.raises(AttributeError):
            import_attr("core.engine.helpers:no_such_attribute_xyz")


class TestImportAttrHappyPath:
    def test_returns_correct_object(self) -> None:
        from core.engine.loading import import_attr

        obj = import_attr("core.engine.helpers:allocate_evenly")
        from core.engine.helpers import allocate_evenly

        assert obj is allocate_evenly

    def test_returns_attribute_from_dynamic_module(self) -> None:
        from core.engine.loading import import_attr

        mod = types.ModuleType("_test_import_attr_mod")
        mod.my_value = 42  # type: ignore[attr-defined]
        sys.modules["_test_import_attr_mod"] = mod
        try:
            result = import_attr("_test_import_attr_mod:my_value")
            assert result == 42
        finally:
            del sys.modules["_test_import_attr_mod"]


class TestLoadProtocolCallableBackwardsCompat:
    """Verify load_protocol_callable behaviour is unchanged after import_attr refactor."""

    def test_loads_valid_callable(self) -> None:
        from core.engine.loading import load_protocol_callable
        from core.plugin.protocols import CostAllocator

        fn = load_protocol_callable("tests.unit.core.engine.conftest:stub_allocator", CostAllocator)
        assert callable(fn)

    def test_missing_module_raises_import_error(self) -> None:
        from core.engine.loading import load_protocol_callable
        from core.plugin.protocols import CostAllocator

        with pytest.raises(ImportError, match="nonexistent_module_xyz"):
            load_protocol_callable("nonexistent_module_xyz:func", CostAllocator)

    def test_missing_attribute_raises_attribute_error(self) -> None:
        from core.engine.loading import load_protocol_callable
        from core.plugin.protocols import CostAllocator

        with pytest.raises(AttributeError, match="no_such_func_xyz"):
            load_protocol_callable("core.engine.helpers:no_such_func_xyz", CostAllocator)

    def test_no_colon_raises_value_error(self) -> None:
        from core.engine.loading import load_protocol_callable
        from core.plugin.protocols import CostAllocator

        with pytest.raises(ValueError, match="module:attribute"):
            load_protocol_callable("core.engine.helpers.func", CostAllocator)

    def test_empty_string_raises_value_error(self) -> None:
        from core.engine.loading import load_protocol_callable
        from core.plugin.protocols import CostAllocator

        with pytest.raises(ValueError, match="must not be empty"):
            load_protocol_callable("", CostAllocator)
