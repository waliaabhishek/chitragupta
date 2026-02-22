from __future__ import annotations

import logging
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import patch

import pytest  # noqa: TC002

from core.plugin.loader import discover_plugins  # noqa: TC001


def _make_plugin_dir(base: Path, name: str) -> Path:
    """Create a plugin package directory with empty __init__.py."""
    pkg = base / name
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    return pkg


class TestDiscoverPlugins:
    def test_discovers_valid_plugin(self, tmp_path: Path) -> None:
        _make_plugin_dir(tmp_path, "test_eco")

        with patch("core.plugin.loader.importlib.import_module") as mock_import:
            mod = ModuleType("plugins.test_eco")
            mock_import.return_value = mod

            stub_plugin: Any = object()
            mod.register = lambda: ("test_eco", lambda: stub_plugin)  # type: ignore[attr-defined]

            results = discover_plugins(tmp_path)

        assert len(results) == 1
        ecosystem_name, factory = results[0]
        assert ecosystem_name == "test_eco"
        assert factory() is stub_plugin

    def test_skips_package_without_register(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        _make_plugin_dir(tmp_path, "no_register")

        with patch("core.plugin.loader.importlib.import_module") as mock_import:
            mod = ModuleType("plugins.no_register")
            mock_import.return_value = mod

            with caplog.at_level(logging.DEBUG):
                results = discover_plugins(tmp_path)

        assert results == []
        assert "no register()" in caplog.text

    def test_skips_package_with_import_failure(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        _make_plugin_dir(tmp_path, "broken")

        with (
            patch(
                "core.plugin.loader.importlib.import_module",
                side_effect=ImportError("boom"),
            ),
            caplog.at_level(logging.WARNING),
        ):
            results = discover_plugins(tmp_path)

        assert results == []
        assert "Failed to import" in caplog.text

    def test_skips_package_with_register_failure(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        _make_plugin_dir(tmp_path, "bad_register")

        with patch("core.plugin.loader.importlib.import_module") as mock_import:
            mod = ModuleType("plugins.bad_register")
            mod.register = lambda: (_ for _ in ()).throw(RuntimeError("register boom"))  # type: ignore[attr-defined]
            mock_import.return_value = mod

            with caplog.at_level(logging.WARNING):
                results = discover_plugins(tmp_path)

        assert results == []
        assert "register() failed" in caplog.text

    def test_skips_malformed_register_result(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        _make_plugin_dir(tmp_path, "malformed")

        with patch("core.plugin.loader.importlib.import_module") as mock_import:
            mod = ModuleType("plugins.malformed")
            mod.register = lambda: "not_a_tuple"  # type: ignore[attr-defined]
            mock_import.return_value = mod

            with caplog.at_level(logging.WARNING):
                results = discover_plugins(tmp_path)

        assert results == []
        assert "malformed result" in caplog.text

    def test_empty_plugins_dir(self, tmp_path: Path) -> None:
        results = discover_plugins(tmp_path)
        assert results == []

    def test_nonexistent_plugins_dir(self, tmp_path: Path) -> None:
        results = discover_plugins(tmp_path / "nonexistent")
        assert results == []

    def test_skips_hidden_and_dunder_dirs(self, tmp_path: Path) -> None:
        (tmp_path / "__pycache__").mkdir()
        (tmp_path / ".hidden").mkdir()
        (tmp_path / "not_a_dir.py").write_text("x = 1")

        results = discover_plugins(tmp_path)
        assert results == []
