from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

if TYPE_CHECKING:
    import pytest

from core.plugin.loader import discover_plugins


def _make_plugin_dir(base: Path, name: str) -> Path:
    """Create a plugin package directory with empty __init__.py."""
    pkg = base / name
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    return pkg


class TestDiscoverPlugins:
    def test_discovers_valid_plugin(self, tmp_path: Path) -> None:
        pkg = tmp_path / "test_eco"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("def register(): return ('test_eco', lambda: object())")

        results = discover_plugins(tmp_path)

        assert len(results) == 1
        ecosystem_name, factory = results[0]
        assert ecosystem_name == "test_eco"
        assert factory() is not None

    def test_skips_package_without_register(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        pkg = tmp_path / "no_register"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("")

        with caplog.at_level(logging.DEBUG):
            results = discover_plugins(tmp_path)

        assert results == []
        assert "no register()" in caplog.text

    def test_skips_package_with_import_failure(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        _make_plugin_dir(tmp_path, "broken")

        with (
            patch(
                "core.plugin.loader._import_plugin_module",
                side_effect=ImportError("boom"),
            ),
            caplog.at_level(logging.WARNING),
        ):
            results = discover_plugins(tmp_path)

        assert results == []
        assert "Failed to import" in caplog.text

    def test_skips_package_with_register_failure(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        pkg = tmp_path / "bad_register"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("def register(): raise RuntimeError('register boom')")

        with caplog.at_level(logging.WARNING):
            results = discover_plugins(tmp_path)

        assert results == []
        assert "register() failed" in caplog.text

    def test_skips_malformed_register_result(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        pkg = tmp_path / "malformed"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("def register(): return 'not_a_tuple'")

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

    def test_external_plugin_loaded_via_file_import(self, tmp_path: Path) -> None:
        pkg = tmp_path / "myplugin"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("def register(): return ('my_eco', lambda: object())")

        results = discover_plugins(tmp_path)

        assert len(results) == 1
        ecosystem_name, _factory = results[0]
        assert ecosystem_name == "my_eco"

    def test_external_plugin_missing_init_py(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        (tmp_path / "broken_plugin").mkdir()

        with caplog.at_level(logging.WARNING):
            results = discover_plugins(tmp_path)

        assert results == []
        assert "__init__.py" in caplog.text

    def test_import_plugin_module_on_sys_path_uses_import_module(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from core.plugin.loader import _import_plugin_module  # noqa: PLC0415

        plugins_path = tmp_path / "ext_plugins"
        plugins_path.mkdir()
        entry = plugins_path / "myplugin"
        entry.mkdir()

        monkeypatch.syspath_prepend(str(tmp_path))

        mock_module = MagicMock()
        with patch("core.plugin.loader.importlib.import_module", return_value=mock_module) as mock_import:
            _import_plugin_module(entry, plugins_path)

        mock_import.assert_called_once_with("ext_plugins.myplugin")

    def test_import_plugin_module_off_sys_path_uses_file_import(self, tmp_path: Path) -> None:
        from core.plugin.loader import _import_plugin_module  # noqa: PLC0415

        plugins_path = tmp_path / "ext_plugins"
        plugins_path.mkdir()
        entry = plugins_path / "myplugin"
        entry.mkdir()
        (entry / "__init__.py").write_text("SENTINEL = 42")

        with patch("core.plugin.loader.importlib.import_module") as mock_import:
            module = _import_plugin_module(entry, plugins_path)

        mock_import.assert_not_called()
        assert module.SENTINEL == 42  # type: ignore[attr-defined]

    def test_exec_module_failure_rolls_back_sys_modules(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        plugins_path = tmp_path / "ext_plugins"
        plugins_path.mkdir()
        entry = plugins_path / "failplugin"
        entry.mkdir()
        (entry / "__init__.py").write_text("raise RuntimeError('boom')")

        with caplog.at_level(logging.WARNING):
            results = discover_plugins(plugins_path)

        assert "chitragupt_plugin_failplugin" not in sys.modules
        assert results == []
        assert "Failed to import" in caplog.text
