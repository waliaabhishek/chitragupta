"""Tests for plugin discovery contract (TASK-003).

Both plugin packages must expose register() -> tuple[str, Callable[[], EcosystemPlugin]]
so the loader can discover them without requiring a PluginRegistry argument.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# 1 & 5. self_managed_kafka.register() — zero-arg, returns 2-tuple
# ---------------------------------------------------------------------------


def test_smk_register_returns_tuple() -> None:
    """register() returns a 2-tuple (ecosystem_name, factory)."""
    from plugins.self_managed_kafka import register

    result = register()

    assert isinstance(result, tuple)
    assert len(result) == 2


def test_smk_register_ecosystem_name() -> None:
    """register()[0] == 'self_managed_kafka'."""
    from plugins.self_managed_kafka import register

    name, _ = register()

    assert name == "self_managed_kafka"


def test_smk_register_factory_is_plugin_class() -> None:
    """register()[1] is SelfManagedKafkaPlugin."""
    from plugins.self_managed_kafka import register
    from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

    _, factory = register()

    assert factory is SelfManagedKafkaPlugin


def test_smk_register_no_args_no_typeerror() -> None:
    """register() called with no arguments must not raise TypeError."""
    from plugins.self_managed_kafka import register

    try:
        register()
    except TypeError as exc:
        pytest.fail(f"register() raised TypeError: {exc}")


# ---------------------------------------------------------------------------
# 2 & 5. confluent_cloud.register() — zero-arg, returns 2-tuple
# ---------------------------------------------------------------------------


def test_cc_register_exists() -> None:
    """confluent_cloud package exposes a register() callable."""
    import plugins.confluent_cloud as cc_pkg

    assert callable(getattr(cc_pkg, "register", None)), (
        "plugins.confluent_cloud has no register() function"
    )


def test_cc_register_returns_tuple() -> None:
    """register() returns a 2-tuple (ecosystem_name, factory)."""
    from plugins.confluent_cloud import register

    result = register()

    assert isinstance(result, tuple)
    assert len(result) == 2


def test_cc_register_ecosystem_name() -> None:
    """register()[0] == 'confluent_cloud'."""
    from plugins.confluent_cloud import register

    name, _ = register()

    assert name == "confluent_cloud"


def test_cc_register_factory_is_plugin_class() -> None:
    """register()[1] is ConfluentCloudPlugin."""
    from plugins.confluent_cloud import register
    from plugins.confluent_cloud.plugin import ConfluentCloudPlugin

    _, factory = register()

    assert factory is ConfluentCloudPlugin


def test_cc_register_no_args_no_typeerror() -> None:
    """register() called with no arguments must not raise TypeError."""
    from plugins.confluent_cloud import register

    try:
        register()
    except TypeError as exc:
        pytest.fail(f"register() raised TypeError: {exc}")


# ---------------------------------------------------------------------------
# 4. Factory callables are callable with zero arguments
# ---------------------------------------------------------------------------


def test_smk_factory_is_callable() -> None:
    """Factory returned by smk register() is callable."""
    from plugins.self_managed_kafka import register

    _, factory = register()

    assert callable(factory)


def test_cc_factory_is_callable() -> None:
    """Factory returned by cc register() is callable."""
    from plugins.confluent_cloud import register

    _, factory = register()

    assert callable(factory)


# ---------------------------------------------------------------------------
# 3. discover_plugins finds both plugins from real src/plugins/ dir
# ---------------------------------------------------------------------------

_PLUGINS_PATH = Path(__file__).parents[3] / "src" / "plugins"


def test_discover_plugins_finds_self_managed_kafka() -> None:
    """discover_plugins returns entry with 'self_managed_kafka'."""
    from core.plugin.loader import discover_plugins

    results = discover_plugins(_PLUGINS_PATH)
    names = [name for name, _ in results]

    assert "self_managed_kafka" in names, f"Got: {names}"


def test_discover_plugins_finds_confluent_cloud() -> None:
    """discover_plugins returns entry with 'confluent_cloud'."""
    from core.plugin.loader import discover_plugins

    results = discover_plugins(_PLUGINS_PATH)
    names = [name for name, _ in results]

    assert "confluent_cloud" in names, f"Got: {names}"


def test_discover_plugins_returns_list_of_tuples() -> None:
    """discover_plugins returns list[tuple[str, callable]]."""
    from core.plugin.loader import discover_plugins

    results = discover_plugins(_PLUGINS_PATH)

    assert isinstance(results, list)
    for item in results:
        assert isinstance(item, tuple)
        assert len(item) == 2
        name, factory = item
        assert isinstance(name, str)
        assert callable(factory)


# ---------------------------------------------------------------------------
# 7. Integration: _create_runner populates registry with both ecosystems
# ---------------------------------------------------------------------------


def test_create_runner_registers_self_managed_kafka() -> None:
    """_create_runner populates registry with 'self_managed_kafka'."""
    from unittest.mock import MagicMock, patch

    from main import _create_runner

    mock_settings = MagicMock()
    mock_settings.schedule.interval_seconds = 3600

    with patch("main.discover_plugins") as mock_discover, patch("main.PluginRegistry") as mock_reg_cls:
        from plugins.confluent_cloud.plugin import ConfluentCloudPlugin
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        mock_discover.return_value = [
            ("self_managed_kafka", SelfManagedKafkaPlugin),
            ("confluent_cloud", ConfluentCloudPlugin),
        ]
        mock_registry = MagicMock()
        mock_reg_cls.return_value = mock_registry

        _create_runner(mock_settings)

    calls = {call.args[0] for call in mock_registry.register.call_args_list}
    assert "self_managed_kafka" in calls


def test_create_runner_registers_confluent_cloud(tmp_path: Path) -> None:
    """_create_runner populates registry with 'confluent_cloud'."""
    from unittest.mock import MagicMock, patch

    from main import _create_runner

    mock_settings = MagicMock()

    with patch("main.discover_plugins") as mock_discover, patch("main.PluginRegistry") as mock_reg_cls:
        from plugins.confluent_cloud.plugin import ConfluentCloudPlugin
        from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

        mock_discover.return_value = [
            ("self_managed_kafka", SelfManagedKafkaPlugin),
            ("confluent_cloud", ConfluentCloudPlugin),
        ]
        mock_registry = MagicMock()
        mock_reg_cls.return_value = mock_registry

        _create_runner(mock_settings)

    calls = {call.args[0] for call in mock_registry.register.call_args_list}
    assert "confluent_cloud" in calls
