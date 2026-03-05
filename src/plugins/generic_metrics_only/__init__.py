from __future__ import annotations

from collections.abc import Callable

from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin


def register() -> tuple[str, Callable[[], GenericMetricsOnlyPlugin]]:
    """Register generic_metrics_only in the plugin registry.

    Tenants set ecosystem: "generic_metrics_only" in their YAML config.
    The ecosystem_name field inside plugin_settings determines the billing label
    (e.g. "self_managed_postgres"). Registry key and ecosystem label deliberately differ.
    """
    return "generic_metrics_only", GenericMetricsOnlyPlugin
