from __future__ import annotations

from collections.abc import Callable

from plugins.generic_metrics_only.plugin import GenericMetricsOnlyPlugin


def register() -> tuple[str, Callable[[], GenericMetricsOnlyPlugin]]:
    """Register generic_metrics_only in the plugin registry.

    Tenants set ecosystem: "generic_metrics_only" in their YAML config.
    The ecosystem label in all billing data is always "generic_metrics_only".
    """
    return "generic_metrics_only", GenericMetricsOnlyPlugin
