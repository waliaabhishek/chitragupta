"""Self-managed Kafka plugin registration."""

from __future__ import annotations

from typing import TYPE_CHECKING

from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

if TYPE_CHECKING:
    from core.plugin.registry import PluginRegistry


def register(registry: PluginRegistry) -> None:
    """Register the self-managed Kafka plugin."""
    registry.register("self_managed_kafka", SelfManagedKafkaPlugin)


__all__ = ["SelfManagedKafkaPlugin", "register"]
