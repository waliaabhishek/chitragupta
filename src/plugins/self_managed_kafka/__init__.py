"""Self-managed Kafka plugin registration."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from plugins.self_managed_kafka.plugin import SelfManagedKafkaPlugin

if TYPE_CHECKING:
    from core.plugin.protocols import EcosystemPlugin


def register() -> tuple[str, Callable[[], EcosystemPlugin]]:
    """Return (ecosystem_name, factory) for the plugin loader."""
    return ("self_managed_kafka", SelfManagedKafkaPlugin)


__all__ = ["SelfManagedKafkaPlugin", "register"]
