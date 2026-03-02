"""Confluent Cloud plugin registration."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from plugins.confluent_cloud.plugin import ConfluentCloudPlugin

if TYPE_CHECKING:
    from core.plugin.protocols import EcosystemPlugin


def register() -> tuple[str, Callable[[], EcosystemPlugin]]:
    """Return (ecosystem_name, factory) for the plugin loader."""
    return ("confluent_cloud", ConfluentCloudPlugin)


__all__ = ["ConfluentCloudPlugin", "register"]
