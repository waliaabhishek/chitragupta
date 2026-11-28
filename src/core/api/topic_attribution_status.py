"""Resolves the rich topic attribution status for a tenant's plugin settings."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from pydantic import ValidationError

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from core.config.models import PluginSettingsBase

TopicAttributionStatusValue = Literal["disabled", "enabled", "config_error"]


@dataclass(frozen=True)
class TopicAttributionStatus:
    status: TopicAttributionStatusValue
    error: str | None = None


def resolve_topic_attribution_status(
    plugin_settings: PluginSettingsBase,
    ecosystem: str,
) -> TopicAttributionStatus:
    """Return the rich TA status by inspecting plugin_settings.

    Handles both raw PluginSettingsBase (topic_attribution stored as dict
    due to extra="allow") and a pre-validated typed model.
    """
    ta = getattr(plugin_settings, "topic_attribution", None)

    # Determine whether TA is enabled — handle dict and typed-model cases.
    if ta is None:
        return TopicAttributionStatus(status="disabled")
    enabled = ta.get("enabled", False) if isinstance(ta, Mapping) else getattr(ta, "enabled", False)

    if not enabled:
        return TopicAttributionStatus(status="disabled")

    # TA is enabled — validate the full config if this is a confluent_cloud tenant.
    if ecosystem == "confluent_cloud":
        from plugins.confluent_cloud.config import CCloudPluginConfig

        try:
            CCloudPluginConfig.model_validate(plugin_settings.plugin_payload())
            return TopicAttributionStatus(status="enabled")
        except ValidationError as exc:
            messages = "; ".join(e["msg"] for e in exc.errors())
            return TopicAttributionStatus(
                status="config_error",
                error=messages,
            )

    # Non-ccloud ecosystem with TA enabled — no additional validation.
    return TopicAttributionStatus(status="enabled")


def resolve_topic_attribution_retention_days(
    plugin_settings: PluginSettingsBase,
    ecosystem: str,
) -> int | None:
    """Resolve the effective Topic Attribution policy without initializing a plugin.

    Built-in configuration models own their defaults and validation, as they do
    for runtime cleanup. Other ecosystems must expose an explicit policy.
    """
    try:
        if ecosystem == "confluent_cloud":
            from plugins.confluent_cloud.config import CCloudPluginConfig

            return CCloudPluginConfig.model_validate(plugin_settings.plugin_payload()).topic_attribution.retention_days
        if ecosystem == "self_managed_kafka":
            from plugins.self_managed_kafka.config import SelfManagedKafkaConfig

            return SelfManagedKafkaConfig.model_validate(
                plugin_settings.plugin_payload()
            ).topic_attribution.retention_days
    except ValidationError:
        return None

    topic_settings = getattr(plugin_settings, "topic_attribution", None)
    if isinstance(topic_settings, Mapping):
        value = topic_settings.get("retention_days")
    else:
        value = getattr(topic_settings, "retention_days", None)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 365:
        return None
    return value
