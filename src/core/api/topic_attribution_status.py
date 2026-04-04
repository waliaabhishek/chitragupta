"""Resolves the rich topic attribution status for a tenant's plugin settings."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

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
    enabled = ta.get("enabled", False) if isinstance(ta, dict) else getattr(ta, "enabled", False)

    if not enabled:
        return TopicAttributionStatus(status="disabled")

    # TA is enabled — validate the full config if this is a confluent_cloud tenant.
    if ecosystem == "confluent_cloud":
        from pydantic import ValidationError

        from plugins.confluent_cloud.config import CCloudPluginConfig

        try:
            CCloudPluginConfig.model_validate(plugin_settings.model_dump())
            return TopicAttributionStatus(status="enabled")
        except ValidationError as exc:
            messages = "; ".join(e["msg"] for e in exc.errors())
            return TopicAttributionStatus(
                status="config_error",
                error=messages,
            )

    # Non-ccloud ecosystem with TA enabled — no additional validation.
    return TopicAttributionStatus(status="enabled")
