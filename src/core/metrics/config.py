from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, SecretStr, model_validator

from core.metrics.prometheus import AuthConfig, PrometheusConfig, PrometheusMetricsSource

if TYPE_CHECKING:
    from core.metrics.protocol import MetricsSource
logger = logging.getLogger(__name__)


class MetricsConnectionConfig(BaseModel):
    """Shared Prometheus connection config for all ecosystem plugins."""

    type: Literal["prometheus"] = "prometheus"
    url: str
    auth_type: Literal["basic", "bearer", "none"] = "none"
    username: str | None = None
    password: SecretStr | None = None
    bearer_token: SecretStr | None = None

    @model_validator(mode="after")
    def validate_auth_credentials(self) -> MetricsConnectionConfig:
        if self.auth_type == "basic":
            if not self.username or not self.password:
                raise ValueError("username and password required for basic auth")
        elif self.auth_type == "bearer":
            if not self.bearer_token:
                raise ValueError("bearer_token required for bearer auth")
        elif self.auth_type == "none" and (self.username or self.password or self.bearer_token):
            raise ValueError("credentials provided but auth_type is 'none'")
        return self


def create_metrics_source(config: MetricsConnectionConfig) -> MetricsSource:
    """Build a PrometheusMetricsSource from a MetricsConnectionConfig."""
    auth: AuthConfig | None = None
    if config.auth_type != "none":
        auth = AuthConfig(
            type=config.auth_type,
            username=config.username,
            password=config.password.get_secret_value() if config.password else None,
            token=config.bearer_token.get_secret_value() if config.bearer_token else None,
        )
    return PrometheusMetricsSource(PrometheusConfig(url=config.url, auth=auth))
