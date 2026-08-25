from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from core.engine.helpers import _CENT, _distribute_remainder

if TYPE_CHECKING:
    from core.config.models import EmitterSpec
    from core.models.topic_attribution import TopicAttributionRow

logger = logging.getLogger(__name__)


@runtime_checkable
class TopicAttributionOutputConfigProtocol(Protocol):
    """Shared topic-attribution lifecycle configuration."""

    @property
    def enabled(self) -> bool: ...

    @property
    def exclude_topic_patterns(self) -> list[str]: ...

    @property
    def retention_days(self) -> int: ...

    @property
    def emitters(self) -> list[EmitterSpec]: ...


@runtime_checkable
class TopicAttributionConfigProtocol(Protocol):
    """Structural interface for topic attribution config consumed by core models.

    Satisfied structurally by the CCloud plugin's TopicAttributionConfig
    without explicit inheritance.
    """

    @property
    def missing_metrics_behavior(self) -> str: ...

    @property
    def exclude_topic_patterns(self) -> list[str]: ...

    @property
    def cost_mapping_overrides(self) -> dict[str, str]: ...

    @property
    def metric_name_overrides(self) -> dict[str, str]: ...


@dataclass(frozen=True)
class TopicAttributionContext:
    """Immutable context passed to topic attribution models."""

    ecosystem: str
    tenant_id: str
    env_id: str
    cluster_resource_id: str
    timestamp: datetime
    product_category: str
    product_type: str
    cluster_cost: Decimal
    topics: frozenset[str]
    topic_metrics: dict[str, dict[str, float]]  # {metric_key: {topic_name: value}}
    config: TopicAttributionConfigProtocol


@dataclass(frozen=True)
class TopicAttributionRowOutputContext:
    """Identity and cost fields shared by reconciled self-managed output rows."""

    ecosystem: str
    tenant_id: str
    timestamp: datetime
    env_id: str
    cluster_resource_id: str
    product_category: str
    product_type: str
    cluster_cost: Decimal


@runtime_checkable
class TopicAttributionModel(Protocol):
    """Protocol for topic attribution models.

    Returns list[TopicAttributionRow] on success, None to signal chain fallback.
    Terminal models must always return a result (never None).
    """

    def attribute(self, ctx: TopicAttributionContext) -> list[TopicAttributionRow] | None: ...


@dataclass(frozen=True)
class TopicUsageRatioModel:
    """Distribute cost across topics by metric usage ratio.

    Returns None if no metric data or all values zero (signals fallback).
    """

    metric_keys: tuple[str, ...]
    method_name: str = "bytes_ratio"

    def attribute(self, ctx: TopicAttributionContext) -> list[TopicAttributionRow] | None:
        topic_usage: dict[str, float] = {}
        for key in self.metric_keys:
            for topic, value in ctx.topic_metrics.get(key, {}).items():
                if topic in ctx.topics:
                    topic_usage[topic] = topic_usage.get(topic, 0.0) + value

        cluster_total = sum(topic_usage.values())
        if not topic_usage or cluster_total == 0.0:
            return None

        ratios = {t: v / cluster_total for t, v in topic_usage.items()}
        for t in ctx.topics:
            if t not in ratios:
                ratios[t] = 0.0

        return _build_rows(ctx, ratios, self.method_name)


@dataclass(frozen=True)
class TopicEvenSplitModel:
    """Distribute cost evenly across all topics.

    Returns None if no topics (signals fallback). Otherwise always succeeds.
    """

    method_name: str = "even_split"

    def attribute(self, ctx: TopicAttributionContext) -> list[TopicAttributionRow] | None:
        if not ctx.topics:
            return None
        topic_count = len(ctx.topics)
        ratios = {t: 1.0 / topic_count for t in ctx.topics}
        return _build_rows(ctx, ratios, self.method_name)


@dataclass(frozen=True)
class TopicSkipModel:
    """Terminal model that produces no rows. Used when missing_metrics_behavior=skip."""

    def attribute(self, ctx: TopicAttributionContext) -> list[TopicAttributionRow]:
        logger.info(
            "Skipping attribution for cluster=%s product_type=%s — no metrics, skip configured",
            ctx.cluster_resource_id,
            ctx.product_type,
        )
        return []


@dataclass(frozen=True)
class TopicMissingMetricsFallbackModel:
    """Fallback that checks missing_metrics_behavior config at runtime.

    Always returns a result (terminal position in chain).
    """

    def attribute(self, ctx: TopicAttributionContext) -> list[TopicAttributionRow]:
        if ctx.config.missing_metrics_behavior == "skip":
            return TopicSkipModel().attribute(ctx)
        return TopicEvenSplitModel().attribute(ctx) or []


@dataclass(frozen=True)
class TopicChainModel:
    """Try models in sequence until one succeeds. Same pattern as ChainModel."""

    models: Sequence[TopicAttributionModel]

    def attribute(self, ctx: TopicAttributionContext) -> list[TopicAttributionRow]:
        for i, model in enumerate(self.models):
            result = model.attribute(ctx)
            if result is not None:
                for row in result:
                    row.metadata["chain_tier"] = i
                return result
        logger.error("Topic chain exhausted without result for cluster=%s", ctx.cluster_resource_id)
        return []


def _make_metrics_chain(metric_keys: tuple[str, ...], method_name: str) -> TopicChainModel:
    return TopicChainModel(
        models=[
            TopicUsageRatioModel(metric_keys=metric_keys, method_name=method_name),
            TopicMissingMetricsFallbackModel(),
        ]
    )


KAFKA_TOPIC_ATTRIBUTION_MODELS: dict[str, TopicAttributionModel] = {
    "KAFKA_NETWORK_WRITE": _make_metrics_chain(("topic_bytes_in",), "bytes_ratio"),
    "KAFKA_NETWORK_READ": _make_metrics_chain(("topic_bytes_out",), "bytes_ratio"),
    "KAFKA_STORAGE": _make_metrics_chain(("topic_retained_bytes",), "retained_bytes_ratio"),
    "KAFKA_PARTITION": TopicEvenSplitModel(),
    "KAFKA_BASE": TopicEvenSplitModel(),
    "KAFKA_NUM_CKU": _make_metrics_chain(("topic_bytes_in", "topic_bytes_out"), "bytes_ratio"),
    "KAFKA_NUM_CKUS": _make_metrics_chain(("topic_bytes_in", "topic_bytes_out"), "bytes_ratio"),
}


def resolve_topic_attribution_models(
    overrides: dict[str, str],
) -> dict[str, TopicAttributionModel | None]:
    """Merge user overrides into default models. Returns None for "disabled" entries."""
    resolved: dict[str, TopicAttributionModel | None] = dict(KAFKA_TOPIC_ATTRIBUTION_MODELS)
    for product_type, method in overrides.items():
        if method == "disabled":
            resolved[product_type] = None
        elif method == "even_split":
            resolved[product_type] = TopicEvenSplitModel()
        elif method == "bytes_ratio":
            default = KAFKA_TOPIC_ATTRIBUTION_MODELS.get(product_type)
            if default:
                resolved[product_type] = default
            else:
                logger.warning("Cannot override %s to bytes_ratio — no default metric_keys", product_type)
        elif method == "retained_bytes_ratio":
            resolved[product_type] = _make_metrics_chain(
                ("topic_retained_bytes",),
                "retained_bytes_ratio",
            )
        else:
            logger.warning("Unknown override method %r for %s — ignored", method, product_type)
    return resolved


# --- Private helpers ---


def build_reconciled_topic_rows(
    ctx: TopicAttributionRowOutputContext,
    *,
    cluster_quantity: Decimal,
    pool_usage: Decimal,
    topic_usage: Mapping[str, Decimal],
    attribution_method: str,
    residual_method: str,
) -> list[TopicAttributionRow]:
    """Build rows that preserve a measured-topic usage residual separately from rounding."""
    del cluster_quantity
    from core.models.topic_attribution import TopicAttributionRow

    raw_cluster_cost = ctx.cluster_cost
    cluster_cost = raw_cluster_cost.quantize(_CENT, rounding=ROUND_HALF_UP)

    def unattributed(method: str) -> list[TopicAttributionRow]:
        return [
            TopicAttributionRow(
                ecosystem=ctx.ecosystem,
                tenant_id=ctx.tenant_id,
                timestamp=ctx.timestamp,
                env_id=ctx.env_id,
                cluster_resource_id=ctx.cluster_resource_id,
                topic_name="__UNATTRIBUTED__",
                product_category=ctx.product_category,
                product_type=ctx.product_type,
                attribution_method=method,
                amount=cluster_cost,
            )
        ]

    positive_topic_usage: dict[str, Decimal] = {}
    for topic, usage in topic_usage.items():
        if not usage.is_finite() or usage < 0:
            return unattributed("invalid_topic_telemetry")
        if usage > 0:
            positive_topic_usage[topic] = usage

    topic_usage_total = sum(positive_topic_usage.values(), start=Decimal(0))
    if pool_usage == 0:
        return unattributed("zero_cluster_usage" if topic_usage_total == 0 else "invalid_topic_telemetry")

    if topic_usage_total > pool_usage:
        overage_cost = abs(raw_cluster_cost) * (topic_usage_total - pool_usage) / pool_usage
        if overage_cost > _CENT:
            return unattributed("invalid_topic_telemetry")
        raw_amounts = {
            topic: raw_cluster_cost * usage / topic_usage_total for topic, usage in positive_topic_usage.items()
        }
        residual_amount: Decimal | None = None
    else:
        raw_amounts = {topic: raw_cluster_cost * usage / pool_usage for topic, usage in positive_topic_usage.items()}
        residual_amount = raw_cluster_cost * (pool_usage - topic_usage_total) / pool_usage

    row_specs = [(topic, attribution_method, raw_amounts[topic]) for topic in sorted(raw_amounts)]
    if residual_amount is not None and (residual_amount.quantize(_CENT, rounding=ROUND_HALF_UP) != 0 or not row_specs):
        row_specs.append(("__UNATTRIBUTED__", residual_method, residual_amount))

    amounts = [raw_amount.quantize(_CENT, rounding=ROUND_HALF_UP) for _, _, raw_amount in row_specs]
    amounts = _distribute_remainder(amounts, (cluster_cost - sum(amounts)).quantize(_CENT))

    return [
        TopicAttributionRow(
            ecosystem=ctx.ecosystem,
            tenant_id=ctx.tenant_id,
            timestamp=ctx.timestamp,
            env_id=ctx.env_id,
            cluster_resource_id=ctx.cluster_resource_id,
            topic_name=topic,
            product_category=ctx.product_category,
            product_type=ctx.product_type,
            attribution_method=method,
            amount=amount,
        )
        for (topic, method, _), amount in zip(row_specs, amounts, strict=True)
    ]


def _build_rows(
    ctx: TopicAttributionContext,
    topic_ratios: dict[str, float],
    attribution_method: str,
) -> list[TopicAttributionRow]:
    from core.models.topic_attribution import TopicAttributionRow

    sorted_topics = sorted(topic_ratios.keys())
    ratios = [topic_ratios[t] for t in sorted_topics]
    amounts = _split_by_ratios(ctx.cluster_cost, ratios)

    return [
        TopicAttributionRow(
            ecosystem=ctx.ecosystem,
            tenant_id=ctx.tenant_id,
            timestamp=ctx.timestamp,
            env_id=ctx.env_id,
            cluster_resource_id=ctx.cluster_resource_id,
            topic_name=topic,
            product_category=ctx.product_category,
            product_type=ctx.product_type,
            attribution_method=attribution_method,
            amount=amount,
        )
        for topic, amount in zip(sorted_topics, amounts, strict=True)
    ]


def _split_by_ratios(total: Decimal, ratios: list[float]) -> list[Decimal]:
    total = total.quantize(_CENT, rounding=ROUND_HALF_UP)
    raw = [total * Decimal(str(r)) for r in ratios]
    quantized = [a.quantize(_CENT, rounding=ROUND_HALF_UP) for a in raw]
    diff = (total - sum(quantized)).quantize(_CENT)
    return _distribute_remainder(quantized, diff)
