from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from core.models.topic_attribution import TopicAttributionRow

logger = logging.getLogger(__name__)


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
    metrics_available: bool  # False if Prometheus returned None
    config: TopicAttributionConfigProtocol


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
        if not ctx.metrics_available:
            return None

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

_CENT = Decimal("0.0001")


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


def _distribute_remainder(amounts: list[Decimal], diff: Decimal) -> list[Decimal]:
    result = list(amounts)
    step = _CENT if diff > 0 else -_CENT
    i = 0
    while diff != 0:
        result[i % len(result)] += step
        diff -= step
        i += 1
    return result
