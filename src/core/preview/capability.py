from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

PreviewTargetFocusVersion = Literal["1.4"]
PreviewConformanceStatus = Literal["non_conforming"]
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KnownGap:
    code: str
    description: str
    owner_task: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class FocusPreviewCapability:
    mapping_profile_version: str
    target_focus_version: PreviewTargetFocusVersion
    conformance_status: PreviewConformanceStatus
    known_gaps: tuple[KnownGap, ...]


FOCUS_PREVIEW_CAPABILITY = FocusPreviewCapability(
    mapping_profile_version="focus-1.4-preview-v1",
    target_focus_version="1.4",
    conformance_status="non_conforming",
    known_gaps=(
        KnownGap(
            "invoice_identity_unavailable",
            "Post-issuance invoice identity is unavailable.",
            "TASK-254.04",
            ("InvoiceDetailId", "InvoiceId"),
        ),
        KnownGap(
            "invoice_issuer_name_unavailable",
            "Provider legal invoice-issuer evidence is unavailable.",
            "TASK-254.04",
            ("InvoiceIssuerName",),
        ),
        KnownGap(
            "provider_host_display_name_unavailable",
            "HostProviderName contains the raw provider cloud code, not a provider display name.",
            "TASK-254.04",
            ("HostProviderName",),
        ),
        KnownGap(
            "provider_region_display_name_unavailable",
            "Confluent inventory does not provide a distinct region display name.",
            "TASK-254.04",
            ("RegionName",),
        ),
        KnownGap(
            "derived_sku_identity_not_provider_authoritative",
            "SKU values are deterministic Chitragupta-derived evidence, not provider-issued identifiers.",
            "TASK-254.04",
            ("SkuId", "SkuMeter", "SkuPriceDetails", "SkuPriceId", "x_ChitraguptaSkuComponents"),
        ),
    ),
)

MAPPING_PROFILE_VERSION = FOCUS_PREVIEW_CAPABILITY.mapping_profile_version
KNOWN_GAPS = FOCUS_PREVIEW_CAPABILITY.known_gaps


def preview_manifest_known_gaps() -> list[dict[str, object]]:
    return [
        {
            "code": gap.code,
            "description": gap.description,
            "columns": list(gap.columns),
        }
        for gap in FOCUS_PREVIEW_CAPABILITY.known_gaps
    ]
