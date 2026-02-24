from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pydantic import SecretStr

if TYPE_CHECKING:
    from plugins.confluent_cloud.connections import CCloudConnection

from core.models import Identity, Resource, ResourceStatus
from plugins.confluent_cloud.crn import parse_ccloud_crn

LOGGER = logging.getLogger(__name__)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse ISO 8601 datetime string to UTC-aware datetime.

    Handles:
    - Z suffix: 2024-01-15T10:30:00Z
    - Offset: 2024-01-15T10:30:00+00:00
    - Microseconds: 2024-01-15T10:30:00.123456Z
    - Naive (no timezone): treated as UTC

    Returns None for empty/invalid values.
    """
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(value)

        # Ensure UTC-aware (handle naive datetimes defensively)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)

        return dt
    except ValueError:
        LOGGER.warning("Could not parse datetime: %s", value)
        return None


def gather_environments(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
) -> Iterable[Resource]:
    """Gather all environments from CCloud org.

    Yields Resource for each environment.
    Endpoint: GET /org/v2/environments
    """
    for item in conn.get("/org/v2/environments"):
        metadata_obj = item.get("metadata", {})

        yield Resource(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            resource_id=item["id"],
            resource_type="environment",
            display_name=item.get("display_name"),
            status=ResourceStatus.ACTIVE,
            created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
            metadata={},
        )


def gather_kafka_clusters(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
    environment_ids: Iterable[str],
) -> Iterable[Resource]:
    """Gather Kafka clusters for each environment (fan-out).

    Yields Resource for each cluster.
    Endpoint: GET /cmk/v2/clusters?environment={env_id}
    """
    for env_id in environment_ids:
        for item in conn.get("/cmk/v2/clusters", params={"environment": env_id}):
            spec = item.get("spec", {})
            metadata_obj = item.get("metadata", {})

            # Normalize cloud/region to lowercase (per design decision)
            cloud = (spec.get("cloud") or "").lower().strip()
            region = (spec.get("region") or "").lower().strip()

            yield Resource(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                resource_id=item["id"],
                resource_type="kafka_cluster",
                display_name=spec.get("display_name"),
                parent_id=spec.get("environment", {}).get("id"),
                status=ResourceStatus.ACTIVE,
                created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
                metadata={
                    "bootstrap_url": spec.get("kafka_bootstrap_endpoint"),
                    "cloud": cloud,
                    "region": region,
                },
            )


def gather_connectors(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
    clusters: Iterable[tuple[str, str]],  # (env_id, cluster_id)
) -> Iterable[Resource]:
    """Gather connectors per Kafka cluster.

    Uses get_raw() because connector API returns dict-of-dicts instead of
    standard envelope.
    Endpoint: GET /connect/v1/environments/{env}/clusters/{cluster}/connectors?expand=info,status,id
    """
    for env_id, cluster_id in clusters:
        path = f"/connect/v1/environments/{env_id}/clusters/{cluster_id}/connectors"
        params = {"expand": "info,status,id"}
        response = conn.get_raw(path, params=params)

        for connector_name, connector_data in response.items():
            info = connector_data.get("info", {})
            config = info.get("config", {})
            connector_id_obj = connector_data.get("id", {})

            # Extract auth mode and credentials for later identity resolution
            auth_mode = config.get("kafka.auth.mode", "UNKNOWN")
            metadata: dict[str, Any] = {
                "kafka_auth_mode": auth_mode,
                "connector_class": config.get("connector.class"),
                "env_id": env_id,
            }

            if auth_mode == "SERVICE_ACCOUNT":
                metadata["kafka_service_account_id"] = config.get("kafka.service.account.id")
            elif auth_mode == "KAFKA_API_KEY":
                metadata["kafka_api_key"] = config.get("kafka.api.key")

            # Connector API does not include created_at in response
            yield Resource(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                resource_id=connector_id_obj.get("id", connector_name),
                resource_type="connector",
                display_name=config.get("name", connector_name),
                parent_id=cluster_id,
                status=ResourceStatus.ACTIVE,
                metadata=metadata,
            )


def gather_schema_registries(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
    environment_ids: Iterable[str],
) -> Iterable[Resource]:
    """Gather Schema Registry clusters per environment.

    Endpoint: GET /srcm/v3/clusters?environment={env_id}
    """
    for env_id in environment_ids:
        for item in conn.get("/srcm/v3/clusters", params={"environment": env_id}):
            spec = item.get("spec", {})
            metadata_obj = item.get("metadata", {})

            # Normalize cloud/region to lowercase
            cloud = (spec.get("cloud") or "").lower().strip()
            region = (spec.get("region") or "").lower().strip()

            yield Resource(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                resource_id=item["id"],
                resource_type="schema_registry",
                display_name=spec.get("display_name"),
                parent_id=spec.get("environment", {}).get("id"),
                status=ResourceStatus.ACTIVE,
                created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
                metadata={
                    "http_endpoint": spec.get("http_endpoint"),
                    "cloud": cloud,
                    "region": region,
                    "crn": metadata_obj.get("resource_name"),
                },
            )


def gather_ksqldb_clusters(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
    environment_ids: Iterable[str],
) -> Iterable[Resource]:
    """Gather ksqlDB clusters per environment.

    Endpoint: GET /ksqldbcm/v2/clusters?environment={env_id}
    """
    for env_id in environment_ids:
        for item in conn.get("/ksqldbcm/v2/clusters", params={"environment": env_id}):
            spec = item.get("spec", {})
            metadata_obj = item.get("metadata", {})

            # Owner from credential_identity, with sentinel fallback
            owner_id = spec.get("credential_identity", {}).get("id")
            if not owner_id:
                owner_id = "ksqldb_owner_unknown"

            yield Resource(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                resource_id=item["id"],
                resource_type="ksqldb_cluster",
                display_name=spec.get("display_name"),
                parent_id=spec.get("environment", {}).get("id"),
                owner_id=owner_id,
                status=ResourceStatus.ACTIVE,
                created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
                metadata={
                    "kafka_cluster_id": spec.get("kafka_cluster", {}).get("id"),
                    "csu_count": spec.get("csu"),
                },
            )


# Flink statement phases that indicate a stopped/completed statement
# Intentionally uppercase: we normalize phase to upper before comparison
STOPPED_PHASES = {"COMPLETED", "FAILED", "STOPPED"}


def gather_flink_compute_pools(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
    environment_ids: Iterable[str],
    flink_regions: dict[str, tuple[str, str]],  # region_id (lowercase) -> (api_key, api_secret)
) -> Iterable[Resource]:
    """Gather Flink compute pools per environment.

    Marks allocatability based on whether region exists in flink_regions config.
    Endpoint: GET /fcpm/v2/compute-pools?environment={env_id}
    """
    for env_id in environment_ids:
        for item in conn.get("/fcpm/v2/compute-pools", params={"environment": env_id}):
            spec = item.get("spec", {})
            metadata_obj = item.get("metadata", {})

            # Normalize cloud and region: lowercase + strip (standard for all string matching)
            cloud = (spec.get("cloud") or "").lower().strip()
            region = (spec.get("region") or "").lower().strip()

            # Allocatable only if we have credentials for this region
            is_allocatable = region in flink_regions

            yield Resource(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                resource_id=item["id"],
                resource_type="flink_compute_pool",
                display_name=spec.get("display_name"),
                parent_id=env_id,
                status=ResourceStatus.ACTIVE,
                created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
                metadata={
                    "cloud": cloud,
                    "region": region,
                    "is_allocatable": is_allocatable,
                    "crn": metadata_obj.get("resource_name"),
                },
            )


def gather_flink_statements(
    ecosystem: str,
    tenant_id: str,
    allocatable_pools: Iterable[tuple[Resource, str, str]],  # (pool, api_key, api_secret)
) -> Iterable[Resource]:
    """Gather Flink statements from allocatable pools using regional API.

    Unlike other gatherers, this does NOT take a CCloudConnection param — it
    creates regional connections internally using pool-specific credentials.

    Connection caching: Connections are cached by (region, cloud, api_key) to avoid
    creating/destroying connections for each pool in the same region.
    """
    # Import here to avoid circular import
    from plugins.confluent_cloud.connections import CCloudConnection

    # Cache connections by (region, cloud, api_key) to avoid creating new connections
    # for every pool in the same region with the same credentials
    conn_cache: dict[tuple[str, str, str], CCloudConnection] = {}

    def get_or_create_connection(region: str, cloud: str, api_key: str, api_secret: str) -> CCloudConnection:
        # api_key uniquely identifies credential pair
        cache_key = (region, cloud, api_key)
        if cache_key not in conn_cache:
            regional_base_url = f"https://flink.{region}.{cloud}.confluent.cloud"
            conn_cache[cache_key] = CCloudConnection(
                api_key=api_key,
                api_secret=SecretStr(api_secret),
                base_url=regional_base_url,
            )
        return conn_cache[cache_key]

    try:
        for pool, api_key, api_secret in allocatable_pools:
            # Region/cloud already normalized (lowercase, stripped) in gather_flink_compute_pools
            cloud = pool.metadata.get("cloud", "")
            region = pool.metadata.get("region", "")
            env_id = pool.parent_id or ""

            # Extract org_id from CRN (fallback to tenant_id)
            crn = pool.metadata.get("crn", "")
            crn_parts = parse_ccloud_crn(crn)
            org_id = crn_parts.get("organization", tenant_id)

            # Get or create regional connection (cached)
            regional_conn = get_or_create_connection(region, cloud, api_key, api_secret)

            path = f"/sql/v1/organizations/{org_id}/environments/{env_id}/statements"
            for item in regional_conn.get(path):
                meta = item.get("metadata", {})
                spec = item.get("spec", {})
                status = item.get("status", {})
                phase = status.get("phase", "").upper()

                # Resource ID: prefer uid, fallback to name, fallback to deterministic sentinel
                resource_id = meta.get("uid") or item.get("name")
                if not resource_id:
                    # Deterministic sentinel based on pool_id + item content hash
                    hash_input = f"{pool.resource_id}:{json.dumps(item, sort_keys=True)}"
                    hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()[:12]
                    resource_id = f"flink_stmt_unknown_{hash_digest}"

                yield Resource(
                    ecosystem=ecosystem,
                    tenant_id=tenant_id,
                    resource_id=resource_id,
                    resource_type="flink_statement",
                    display_name=item.get("name"),
                    parent_id=env_id,
                    owner_id=spec.get("principal"),
                    status=ResourceStatus.ACTIVE,
                    metadata={
                        "statement_name": item.get("name"),
                        "compute_pool_id": spec.get("compute_pool", {}).get("id", pool.resource_id),
                        "is_stopped": phase in STOPPED_PHASES,
                    },
                )
    finally:
        # Clean up all cached connections
        for conn in conn_cache.values():
            conn.close()


# =============================================================================
# Identity Gatherers
# =============================================================================


def gather_service_accounts(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
) -> Iterable[Identity]:
    """Gather all service accounts (org-scoped).

    Endpoint: GET /iam/v2/service-accounts
    """
    for item in conn.get("/iam/v2/service-accounts"):
        metadata_obj = item.get("metadata", {})
        yield Identity(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            identity_id=item["id"],
            identity_type="service_account",
            display_name=item.get("display_name"),
            created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
            metadata={
                "description": item.get("description"),
            },
        )


def gather_users(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
) -> Iterable[Identity]:
    """Gather all user accounts (org-scoped).

    Endpoint: GET /iam/v2/users
    """
    for item in conn.get("/iam/v2/users"):
        metadata_obj = item.get("metadata", {})
        yield Identity(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            identity_id=item["id"],
            identity_type="user",
            display_name=item.get("full_name"),
            created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
            metadata={
                "crn": metadata_obj.get("resource_name"),
            },
        )


def gather_api_keys(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
) -> Iterable[Identity]:
    """Gather all API keys (org-scoped).

    Stores owner reference for later resolution.
    Endpoint: GET /iam/v2/api-keys
    """
    for item in conn.get("/iam/v2/api-keys"):
        spec = item.get("spec", {})
        metadata_obj = item.get("metadata", {})
        yield Identity(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            identity_id=item["id"],
            identity_type="api_key",
            display_name=spec.get("description"),
            created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
            metadata={
                "owner_id": spec.get("owner", {}).get("id"),
                "resource_id": spec.get("resource", {}).get("id"),
            },
        )


def gather_identity_providers(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
) -> Iterable[Identity]:
    """Gather all identity providers (org-scoped).

    Endpoint: GET /iam/v2/identity-providers
    """
    for item in conn.get("/iam/v2/identity-providers"):
        metadata_obj = item.get("metadata", {})
        yield Identity(
            ecosystem=ecosystem,
            tenant_id=tenant_id,
            identity_id=item["id"],
            identity_type="identity_provider",
            display_name=item.get("display_name"),
            created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
            metadata={
                "description": item.get("description"),
                "crn": metadata_obj.get("resource_name"),
            },
        )


def gather_identity_pools(
    conn: CCloudConnection,
    ecosystem: str,
    tenant_id: str,
    provider_ids: Iterable[str],
) -> Iterable[Identity]:
    """Gather identity pools per identity provider (fan-out).

    Endpoint: GET /iam/v2/identity-providers/{provider_id}/identity-pools
    """
    for provider_id in provider_ids:
        path = f"/iam/v2/identity-providers/{provider_id}/identity-pools"
        for item in conn.get(path):
            metadata_obj = item.get("metadata", {})
            yield Identity(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                identity_id=item["id"],
                identity_type="identity_pool",
                display_name=item.get("display_name"),
                created_at=_parse_iso_datetime(metadata_obj.get("created_at")),
                metadata={
                    "description": item.get("description"),
                    "provider_id": provider_id,
                },
            )
