from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient  # noqa: TC002

from core.models.topic_attribution import TopicAttributionRow
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend  # noqa: TC001

_BASE_URL = "/api/v1/tenants/test-tenant/topic-attributions"
_CLUSTER_ID = "lkc-test"
_ECOSYSTEM = "test-eco"
_TENANT_ID = "test-tenant"
_ENV_ID = "env-test"
# Explicit date range so the default 30-day window does not exclude seeded rows.
_BASE_PARAMS: dict[str, str] = {
    "start_date": "2026-02-01",
    "end_date": "2026-02-28",
}


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _make_ta_row(
    topic_name: str,
    amount: Decimal = Decimal("10.00"),
    hour: int = 0,
    cluster_resource_id: str = _CLUSTER_ID,
) -> TopicAttributionRow:
    return TopicAttributionRow(
        ecosystem=_ECOSYSTEM,
        tenant_id=_TENANT_ID,
        timestamp=datetime(2026, 2, 15, hour, tzinfo=UTC),
        env_id=_ENV_ID,
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_WRITE",
        attribution_method="bytes_ratio",
        amount=amount,
    )


def _seed_ta_rows(backend: SQLModelBackend, rows: list[TopicAttributionRow]) -> None:
    with backend.create_unit_of_work() as uow:
        uow.topic_attributions.upsert_batch(rows)
        uow.commit()


def _add_resource_tag(
    backend: SQLModelBackend,
    cluster_resource_id: str,
    topic_name: str,
    tag_key: str,
    tag_value: str,
    tenant_id: str = _TENANT_ID,
) -> None:
    entity_id = f"{cluster_resource_id}:topic:{topic_name}"
    with backend.create_unit_of_work() as uow:
        uow.tags.add_tag(tenant_id, "resource", entity_id, tag_key, tag_value, "test")
        uow.commit()


def _add_identity_tag(
    backend: SQLModelBackend,
    entity_id: str,
    tag_key: str,
    tag_value: str,
    tenant_id: str = _TENANT_ID,
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.tags.add_tag(tenant_id, "identity", entity_id, tag_key, tag_value, "test")
        uow.commit()


# ---------------------------------------------------------------------------
# Verification case 1: No tag params — existing behavior unchanged
# ---------------------------------------------------------------------------


class TestTopicAttributionListNoTagParams:
    def test_no_tag_params_returns_all_rows_for_tenant(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [
            _make_ta_row("topic-a", Decimal("10.00"), hour=0),
            _make_ta_row("topic-b", Decimal("20.00"), hour=1),
        ]
        _seed_ta_rows(in_memory_backend, rows)

        response = app_with_backend.get(_BASE_URL, params=_BASE_PARAMS)

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2


# ---------------------------------------------------------------------------
# Verification case 2: tag_key only — any matching value
# ---------------------------------------------------------------------------


class TestTopicAttributionListTagKeyOnly:
    def test_tag_key_only_returns_only_tagged_rows(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [
            _make_ta_row("topic-a", Decimal("10.00"), hour=0),
            _make_ta_row("topic-b", Decimal("20.00"), hour=1),
        ]
        _seed_ta_rows(in_memory_backend, rows)
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-a", "owner", "alice")

        response = app_with_backend.get(_BASE_URL, params={**_BASE_PARAMS, "tag_key": "owner"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["topic_name"] == "topic-a"

    def test_tag_key_only_matches_any_value(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [
            _make_ta_row("topic-a", Decimal("10.00"), hour=0),
            _make_ta_row("topic-b", Decimal("20.00"), hour=1),
        ]
        _seed_ta_rows(in_memory_backend, rows)
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-a", "owner", "alice")
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-b", "owner", "bob")

        response = app_with_backend.get(_BASE_URL, params={**_BASE_PARAMS, "tag_key": "owner"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2

    def test_tag_key_only_excludes_rows_without_that_key(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [
            _make_ta_row("topic-a", Decimal("10.00"), hour=0),
            _make_ta_row("topic-b", Decimal("20.00"), hour=1),
        ]
        _seed_ta_rows(in_memory_backend, rows)
        # topic-a has "env" tag only — no "owner"
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-a", "env", "prod")
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-b", "owner", "bob")

        response = app_with_backend.get(_BASE_URL, params={**_BASE_PARAMS, "tag_key": "owner"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["topic_name"] == "topic-b"


# ---------------------------------------------------------------------------
# Verification case 3: tag_key + tag_value — exact match
# ---------------------------------------------------------------------------


class TestTopicAttributionListTagKeyValue:
    def test_tag_key_and_value_filters_exactly(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [
            _make_ta_row("topic-a", Decimal("10.00"), hour=0),
            _make_ta_row("topic-b", Decimal("20.00"), hour=1),
        ]
        _seed_ta_rows(in_memory_backend, rows)
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-a", "owner", "alice")
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-b", "owner", "bob")

        response = app_with_backend.get(_BASE_URL, params={**_BASE_PARAMS, "tag_key": "owner", "tag_value": "alice"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["topic_name"] == "topic-a"

    def test_tag_key_and_value_returns_200_on_match(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [_make_ta_row("topic-a", Decimal("10.00"), hour=0)]
        _seed_ta_rows(in_memory_backend, rows)
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-a", "dept", "eng")

        response = app_with_backend.get(_BASE_URL, params={**_BASE_PARAMS, "tag_key": "dept", "tag_value": "eng"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 1
        assert data["items"][0]["topic_name"] == "topic-a"


# ---------------------------------------------------------------------------
# Verification case 4: Tenant isolation
# ---------------------------------------------------------------------------


class TestTopicAttributionListTenantIsolation:
    def test_tags_from_other_tenant_do_not_match(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [_make_ta_row("topic-a", Decimal("10.00"), hour=0)]
        _seed_ta_rows(in_memory_backend, rows)
        # Tag added for the same entity_id but under a different tenant
        _add_resource_tag(
            in_memory_backend,
            _CLUSTER_ID,
            "topic-a",
            "owner",
            "alice",
            tenant_id="other-tenant",
        )

        response = app_with_backend.get(_BASE_URL, params={**_BASE_PARAMS, "tag_key": "owner"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0


# ---------------------------------------------------------------------------
# Verification case 5: No identity bleed — entity_type='resource' scopes subquery
# ---------------------------------------------------------------------------


class TestTopicAttributionListNoIdentityBleed:
    def test_identity_tag_does_not_bleed_into_resource_filter(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [_make_ta_row("topic-a", Decimal("10.00"), hour=0)]
        _seed_ta_rows(in_memory_backend, rows)
        # Add an identity tag whose entity_id matches the resource_id format of topic-a
        entity_id = f"{_CLUSTER_ID}:topic:topic-a"
        _add_identity_tag(in_memory_backend, entity_id, "owner", "alice")
        # No resource tag — filtering by owner should return 0 rows

        response = app_with_backend.get(_BASE_URL, params={**_BASE_PARAMS, "tag_key": "owner"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0


# ---------------------------------------------------------------------------
# Verification cases 7 and 8: No match returns 0 results, not an error
# ---------------------------------------------------------------------------


class TestTopicAttributionListNoMatch:
    def test_nonexistent_tag_key_returns_zero_results(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [_make_ta_row("topic-a", Decimal("10.00"), hour=0)]
        _seed_ta_rows(in_memory_backend, rows)
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-a", "env", "prod")

        response = app_with_backend.get(_BASE_URL, params={**_BASE_PARAMS, "tag_key": "nonexistent_key"})

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert data["items"] == []

    def test_tag_key_exists_but_value_mismatch_returns_zero(
        self, app_with_backend: TestClient, in_memory_backend: SQLModelBackend
    ) -> None:
        rows = [_make_ta_row("topic-a", Decimal("10.00"), hour=0)]
        _seed_ta_rows(in_memory_backend, rows)
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-a", "owner", "alice")

        response = app_with_backend.get(
            _BASE_URL, params={**_BASE_PARAMS, "tag_key": "owner", "tag_value": "no_such_value"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert data["items"] == []


# ---------------------------------------------------------------------------
# Verification case: iter_by_filters streaming path with tag filter
# ---------------------------------------------------------------------------


class TestTopicAttributionIterByFiltersTagFilter:
    def test_iter_by_filters_tag_key_only_yields_tagged_rows(self, in_memory_backend: SQLModelBackend) -> None:
        rows = [
            _make_ta_row("topic-a", Decimal("10.00"), hour=0),
            _make_ta_row("topic-b", Decimal("20.00"), hour=1),
        ]
        _seed_ta_rows(in_memory_backend, rows)
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-a", "owner", "alice")

        with in_memory_backend.create_read_only_unit_of_work() as uow:
            result = list(
                uow.topic_attributions.iter_by_filters(
                    ecosystem=_ECOSYSTEM,
                    tenant_id=_TENANT_ID,
                    tag_key="owner",
                    tags_repo=uow.tags,
                )
            )

        assert len(result) == 1
        assert result[0].topic_name == "topic-a"

    def test_iter_by_filters_tag_key_value_yields_exact_match(self, in_memory_backend: SQLModelBackend) -> None:
        rows = [
            _make_ta_row("topic-a", Decimal("10.00"), hour=0),
            _make_ta_row("topic-b", Decimal("20.00"), hour=1),
        ]
        _seed_ta_rows(in_memory_backend, rows)
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-a", "owner", "alice")
        _add_resource_tag(in_memory_backend, _CLUSTER_ID, "topic-b", "owner", "bob")

        with in_memory_backend.create_read_only_unit_of_work() as uow:
            result = list(
                uow.topic_attributions.iter_by_filters(
                    ecosystem=_ECOSYSTEM,
                    tenant_id=_TENANT_ID,
                    tag_key="owner",
                    tag_value="alice",
                    tags_repo=uow.tags,
                )
            )

        assert len(result) == 1
        assert result[0].topic_name == "topic-a"
