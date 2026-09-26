from __future__ import annotations

import asyncio
import hashlib
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from core.api.app import create_app
from core.models.chargeback import ChargebackRow, CostType
from core.models.topic_attribution import TopicAttributionRow
from tests.integration.core.api.backend_provider import install_backend

if TYPE_CHECKING:
    from starlette.types import Message, Scope

    from core.config.models import AppSettings
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from core.storage.interface import StorageBackend
    from tests.integration.core.api.backend_provider import FixedTenantBackendProvider


def _event_loop_thread(client: TestClient) -> int:
    assert client.portal is not None
    return client.portal.call(threading.get_ident)


def _seed_fixed_rows(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(
            ChargebackRow(
                ecosystem="test-eco",
                tenant_id="test-tenant",
                timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                resource_id="resource-1",
                product_category="compute",
                product_type="kafka",
                identity_id="user-1",
                cost_type=CostType.USAGE,
                amount=Decimal("10.00"),
                allocation_method="direct",
                allocation_detail=None,
                tags={},
                metadata={},
            )
        )
        uow.topic_attributions.upsert_batch(
            [
                TopicAttributionRow(
                    ecosystem="test-eco",
                    tenant_id="test-tenant",
                    timestamp=datetime(2026, 2, 15, tzinfo=UTC),
                    env_id="env-1",
                    cluster_resource_id="lkc-abc",
                    topic_name="orders",
                    product_category="KAFKA",
                    product_type="KAFKA_NETWORK_WRITE",
                    attribution_method="bytes_ratio",
                    amount=Decimal("10.00"),
                )
            ]
        )
        uow.commit()


def _record_worker_ownership(
    backend: SQLModelBackend,
    provider: FixedTenantBackendProvider,
    repository_name: str,
    method_name: str,
    events: list[tuple[str, int]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_acquire = provider.acquire_backend

    @contextmanager
    def acquire_backend(tenant_name: str, tenant_config: Any) -> Iterator[SQLModelBackend]:
        events.append(("lease-enter", threading.get_ident()))
        try:
            with original_acquire(tenant_name, tenant_config) as leased_backend:
                yield leased_backend
        finally:
            events.append(("lease-exit", threading.get_ident()))

    monkeypatch.setattr(provider, "acquire_backend", acquire_backend)
    original_create_uow = backend.create_read_only_unit_of_work

    def create_uow() -> Any:
        events.append(("uow-create", threading.get_ident()))
        manager = original_create_uow()

        @contextmanager
        def owned_uow() -> Iterator[Any]:
            with manager as uow:
                events.append(("uow-enter", threading.get_ident()))
                repository = getattr(uow, repository_name)
                original_method = getattr(repository, method_name)

                if method_name == "iter_by_filters":

                    def tracked_method(*args: object, **kwargs: object) -> Iterator[object]:
                        events.append(("repo-use", threading.get_ident()))
                        yield from original_method(*args, **kwargs)

                else:

                    def tracked_method(*args: object, **kwargs: object) -> object:
                        events.append(("repo-use", threading.get_ident()))
                        return original_method(*args, **kwargs)

                setattr(repository, method_name, tracked_method)
                yield uow
            assert uow._session is None
            events.append(("uow-close", threading.get_ident()))

        return owned_uow()

    monkeypatch.setattr(backend, "create_read_only_unit_of_work", create_uow)


@pytest.mark.parametrize(
    ("name", "method", "path", "params", "body", "repository", "repository_method"),
    [
        (
            "chargeback aggregate",
            "GET",
            "/api/v1/tenants/test-tenant/chargebacks/aggregate",
            {"group_by": "identity_id", "start_date": "2026-02-01", "end_date": "2026-02-28"},
            None,
            "chargebacks",
            "aggregate",
        ),
        (
            "chargeback list",
            "GET",
            "/api/v1/tenants/test-tenant/chargebacks",
            {"start_date": "2026-02-01", "end_date": "2026-02-28"},
            None,
            "chargebacks",
            "find_by_filters",
        ),
        (
            "chargeback export",
            "POST",
            "/api/v1/tenants/test-tenant/export",
            None,
            {"start_date": "2026-02-01", "end_date": "2026-02-28", "columns": ["timestamp", "amount"]},
            "chargebacks",
            "iter_by_filters",
        ),
        (
            "topic aggregate",
            "GET",
            "/api/v1/tenants/test-tenant/topic-attributions/aggregate",
            {"group_by": "topic_name", "start_date": "2026-02-01", "end_date": "2026-02-28"},
            None,
            "topic_attributions",
            "aggregate",
        ),
        (
            "topic list",
            "GET",
            "/api/v1/tenants/test-tenant/topic-attributions",
            {"start_date": "2026-02-01", "end_date": "2026-02-28"},
            None,
            "topic_attributions",
            "find_by_filters",
        ),
        (
            "topic export",
            "POST",
            "/api/v1/tenants/test-tenant/topic-attributions/export",
            {"start_date": "2026-02-01", "end_date": "2026-02-28"},
            None,
            "topic_attributions",
            "iter_by_filters",
        ),
    ],
)
def test_heavy_read_route_owns_lease_and_uow_in_worker(
    name: str,
    method: str,
    path: str,
    params: dict[str, str] | None,
    body: dict[str, object] | None,
    repository: str,
    repository_method: str,
    settings_with_tenant: AppSettings,
    in_memory_backend: SQLModelBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del name
    _seed_fixed_rows(in_memory_backend)
    app = create_app(settings_with_tenant)
    events: list[tuple[str, int]] = []

    with TestClient(app) as client:
        provider = install_backend(app, "test-tenant", in_memory_backend)
        _record_worker_ownership(in_memory_backend, provider, repository, repository_method, events, monkeypatch)
        event_loop_thread = _event_loop_thread(client)
        response = client.request(method, path, params=params, json=body)

    assert response.status_code == 200
    assert [name for name, _ in events] == [
        "lease-enter",
        "uow-create",
        "uow-enter",
        "repo-use",
        "uow-close",
        "lease-exit",
    ]
    worker_threads = {thread for _, thread in events}
    assert len(worker_threads) == 1
    assert worker_threads.isdisjoint({event_loop_thread, threading.get_ident()})


def test_heavy_read_route_response_hashes_match_pre_edit_baseline(
    app_with_backend: TestClient, in_memory_backend: SQLModelBackend
) -> None:
    _seed_fixed_rows(in_memory_backend)
    requests = [
        (
            "chargebacks-aggregate",
            "GET",
            "/api/v1/tenants/test-tenant/chargebacks/aggregate?group_by=identity_id&start_date=2026-02-01&end_date=2026-02-28",
            None,
            "9d084689fe401bd38bb6eb9de504b46db5159cf120df3a39fa357a10545eb054",  # pragma: allowlist secret
            {"content-type": "application/json"},
        ),
        (
            "chargebacks-list",
            "GET",
            "/api/v1/tenants/test-tenant/chargebacks?start_date=2026-02-01&end_date=2026-02-28",
            None,
            "54c486c4c7edb82defbe3fe9d2678df959d30b4c4b99cd3b51eaf6cbb85e08dc",  # pragma: allowlist secret
            {"content-type": "application/json"},
        ),
        (
            "chargebacks-export",
            "POST",
            "/api/v1/tenants/test-tenant/export",
            {
                "start_date": "2026-02-01",
                "end_date": "2026-02-28",
                "columns": [
                    "timestamp",
                    "resource_id",
                    "product_category",
                    "product_type",
                    "identity_id",
                    "cost_type",
                    "amount",
                    "allocation_method",
                    "tags",
                ],
            },
            "2fa6e3aca682dcc4f0a4918231cda17454a4df83829929ab65fb71b34cfed022",  # pragma: allowlist secret
            {
                "content-type": "text/csv; charset=utf-8",
                "content-disposition": "attachment; filename=chargebacks.csv",
            },
        ),
        (
            "topic-aggregate",
            "GET",
            "/api/v1/tenants/test-tenant/topic-attributions/aggregate?group_by=topic_name&start_date=2026-02-01&end_date=2026-02-28",
            None,
            "48221bf2b4b1c47b6463d8f06fd26541d7750fec2fbd952d0f8f9ba12e59bfdf",  # pragma: allowlist secret
            {"content-type": "application/json"},
        ),
        (
            "topic-list",
            "GET",
            "/api/v1/tenants/test-tenant/topic-attributions?start_date=2026-02-01&end_date=2026-02-28",
            None,
            "08ed283a6971f005a4fae7bd8e03ce7ca22250e1512ecdfab7e363f69fe61374",  # pragma: allowlist secret
            {"content-type": "application/json"},
        ),
        (
            "topic-export",
            "POST",
            "/api/v1/tenants/test-tenant/topic-attributions/export?start_date=2026-02-01&end_date=2026-02-28",
            None,
            "64b5eb5ff2798b1d8eef354aa9507beaa8ba03a1319ffd59862e0dad5eb03fc3",  # pragma: allowlist secret
            {
                "content-type": "text/csv; charset=utf-8",
                "content-disposition": "attachment; filename=topic_attributions.csv",
            },
        ),
    ]

    for _name, method, path, body, expected_hash, expected_headers in requests:
        response = (
            app_with_backend.request(method, path, json=body)
            if body is not None
            else app_with_backend.request(method, path)
        )
        assert response.status_code == 200
        assert {header: response.headers[header] for header in expected_headers} == expected_headers
        assert hashlib.sha256(response.content).hexdigest() == expected_hash


def test_five_concurrent_api_queries_leave_event_loop_live(
    settings_with_tenant: AppSettings,
    in_memory_backend: SQLModelBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_fixed_rows(in_memory_backend)
    settings = settings_with_tenant.model_copy(
        update={"api": settings_with_tenant.api.model_copy(update={"request_timeout_seconds": 10})}
    )
    app = create_app(settings)
    entered = threading.Event()
    heartbeat_advanced = threading.Event()
    release = threading.Event()
    operation_threads: list[int] = []
    heartbeat_count = 0
    heartbeat_task: asyncio.Task[None] | None = None

    @app.middleware("http")
    async def heartbeat_middleware(request: Any, call_next: Any) -> Any:
        async def pulse() -> None:
            nonlocal heartbeat_count
            while not release.is_set():
                if entered.is_set():
                    heartbeat_count += 1
                    if heartbeat_count >= 10:
                        heartbeat_advanced.set()
                await asyncio.sleep(0)

        nonlocal heartbeat_task
        if heartbeat_task is None:
            heartbeat_task = asyncio.create_task(pulse())
        return await call_next(request)

    with TestClient(app) as client:
        install_backend(app, "test-tenant", in_memory_backend)
        original_create_uow = in_memory_backend.create_read_only_unit_of_work

        def record_repository_call() -> None:
            operation_threads.append(threading.get_ident())
            if len(operation_threads) >= 5:
                entered.set()
            if not release.wait(5):
                raise TimeoutError("test query gate was not released")

        def create_uow() -> Any:
            manager = original_create_uow()

            @contextmanager
            def owned_uow() -> Iterator[Any]:
                with manager as uow:
                    for repository_name, method_name in (
                        ("chargebacks", "aggregate"),
                        ("chargebacks", "find_by_filters"),
                        ("topic_attributions", "aggregate"),
                        ("chargebacks", "iter_by_filters"),
                        ("topic_attributions", "iter_by_filters"),
                    ):
                        repository = getattr(uow, repository_name)
                        original_method = getattr(repository, method_name)

                        if method_name == "iter_by_filters":

                            def tracked_method(
                                *args: object,
                                _original: Any = original_method,
                                **kwargs: object,
                            ) -> Iterator[object]:
                                record_repository_call()
                                yield from _original(*args, **kwargs)

                        else:

                            def tracked_method(
                                *args: object,
                                _original: Any = original_method,
                                **kwargs: object,
                            ) -> Any:
                                record_repository_call()
                                return _original(*args, **kwargs)

                        setattr(repository, method_name, tracked_method)
                    yield uow

            return owned_uow()

        monkeypatch.setattr(in_memory_backend, "create_read_only_unit_of_work", create_uow)
        event_loop_thread = _event_loop_thread(client)
        date_range = {"start_date": "2026-02-01", "end_date": "2026-02-28"}
        requests = [
            (
                "GET",
                "/api/v1/tenants/test-tenant/chargebacks/aggregate",
                {**date_range, "group_by": "identity_id"},
                None,
                "9d084689fe401bd38bb6eb9de504b46db5159cf120df3a39fa357a10545eb054",  # pragma: allowlist secret
                {"content-type": "application/json"},
            ),
            (
                "GET",
                "/api/v1/tenants/test-tenant/chargebacks",
                date_range,
                None,
                "54c486c4c7edb82defbe3fe9d2678df959d30b4c4b99cd3b51eaf6cbb85e08dc",  # pragma: allowlist secret
                {"content-type": "application/json"},
            ),
            (
                "GET",
                "/api/v1/tenants/test-tenant/topic-attributions/aggregate",
                {**date_range, "group_by": "topic_name"},
                None,
                "48221bf2b4b1c47b6463d8f06fd26541d7750fec2fbd952d0f8f9ba12e59bfdf",  # pragma: allowlist secret
                {"content-type": "application/json"},
            ),
            (
                "POST",
                "/api/v1/tenants/test-tenant/export",
                None,
                {
                    **date_range,
                    "columns": [
                        "timestamp",
                        "resource_id",
                        "product_category",
                        "product_type",
                        "identity_id",
                        "cost_type",
                        "amount",
                        "allocation_method",
                        "tags",
                    ],
                },
                "2fa6e3aca682dcc4f0a4918231cda17454a4df83829929ab65fb71b34cfed022",  # pragma: allowlist secret
                {
                    "content-type": "text/csv; charset=utf-8",
                    "content-disposition": "attachment; filename=chargebacks.csv",
                },
            ),
            (
                "POST",
                "/api/v1/tenants/test-tenant/topic-attributions/export",
                date_range,
                None,
                "64b5eb5ff2798b1d8eef354aa9507beaa8ba03a1319ffd59862e0dad5eb03fc3",  # pragma: allowlist secret
                {
                    "content-type": "text/csv; charset=utf-8",
                    "content-disposition": "attachment; filename=topic_attributions.csv",
                },
            ),
        ]
        try:
            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [
                    pool.submit(client.request, method, path, params=params, json=body)
                    for method, path, params, body, _, _ in requests
                ]
                assert entered.wait(4), f"fewer than five repository calls entered: {operation_threads}"
                assert heartbeat_advanced.wait(2), "event-loop heartbeat stopped while SQLite calls were blocked"
                release.set()
                responses = [future.result(timeout=10) for future in futures]
        finally:
            release.set()

    for response, request in zip(responses, requests, strict=True):
        _, _, _, _, expected_hash, expected_headers = request
        assert response.status_code == 200
        assert hashlib.sha256(response.content).hexdigest() == expected_hash
        assert {header: response.headers[header] for header in expected_headers} == expected_headers

    assert len(operation_threads) == 5
    assert len(set(operation_threads)) == 5
    assert event_loop_thread not in operation_threads
    assert heartbeat_count >= 10


@pytest.mark.parametrize(
    ("path", "repository_name", "request_body"),
    [
        (
            "/api/v1/tenants/test-tenant/export",
            "chargebacks",
            b'{"start_date":"2026-02-01","end_date":"2026-02-28"}',
        ),
        ("/api/v1/tenants/test-tenant/topic-attributions/export", "topic_attributions", b""),
    ],
)
def test_csv_timeout_before_headers_closes_route_lease_and_uow(
    path: str,
    repository_name: str,
    request_body: bytes,
    settings_with_tenant: AppSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = settings_with_tenant.model_copy(
        update={"api": settings_with_tenant.api.model_copy(update={"request_timeout_seconds": 1})}
    )
    app = create_app(settings)
    setup_entered = threading.Event()
    release_setup = threading.Event()
    uow_closed = threading.Event()
    events: list[tuple[str, int]] = []
    uow = MagicMock()
    getattr(uow, repository_name).iter_by_filters.return_value = iter([])

    class BlockingBackend:
        def create_read_only_unit_of_work(self) -> Any:
            events.append(("uow-create", threading.get_ident()))

            @contextmanager
            def blocked_uow() -> Iterator[MagicMock]:
                events.append(("uow-enter", threading.get_ident()))
                setup_entered.set()
                if not release_setup.wait(5):
                    raise TimeoutError("test setup gate was not released")
                try:
                    yield uow
                finally:
                    events.append(("uow-close", threading.get_ident()))
                    uow_closed.set()

            return blocked_uow()

    backend = BlockingBackend()

    async def exercise_request() -> None:
        async with app.router.lifespan_context(app):
            provider = install_backend(app, "test-tenant", cast("StorageBackend", backend))
            original_acquire = provider.acquire_backend

            @contextmanager
            def acquire_backend(tenant_name: str, tenant_config: Any) -> Iterator[BlockingBackend]:
                events.append(("lease-enter", threading.get_ident()))
                try:
                    with original_acquire(tenant_name, tenant_config):
                        yield backend
                finally:
                    events.append(("lease-exit", threading.get_ident()))

            monkeypatch.setattr(provider, "acquire_backend", acquire_backend)
            event_loop_thread = threading.get_ident()
            request_messages: asyncio.Queue[Message] = asyncio.Queue()
            request_messages.put_nowait({"type": "http.request", "body": request_body, "more_body": False})
            request_headers = [(b"host", b"testserver")]
            if request_body:
                request_headers.extend(
                    [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(request_body)).encode()),
                    ]
                )
            response_messages: list[Message] = []
            response_complete = asyncio.Event()

            async def receive() -> Message:
                return await request_messages.get()

            async def send(message: Message) -> None:
                response_messages.append(message)
                if message.get("type") == "http.response.body" and message.get("more_body") is False:
                    response_complete.set()

            scope: Scope = {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.4"},
                "http_version": "1.1",
                "method": "POST",
                "scheme": "http",
                "path": path,
                "raw_path": path.encode(),
                "query_string": b"",
                "root_path": "",
                "headers": request_headers,
                "client": ("127.0.0.1", 12345),
                "server": ("testserver", 80),
                "state": {},
            }
            task = asyncio.create_task(app(scope, receive, send))
            try:
                assert await asyncio.to_thread(setup_entered.wait, 3), "route worker did not enter UoW setup"
                await asyncio.wait_for(response_complete.wait(), timeout=3)
                start = next(message for message in response_messages if message["type"] == "http.response.start")
                assert start["status"] == 504
                response_headers = dict(start["headers"])
                assert b"content-disposition" not in response_headers
                body = b"".join(
                    message.get("body", b"") for message in response_messages if message["type"] == "http.response.body"
                )
                assert body == b'{"detail":"Request exceeded 1s timeout"}'
                assert not uow_closed.is_set()
            finally:
                release_setup.set()
                assert await asyncio.to_thread(uow_closed.wait, 3), "timed-out route worker did not close its UoW"

            await asyncio.wait_for(task, timeout=3)
            assert [name for name, _ in events] == [
                "lease-enter",
                "uow-create",
                "uow-enter",
                "uow-close",
                "lease-exit",
            ]
            worker_threads = {thread for _, thread in events}
            assert len(worker_threads) == 1
            assert worker_threads.isdisjoint({event_loop_thread, threading.get_ident()})

    asyncio.run(exercise_request())
