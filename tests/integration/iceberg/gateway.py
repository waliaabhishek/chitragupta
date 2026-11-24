from __future__ import annotations

import json
import select
import socket
import ssl
import subprocess
import threading
from collections.abc import Callable
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socketserver import StreamRequestHandler, ThreadingTCPServer
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

CATALOG_TOKEN = "fixture-catalog-bearer"
BUCKET = "warehouse"
NAMESPACE = "finance"
S3_HOST = "minio:9000"
S3_URL = f"http://{S3_HOST}"
REGION = "us-east-1"
ADMIN_KEY = "fixture-admin"
ADMIN_SECRET = "fixture-admin-password"  # pragma: allowlist secret
TABLES = ("alpha", "bravo")


def table_key(table: str) -> str:
    return f"fixture-{table}-key"


def table_secret(table: str) -> str:
    return f"fixture-{table}-s3-password"


def table_token(table: str) -> str:
    return f"fixture-{table}-bearer"


def table_prefix(table: str) -> str:
    return f"{BUCKET}/{NAMESPACE}/{table}/"


class CatalogGateway(ThreadingHTTPServer):
    def __init__(self, ca: Path) -> None:
        super().__init__(("127.0.0.1", 0), CatalogHandler)
        certificate_key = ca.with_suffix(".key")
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-keyout",
                str(certificate_key),
                "-out",
                str(ca),
            ],
            check=True,
            capture_output=True,
            timeout=10,
        )
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(ca, certificate_key)
        self.socket = context.wrap_socket(self.socket, server_side=True)
        self.uri = f"https://localhost:{self.server_port}"
        self.ca = ca
        self.requests: list[tuple[str, str, str | None]] = []
        self.commit_payloads: list[tuple[str, dict[str, object]]] = []
        self.commit_responses: list[tuple[str, int]] = []
        self.next_commit_fault: str | None = None
        self.before_next_commit: Callable[[str, dict[str, object]], None] | None = None
        self.lost_commit_metadata: str | None = None
        self.stall_lookup = False
        self.lookup_received = threading.Event()
        self.release_lookup = threading.Event()


class CatalogHandler(BaseHTTPRequestHandler):
    server: CatalogGateway

    def _reply(self, code: int, payload: bytes) -> None:
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def _forward(self) -> None:
        path = urlsplit(self.path).path
        size = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(size) if size else None
        if path in ("/api/v1/query", "/api/v1/query_range"):
            self._reply(200, b'{"status":"success","data":{"resultType":"matrix","result":[]}}')
            return

        for table in TABLES:
            if path.endswith(f"/namespaces/{NAMESPACE}/tables/{table}"):
                break
        else:
            table = ""
        commit = bool(table and self.command == "POST")
        auth = self.headers.get("Authorization")
        self.server.requests.append((self.command, table, auth))
        expected = table_token(table) if commit else CATALOG_TOKEN
        if auth != f"Bearer {expected}":
            self._reply(401, b'{"error":{"message":"Wrong bearer","type":"UnauthorizedException","code":401}}')
            return
        if self.command == "GET" and table == "alpha" and self.server.stall_lookup:
            self.server.lookup_received.set()
            self.server.release_lookup.wait(timeout=30)
            return

        fault = None
        if commit:
            payload_json = json.loads(body) if body is not None else {}
            self.server.commit_payloads.append((table, payload_json))
            callback = self.server.before_next_commit
            self.server.before_next_commit = None
            if callback is not None:
                callback(table, payload_json)
        if commit and table == "alpha":
            fault = self.server.next_commit_fault
            self.server.next_commit_fault = None
        if fault == "reject":
            self.server.commit_responses.append((table, 409))
            self._reply(409, b'{"error":{"message":"Test conflict","type":"CommitFailedException","code":409}}')
            return

        headers = {key: self.headers[key] for key in ("Accept", "Content-Type") if key in self.headers}
        upstream = HTTPConnection("catalog", 8181, timeout=8)
        try:
            upstream.request(self.command, self.path, body=body, headers=headers)
            response = upstream.getresponse()
            payload = response.read()
        finally:
            upstream.close()

        if response.status == 200 and fault == "lose-response":
            self.server.commit_responses.append((table, response.status))
            self.server.lost_commit_metadata = json.loads(payload)["metadata-location"]
            self.close_connection = True
            return
        if commit:
            self.server.commit_responses.append((table, response.status))
        if response.status == 200 and table and self.command == "GET":
            loaded = json.loads(payload)
            loaded.setdefault("config", {})["token"] = table_token(table)
            if "vended-credentials" in self.headers.get("X-Iceberg-Access-Delegation", ""):
                loaded["storage-credentials"] = [
                    {
                        "prefix": f"s3://{table_prefix(table)}",
                        "config": {
                            "s3.access-key-id": table_key(table),
                            "s3.secret-access-key": table_secret(table),
                        },
                    }
                ]
            payload = json.dumps(loaded).encode()
        self._reply(response.status, payload)

    do_GET = do_POST = do_HEAD = _forward  # noqa: N815 — BaseHTTPRequestHandler dispatch

    def log_message(self, *args: object) -> None:
        pass


@contextmanager
def catalog_gateway(ca: Path) -> Iterator[CatalogGateway]:
    with CatalogGateway(ca) as gateway:
        worker = threading.Thread(target=gateway.serve_forever)
        worker.start()
        try:
            yield gateway
        finally:
            gateway.release_lookup.set()
            gateway.shutdown()
            worker.join()


class StorageHandler(StreamRequestHandler):
    server: StorageGateway
    rbufsize = 0

    def handle(self) -> None:
        line = self.rfile.readline(4096)
        if not line:
            return
        if not self.server.release.is_set() and f"/{table_prefix('alpha')}".encode() in line:
            self.server.stalled.set()
            self.server.release.wait(timeout=15)
            return

        # Preserve the original bytes, including the signed S3 Host header.
        with socket.create_connection(("minio", 9000), timeout=5) as upstream:
            upstream.sendall(line)
            peers = {self.request: upstream, upstream: self.request}
            while ready := select.select(list(peers), [], [], 5)[0]:
                for stream in ready:
                    if not (data := stream.recv(65536)):
                        return
                    peers[stream].sendall(data)


class StorageGateway(ThreadingTCPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), StorageHandler)
        self.endpoint = f"http://127.0.0.1:{self.server_address[1]}"
        self.stalled = threading.Event()
        self.release = threading.Event()


@contextmanager
def storage_gateway() -> Iterator[StorageGateway]:
    with StorageGateway() as gateway:
        worker = threading.Thread(target=gateway.serve_forever)
        worker.start()
        try:
            yield gateway
        finally:
            gateway.release.set()
            gateway.shutdown()
            worker.join()
