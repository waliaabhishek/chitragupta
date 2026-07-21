from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
import time
import uuid
import zipfile
from collections.abc import Mapping
from pathlib import Path
from typing import IO, Any
from urllib.parse import urljoin, urlparse, urlunparse

import httpx as _httpx

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = frozenset({"ready", "failed", "expired"})
_ARCHIVE_SPOOL_BYTES = 8 * 1024 * 1024
_COPY_CHUNK_BYTES = 64 * 1024


class _HttpxNamespace:
    Client: type[_httpx.Client] = _httpx.Client


httpx = _HttpxNamespace()


class _IntegrityError(RuntimeError):
    """Downloaded package bytes do not match their declared metadata."""


def _headers(values: list[str]) -> tuple[tuple[str, str], ...]:
    result: list[tuple[str, str]] = []
    for value in values:
        if "=" not in value:
            raise ValueError("header must use NAME=VALUE")
        name, header_value = value.split("=", 1)
        if not name:
            raise ValueError("header name must not be empty")
        result.append((name, header_value))
    return tuple(result)


def _download_url(api_url: str, supplied: str) -> str:
    api = urlparse(api_url)
    resolved = urljoin(f"{api.scheme}://{api.netloc}{api.path.rstrip('/')}/", supplied)
    target = urlparse(resolved)
    if (target.scheme, target.netloc) != (api.scheme, api.netloc):
        raise ValueError("API returned a cross-origin download URL")
    return resolved


def _sanitized_url(value: str) -> str:
    parsed = urlparse(value)
    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    try:
        port = parsed.port
    except ValueError:
        port = None
    netloc = f"{host}:{port}" if port is not None else host
    return urlunparse((parsed.scheme, netloc, parsed.path, "", "", ""))


def _raise_for_status(response: _httpx.Response) -> None:
    if response.is_error:
        safe_url = _sanitized_url(str(response.request.url))
        raise RuntimeError(f"{response.request.method} {safe_url} returned HTTP {response.status_code}")


def _local_path(value: str) -> Path:
    parsed = urlparse(value)
    if parsed.scheme or parsed.netloc:
        raise argparse.ArgumentTypeError("output must be a local path")
    if value == "-":
        raise argparse.ArgumentTypeError("output must be a local path")
    return Path(value)


def _add_connection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--header", action="append", default=[])


def _add_result_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="chitragupta-preview")
    subparsers = parser.add_subparsers(dest="command", required=True)

    daily = subparsers.add_parser("daily-full")
    _add_connection_arguments(daily)
    daily.add_argument("--start-date", required=True)
    daily.add_argument("--end-date", required=True)
    daily.add_argument("--output-dir", required=True, type=_local_path)

    request = subparsers.add_parser("request")
    _add_connection_arguments(request)
    request.add_argument("--month")
    request.add_argument("--start-date")
    request.add_argument("--end-date")
    request.add_argument("--column-profile", choices=("full", "summary", "custom"), default="full")
    request.add_argument("--column", action="append")
    request.add_argument("--no-wait", action="store_true")
    _add_result_arguments(request)
    request_output = request.add_mutually_exclusive_group()
    request_output.add_argument("--output-dir", type=_local_path)
    request_output.add_argument("--archive")

    status = subparsers.add_parser("status")
    _add_connection_arguments(status)
    status.add_argument("request_id")
    status.add_argument("--wait", action="store_true")
    _add_result_arguments(status)

    download = subparsers.add_parser("download")
    _add_connection_arguments(download)
    download.add_argument("request_id")
    download_output = download.add_mutually_exclusive_group(required=True)
    download_output.add_argument("--output-dir", type=_local_path)
    download_output.add_argument("--file")
    download_output.add_argument("--archive")
    download.add_argument("--output", type=_local_path)
    _add_result_arguments(download)
    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.command == "request":
        has_daily = args.start_date is not None or args.end_date is not None
        if (args.month is None) == (not has_daily):
            parser.error("request requires either --month or --start-date with --end-date")
        if has_daily and (args.start_date is None or args.end_date is None):
            parser.error("Daily request requires both --start-date and --end-date")
        if args.column and args.column_profile != "custom":
            parser.error("--column may be supplied only with --column-profile custom")
        if args.no_wait and (args.output_dir is not None or args.archive is not None):
            parser.error("--no-wait cannot be combined with download output")
        if args.archive == "-" and args.json:
            parser.error("--json cannot be combined with archive stdout")
        if args.archive is not None and args.archive != "-":
            args.archive = _parse_local_archive(parser, args.archive)
    elif args.command == "download":
        if (args.file is None) != (args.output is None):
            parser.error("--file and --output must be supplied together")
        if args.archive == "-" and args.json:
            parser.error("--json cannot be combined with archive stdout")
        if args.archive is not None and args.archive != "-":
            args.archive = _parse_local_archive(parser, args.archive)


def _parse_local_archive(parser: argparse.ArgumentParser, value: str) -> Path:
    try:
        return _local_path(value)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))


def _request_body(args: argparse.Namespace) -> dict[str, object]:
    if args.command == "daily-full":
        return {
            "grain": "daily",
            "start_date": args.start_date,
            "end_date": args.end_date,
            "column_profile": "full",
        }
    if args.month is not None:
        body: dict[str, object] = {
            "grain": "monthly",
            "month": args.month,
            "column_profile": args.column_profile,
        }
    else:
        body = {
            "grain": "daily",
            "start_date": args.start_date,
            "end_date": args.end_date,
            "column_profile": args.column_profile,
        }
    if args.column_profile == "custom":
        body["columns"] = args.column or []
    return body


def _get_status(
    client: _httpx.Client,
    *,
    status_url: str,
    headers: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    response = client.get(status_url, headers=headers)
    _raise_for_status(response)
    value = response.json()
    if not isinstance(value, dict):
        raise RuntimeError("FOCUS Mapping Preview API returned an invalid status response")
    return value


def _wait_for_terminal(
    client: _httpx.Client,
    *,
    status: dict[str, Any],
    status_url: str,
    headers: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    while status.get("status") not in _TERMINAL_STATUSES:
        time.sleep(1)
        status = _get_status(client, status_url=status_url, headers=headers)
    return status


def _print_json(value: Mapping[str, object]) -> None:
    print(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _print_summary(status: Mapping[str, object]) -> None:
    print(f"{status.get('request_id', '')} {status.get('status', '')}".strip())


def _report_terminal_failure(status: Mapping[str, object], *, json_output: bool = False) -> int:
    if json_output:
        _print_json(status)
    if status.get("status") == "failed":
        diagnostic = status.get("diagnostic")
        if isinstance(diagnostic, Mapping):
            print(
                f"Preview failed [{diagnostic.get('code', 'unknown')}]: {diagnostic.get('message', '')}",
                file=sys.stderr,
            )
            correlations = diagnostic.get("source_correlation_ids", [])
            if isinstance(correlations, list):
                for correlation in correlations:
                    print(f"Source correlation: {correlation}", file=sys.stderr)
        else:
            print("Preview failed", file=sys.stderr)
    elif status.get("status") == "expired":
        print(f"Preview expired: {status.get('request_id', '')}", file=sys.stderr)
    else:
        print(f"Preview is not ready: {status.get('status', 'unknown')}", file=sys.stderr)
    return 1


def _metadata(artifact: object) -> dict[str, object]:
    if not isinstance(artifact, dict):
        raise _IntegrityError("Package metadata is invalid")
    return artifact


def _safe_artifact_name(value: object) -> str:
    if not isinstance(value, str) or not value or Path(value).name != value or value in {".", ".."}:
        raise _IntegrityError("Package contains an unsafe artifact name")
    return value


def _expected_digest(metadata: Mapping[str, object]) -> tuple[int, str]:
    size = metadata.get("size_bytes")
    digest = metadata.get("sha256")
    if not isinstance(size, int) or size < 0:
        raise _IntegrityError("Package contains invalid artifact size metadata")
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or digest != digest.lower()
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise _IntegrityError("Package contains invalid artifact checksum metadata")
    return size, digest


def _verify_bytes(body: bytes, metadata: Mapping[str, object]) -> None:
    expected_size, expected_sha256 = _expected_digest(metadata)
    if len(body) != expected_size or hashlib.sha256(body).hexdigest() != expected_sha256:
        raise _IntegrityError("Downloaded artifact failed checksum verification")


def _package_metadata(status: Mapping[str, object]) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    package = status.get("package")
    if not isinstance(package, dict):
        raise RuntimeError("Ready preview response has no package metadata")
    manifest = _metadata(package.get("manifest"))
    if _safe_artifact_name(manifest.get("name")) != "manifest.json":
        raise _IntegrityError("Package manifest name is invalid")
    files_value = package.get("files")
    if not isinstance(files_value, list) or not files_value:
        raise _IntegrityError("Package file metadata is invalid")
    files = tuple(_metadata(item) for item in files_value)
    names = tuple(_safe_artifact_name(item.get("name")) for item in files)
    if len(set(names)) != len(names):
        raise _IntegrityError("Package contains duplicate artifact names")
    return manifest, files


def _manifest_files(
    body: bytes,
    *,
    status: Mapping[str, object],
    status_files: tuple[dict[str, object], ...],
) -> tuple[dict[str, object], ...]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _IntegrityError("Downloaded manifest is invalid") from exc
    if not isinstance(value, dict) or value.get("request_id") != status.get("request_id"):
        raise _IntegrityError("Downloaded manifest does not match the requested package")
    files_value = value.get("files")
    if not isinstance(files_value, list):
        raise _IntegrityError("Downloaded manifest file metadata is invalid")
    files = tuple(_metadata(item) for item in files_value)
    keys = ("name", "media_type", "size_bytes", "sha256", "order")
    actual = tuple(tuple(item.get(key) for key in keys) for item in files)
    expected = tuple(tuple(item.get(key) for key in keys) for item in status_files)
    if actual != expected:
        raise _IntegrityError("Downloaded manifest metadata does not match package status")
    for index, item in enumerate(files, 1):
        _safe_artifact_name(item.get("name"))
        _expected_digest(item)
        if item.get("order") != index:
            raise _IntegrityError("Downloaded manifest file order is invalid")
    return files


def _download_bytes(
    client: _httpx.Client,
    *,
    api_url: str,
    artifact: Mapping[str, object],
    headers: tuple[tuple[str, str], ...],
) -> bytes:
    supplied_url = artifact.get("download_url")
    if not isinstance(supplied_url, str):
        raise _IntegrityError("Package download URL is invalid")
    target_url = _download_url(api_url, supplied_url)
    response = client.get(target_url, headers=headers)
    _raise_for_status(response)
    body = response.content
    _verify_bytes(body, artifact)
    return body


def _download_to_path(
    client: _httpx.Client,
    *,
    api_url: str,
    artifact: Mapping[str, object],
    headers: tuple[tuple[str, str], ...],
    target: Path,
) -> None:
    supplied_url = artifact.get("download_url")
    if not isinstance(supplied_url, str):
        raise _IntegrityError("Package download URL is invalid")
    expected_size, expected_sha256 = _expected_digest(artifact)
    target_url = _download_url(api_url, supplied_url)
    size = 0
    digest = hashlib.sha256()
    with client.stream("GET", target_url, headers=headers) as response:
        _raise_for_status(response)
        with target.open("xb") as destination:
            for chunk in response.iter_bytes(_COPY_CHUNK_BYTES):
                destination.write(chunk)
                size += len(chunk)
                digest.update(chunk)
            destination.flush()
            os.fsync(destination.fileno())
    if size != expected_size or digest.hexdigest() != expected_sha256:
        raise _IntegrityError("Downloaded artifact failed checksum verification")


def _write_atomic(target: Path, body: bytes | IO[bytes]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w+b",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            if isinstance(body, bytes):
                temporary.write(body)
            else:
                body.seek(0)
                shutil.copyfileobj(body, temporary, length=_COPY_CHUNK_BYTES)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, target)
        temporary_name = None
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)


def _verified_manifest(
    client: _httpx.Client,
    *,
    api_url: str,
    status: Mapping[str, object],
    headers: tuple[tuple[str, str], ...],
) -> tuple[bytes, tuple[dict[str, object], ...]]:
    manifest, files = _package_metadata(status)
    manifest_body = _download_bytes(client, api_url=api_url, artifact=manifest, headers=headers)
    manifest_files = _manifest_files(manifest_body, status=status, status_files=files)
    return manifest_body, manifest_files


def _download_directory(
    client: _httpx.Client,
    *,
    api_url: str,
    status: Mapping[str, object],
    headers: tuple[tuple[str, str], ...],
    output_dir: Path,
) -> None:
    manifest, status_files = _package_metadata(status)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            dir=output_dir.parent,
            prefix=f".{output_dir.name}.",
            suffix=".tmp",
        )
    )
    backup: Path | None = None
    try:
        manifest_name = _safe_artifact_name(manifest.get("name"))
        manifest_path = staging / manifest_name
        _download_to_path(
            client,
            api_url=api_url,
            artifact=manifest,
            headers=headers,
            target=manifest_path,
        )
        manifest_files = _manifest_files(
            manifest_path.read_bytes(),
            status=status,
            status_files=status_files,
        )
        if len(status_files) != len(manifest_files):
            raise _IntegrityError("Downloaded package file count is invalid")
        for artifact in status_files:
            _download_to_path(
                client,
                api_url=api_url,
                artifact=artifact,
                headers=headers,
                target=staging / _safe_artifact_name(artifact.get("name")),
            )
        if output_dir.exists() and not output_dir.is_dir():
            raise FileExistsError(f"Output directory target is not a directory: {output_dir}")
        if output_dir.exists():
            backup = output_dir.parent / f".{output_dir.name}.{uuid.uuid4().hex}.backup"
            os.replace(output_dir, backup)
        try:
            os.replace(staging, output_dir)
        except Exception:
            if backup is not None:
                os.replace(backup, output_dir)
                backup = None
            raise
        if backup is not None:
            shutil.rmtree(backup)
            backup = None
    finally:
        if staging.exists():
            shutil.rmtree(staging)
        if backup is not None and backup.exists() and not output_dir.exists():
            os.replace(backup, output_dir)


def _download_file(
    client: _httpx.Client,
    *,
    api_url: str,
    status: Mapping[str, object],
    headers: tuple[tuple[str, str], ...],
    file_name: str,
    output: Path,
) -> None:
    _manifest_body, manifest_files = _verified_manifest(
        client,
        api_url=api_url,
        status=status,
        headers=headers,
    )
    selected = next((item for item in manifest_files if item.get("name") == file_name), None)
    if selected is None:
        raise _IntegrityError("Requested file is not enumerated by the package manifest")
    package = status["package"]
    assert isinstance(package, dict)
    status_files = tuple(_metadata(item) for item in package["files"])
    artifact = next(item for item in status_files if item.get("name") == file_name)
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
        )
    )
    try:
        temporary = staging / "download"
        _download_to_path(
            client,
            api_url=api_url,
            artifact=artifact,
            headers=headers,
            target=temporary,
        )
        os.replace(temporary, output)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _stream_archive(
    client: _httpx.Client,
    *,
    api_url: str,
    package: Mapping[str, object],
    headers: tuple[tuple[str, str], ...],
    spool: IO[bytes],
) -> None:
    supplied_url = package.get("download_all_url")
    if not isinstance(supplied_url, str):
        raise _IntegrityError("Package archive URL is invalid")
    target_url = _download_url(api_url, supplied_url)
    with client.stream("GET", target_url, headers=headers) as response:
        _raise_for_status(response)
        for chunk in response.iter_bytes(_COPY_CHUNK_BYTES):
            spool.write(chunk)
    spool.seek(0)


def _verify_archive(spool: IO[bytes], *, status: Mapping[str, object]) -> None:
    manifest, status_files = _package_metadata(status)
    manifest_name = _safe_artifact_name(manifest.get("name"))
    expected_names = [manifest_name, *(_safe_artifact_name(item.get("name")) for item in status_files)]
    try:
        with zipfile.ZipFile(spool) as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            if names != expected_names or len(names) != len(set(names)):
                raise _IntegrityError("Downloaded archive membership or order is invalid")
            for name in names:
                _safe_artifact_name(name)
            with archive.open(infos[0]) as manifest_source:
                manifest_body = manifest_source.read()
            _verify_bytes(manifest_body, manifest)
            manifest_files = _manifest_files(manifest_body, status=status, status_files=status_files)
            for info, metadata in zip(infos[1:], manifest_files, strict=True):
                expected_size, expected_sha256 = _expected_digest(metadata)
                size = 0
                digest = hashlib.sha256()
                with archive.open(info) as source:
                    while chunk := source.read(_COPY_CHUNK_BYTES):
                        size += len(chunk)
                        digest.update(chunk)
                if size != expected_size or digest.hexdigest() != expected_sha256:
                    raise _IntegrityError("Downloaded archive artifact failed checksum verification")
    except (zipfile.BadZipFile, OSError, EOFError) as exc:
        raise _IntegrityError("Downloaded archive is invalid") from exc
    finally:
        spool.seek(0)


def _download_archive(
    client: _httpx.Client,
    *,
    api_url: str,
    status: Mapping[str, object],
    headers: tuple[tuple[str, str], ...],
    destination: Path | str,
) -> None:
    package = status.get("package")
    if not isinstance(package, dict):
        raise RuntimeError("Ready preview response has no package metadata")
    with tempfile.SpooledTemporaryFile(max_size=_ARCHIVE_SPOOL_BYTES, mode="w+b") as spool:
        _stream_archive(client, api_url=api_url, package=package, headers=headers, spool=spool)
        _verify_archive(spool, status=status)
        if destination == "-":
            shutil.copyfileobj(spool, sys.stdout.buffer, length=_COPY_CHUNK_BYTES)
            sys.stdout.buffer.flush()
        else:
            assert isinstance(destination, Path)
            _write_atomic(destination, spool)


def _perform_download(
    client: _httpx.Client,
    *,
    args: argparse.Namespace,
    api_url: str,
    status: Mapping[str, object],
    headers: tuple[tuple[str, str], ...],
) -> None:
    if args.output_dir is not None:
        _download_directory(
            client,
            api_url=api_url,
            status=status,
            headers=headers,
            output_dir=args.output_dir,
        )
    elif getattr(args, "file", None) is not None:
        _download_file(
            client,
            api_url=api_url,
            status=status,
            headers=headers,
            file_name=args.file,
            output=args.output,
        )
    elif args.archive is not None:
        _download_archive(
            client,
            api_url=api_url,
            status=status,
            headers=headers,
            destination=args.archive,
        )


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    _validate_args(parser, args)
    try:
        headers = _headers(args.header)
        api_url = args.api_url.rstrip("/")
        base = f"{api_url}/tenants/{args.tenant}/focus-preview/requests"
        with httpx.Client(timeout=30.0) as client:
            if args.command in {"daily-full", "request"}:
                response = client.post(base, headers=headers, json=_request_body(args))
                _raise_for_status(response)
                status = response.json()
                if not isinstance(status, dict):
                    raise RuntimeError("FOCUS Mapping Preview API returned an invalid status response")
                if args.command == "request" and args.no_wait:
                    if args.json:
                        _print_json(status)
                    else:
                        print(status.get("request_id", ""))
                    return 0
                status_url = f"{base}/{status['request_id']}"
                status = _wait_for_terminal(client, status=status, status_url=status_url, headers=headers)
                if status.get("status") != "ready":
                    return _report_terminal_failure(status, json_output=getattr(args, "json", False))
                if args.command == "daily-full":
                    _download_directory(
                        client,
                        api_url=api_url,
                        status=status,
                        headers=headers,
                        output_dir=args.output_dir,
                    )
                    return 0
                _perform_download(client, args=args, api_url=api_url, status=status, headers=headers)
                if args.json:
                    _print_json(status)
                elif args.output_dir is None and args.archive is None:
                    _print_summary(status)
                return 0

            status_url = f"{base}/{args.request_id}"
            status = _get_status(client, status_url=status_url, headers=headers)
            if args.command == "status":
                if args.wait:
                    status = _wait_for_terminal(client, status=status, status_url=status_url, headers=headers)
                if status.get("status") in {"failed", "expired"}:
                    return _report_terminal_failure(status, json_output=args.json)
                if args.json:
                    _print_json(status)
                else:
                    _print_summary(status)
                return 0

            if status.get("status") != "ready":
                return _report_terminal_failure(status, json_output=args.json)
            _perform_download(client, args=args, api_url=api_url, status=status, headers=headers)
            if args.json:
                _print_json(status)
            return 0
    except _IntegrityError as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except _httpx.HTTPError as exc:
        request = getattr(exc, "request", None)
        if request is None:
            message = "FOCUS Mapping Preview API request failed"
        else:
            message = f"{request.method} {_sanitized_url(str(request.url))} failed"
        print(message, file=sys.stderr)
        return 1
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
