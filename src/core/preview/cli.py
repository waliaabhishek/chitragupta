from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

import httpx

logger = logging.getLogger(__name__)


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


def _raise_for_status(response: httpx.Response) -> None:
    if response.is_error:
        safe_url = _sanitized_url(str(response.request.url))
        raise RuntimeError(f"{response.request.method} {safe_url} returned HTTP {response.status_code}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="chitragupta-preview")
    subparsers = parser.add_subparsers(dest="command", required=True)
    daily = subparsers.add_parser("daily-full")
    daily.add_argument("--api-url", required=True)
    daily.add_argument("--tenant", required=True)
    daily.add_argument("--start-date", required=True)
    daily.add_argument("--end-date", required=True)
    daily.add_argument("--output-dir", required=True, type=Path)
    daily.add_argument("--header", action="append", default=[])
    request = subparsers.add_parser("request")
    request.add_argument("--api-url", required=True)
    request.add_argument("--tenant", required=True)
    request.add_argument("--month")
    request.add_argument("--start-date")
    request.add_argument("--end-date")
    request.add_argument("--column-profile", choices=("full", "summary", "custom"), default="full")
    request.add_argument("--column", action="append")
    request.add_argument("--output-dir", required=True, type=Path)
    request.add_argument("--header", action="append", default=[])
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command == "request":
        has_daily = args.start_date is not None or args.end_date is not None
        if (args.month is None) == (not has_daily):
            parser.error("request requires either --month or --start-date with --end-date")
        if has_daily and (args.start_date is None or args.end_date is None):
            parser.error("Daily request requires both --start-date and --end-date")
        if args.column and args.column_profile != "custom":
            parser.error("--column may be supplied only with --column-profile custom")
    try:
        headers = _headers(args.header)
        api_url = args.api_url.rstrip("/")
        base = f"{api_url}/tenants/{args.tenant}/focus-preview/requests"
        with httpx.Client(timeout=30.0) as client:
            if args.command == "daily-full":
                body: dict[str, object] = {
                    "grain": "daily",
                    "start_date": args.start_date,
                    "end_date": args.end_date,
                    "column_profile": "full",
                }
            elif args.month is not None:
                body = {
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
            if args.command == "request" and args.column_profile == "custom":
                body["columns"] = args.column or []
            response = client.post(
                base,
                headers=headers,
                json=body,
            )
            _raise_for_status(response)
            status = response.json()
            status_url = f"{base}/{status['request_id']}"
            while status["status"] not in {"ready", "failed"}:
                time.sleep(1)
                response = client.get(status_url, headers=headers)
                _raise_for_status(response)
                status = response.json()
            if status["status"] == "failed":
                diagnostic = status["diagnostic"]
                print(f"Preview failed [{diagnostic['code']}]: {diagnostic['message']}", file=sys.stderr)
                for correlation in diagnostic.get("source_correlation_ids", []):
                    print(f"Source correlation: {correlation}", file=sys.stderr)
                return 1
            package = status["package"]
            artifacts = [package["manifest"], *package["files"]]
            args.output_dir.mkdir(parents=True, exist_ok=True)
            for artifact in artifacts:
                target_url = _download_url(api_url, artifact["download_url"])
                response = client.get(target_url, headers=headers)
                _raise_for_status(response)
                (args.output_dir / artifact["name"]).write_bytes(response.content)
        return 0
    except httpx.HTTPError as exc:
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
