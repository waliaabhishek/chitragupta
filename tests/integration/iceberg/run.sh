#!/usr/bin/env bash
set -euo pipefail
if [[ $# -gt 1 || ( $# -eq 1 && $1 != --trino && $1 != --example && $1 != --public-api && $1 != --microbatch ) ]]; then
    echo "Usage: $0 [--trino | --example | --public-api | --microbatch]" >&2
    exit 2
fi
scratch=$(mktemp -d "${TMPDIR:-/tmp}/chitragupta-iceberg.XXXXXXXX")
project=$(basename "$scratch" | tr '[:upper:].' '[:lower:]-')
export BUILDX_CONFIG="$scratch/buildx"
compose=(docker compose -p "$project" -f "$(dirname "$0")/compose.yaml")
if [[ ${1:-} == --public-api || ${1:-} == --microbatch ]]; then
    export ICEBERG_PUBLIC_MODE="$1"
    compose+=(-f "$(dirname "$0")/compose.trino.yaml" -f "$(dirname "$0")/compose.public.yaml")
elif [[ ${1:-} == --trino ]]; then
    compose+=(-f "$(dirname "$0")/compose.trino.yaml")
elif [[ ${1:-} == --example ]]; then
    export ICEBERG_EXAMPLE=1
    compose+=(-f "$(dirname "$0")/compose.trino.yaml")
fi

exec python3 "$(dirname "$0")/compose_deadline.py" "$scratch" "${compose[@]}"
