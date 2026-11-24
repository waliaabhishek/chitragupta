#!/usr/bin/env bash
set -euo pipefail
if [[ $# -gt 1 || ( $# -eq 1 && $1 != --trino ) ]]; then
    echo "Usage: $0 [--trino]" >&2
    exit 2
fi
scratch=$(mktemp -d "${TMPDIR:-/tmp}/chitragupta-iceberg.XXXXXXXX")
project=$(basename "$scratch" | tr '[:upper:].' '[:lower:]-')
export BUILDX_CONFIG="$scratch/buildx"
compose=(docker compose -p "$project" -f "$(dirname "$0")/compose.yaml")
if [[ ${1:-} == --trino ]]; then
    compose+=(-f "$(dirname "$0")/compose.trino.yaml")
fi

cleanup() {
    result=$?
    if ! "${compose[@]}" down --volumes --remove-orphans; then
        result=1
    fi
    rm -rf "$scratch"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up --build --abort-on-container-exit --exit-code-from checks
