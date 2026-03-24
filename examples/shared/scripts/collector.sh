#!/bin/sh
# Prometheus collector for Chitragupta chargeback metrics.
# Scrapes /metrics in OpenMetrics format, writes TSDB blocks via promtool.
# This script requires the Chitragupta metrics server to serve OpenMetrics format.
# prometheus_client serves OpenMetrics when the Accept header requests it; this
# script always requests OpenMetrics. If you need Prometheus text format, use a
# standard Prometheus scraper instead — text format uses millisecond timestamps
# which promtool misinterprets as seconds.
#
# Required env vars:
#   CHITRAGUPTA_METRICS_URL     — URL to /metrics endpoint (e.g. http://localhost:9090/metrics)
#   CHITRAGUPTA_HEALTH_URL      — URL to /health endpoint  (e.g. http://localhost:8080/health)
#   TSDB_OUT_DIR               — output directory for TSDB blocks (default: /data/prometheus)
#
# Optional env vars:
#   CHITRAGUPTA_METRICS_FORMAT  — "openmetrics" (default) or "text".
#                                "text" is incompatible with this script and
#                                causes an immediate exit with an error.
#
# Modes:
#   catch-up (fast): when scraped data timestamp is >5 days old — polls every 1s
#   current (slow):  when scraped data timestamp is recent       — polls every 600s

set -e

METRICS_URL="${CHITRAGUPTA_METRICS_URL:-http://localhost:9090/metrics}"
HEALTH_URL="${CHITRAGUPTA_HEALTH_URL:-http://localhost:8080/health}"
TSDB_OUT_DIR="${TSDB_OUT_DIR:-/data/prometheus}"
METRICS_FORMAT="${CHITRAGUPTA_METRICS_FORMAT:-openmetrics}"
CATCHUP_CUTOFF_DAYS=5
SLOW_INTERVAL=600

# Validate format — this script only supports OpenMetrics.
# Prometheus text format uses millisecond timestamps; promtool expects seconds.
if [ "${METRICS_FORMAT}" = "openmetrics" ]; then
    ACCEPT_HEADER="Accept: application/openmetrics-text; version=1.0.0"
elif [ "${METRICS_FORMAT}" = "text" ]; then
    echo "ERROR: CHITRAGUPTA_METRICS_FORMAT=text is incompatible with this collector script." >&2
    echo "       Prometheus text format uses millisecond timestamps; promtool expects seconds." >&2
    echo "       Use a standard Prometheus scraper instead." >&2
    exit 1
else
    echo "ERROR: Unknown CHITRAGUPTA_METRICS_FORMAT=${METRICS_FORMAT}. Must be 'openmetrics' or 'text'." >&2
    exit 1
fi

wait_for_ready () {
    echo "Waiting for Chitragupta readiness at ${HEALTH_URL} ..."
    while true; do
        status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "${HEALTH_URL}" 2>/dev/null || echo "000")
        if [ "${status}" = "200" ]; then
            echo "Ready."
            return
        fi
        sleep 3
    done
}

scrape_interval_seconds () {
    # Determine polling mode by checking whether the most recent metric timestamp
    # is older than CATCHUP_CUTOFF_DAYS. Falls back to slow interval on parse error.
    # OpenMetrics timestamps are seconds (float) — compare integer part directly.
    cutoff=$(( $(date '+%s') - CATCHUP_CUTOFF_DAYS * 86400 ))
    ts=$(curl -s --max-time 10 -H "${ACCEPT_HEADER}" "${METRICS_URL}" 2>/dev/null \
        | grep -E '^chitragupta_chargeback_amount' \
        | head -1 \
        | awk '{print $NF}' \
        | cut -d'.' -f1)
    if [ -n "${ts}" ] && [ "${ts}" -gt "${cutoff}" ] 2>/dev/null; then
        echo ${SLOW_INTERVAL}
    else
        echo 1
    fi
}

mkdir -p "${TSDB_OUT_DIR}"

while true; do
    wait_for_ready

    interval=$(scrape_interval_seconds)

    tmp_metrics=$(mktemp /tmp/chitragupta_metrics_XXXXXX.txt)
    # generate_openmetrics_latest() already appends "# EOF" — do NOT add it
    # again or the file becomes invalid OpenMetrics.
    if curl -s --max-time 60 -H "${ACCEPT_HEADER}" "${METRICS_URL}" -o "${tmp_metrics}"; then
        promtool tsdb create-blocks-from openmetrics "${tmp_metrics}" "${TSDB_OUT_DIR}" \
            && echo "TSDB block written to ${TSDB_OUT_DIR}"
    else
        echo "Scrape failed — skipping this cycle"
    fi
    rm -f "${tmp_metrics}"

    sleep "${interval}"
done
