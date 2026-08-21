#!/usr/bin/env bash
set -u -o pipefail

readonly LAB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
readonly COMPOSE_COMMAND=${COMPOSE:-docker-compose}
readonly VALIDATION_DETAILS="$LAB_DIR/evidence/latest/validator-result.json"

cd "$LAB_DIR"

fail() {
  local code=$1
  local prefix=$2
  shift 2
  printf '%s %s\n' "$prefix" "$*" >&2
  exit "$code"
}

validation_preflight_fail() {
  local category=$1
  local message=$2
  local artifact=${3:-null}
  local result_dir="$LAB_DIR/evidence/validation-preflight-$(date -u +%Y%m%dT%H%M%SZ)"
  mkdir -p "$result_dir"
  printf '{"status":"fail","failures":[{"category":"%s","message":"%s","artifact":%s}]}\n' \
    "$category" "$message" "$artifact" >"$result_dir/validator-result.json"
  rm -f -- "$LAB_DIR/evidence/latest"
  ln -s "$(basename "$result_dir")" "$LAB_DIR/evidence/latest"
  fail 7 'validation_failed:' "$message; details=$VALIDATION_DETAILS"
}

principal_contract_preflight() {
  local status
  uv run python -c '
import sys
sys.path.insert(0, "scripts")
import validate_evidence
contract, failure = validate_evidence._principal_contract()
if failure is not None:
    raise SystemExit(10 if failure.message.endswith("absent") else 11)
raise SystemExit(0 if validate_evidence._principal_contract_failure(contract) is None else 11)
' >/dev/null 2>&1
  status=$?
  case $status in
    0)
      return
      ;;
    10)
      validation_preflight_fail principal_contract 'principal allocation contract is absent' '"principal-allocation-contract.yaml"'
      ;;
    *)
      validation_preflight_fail principal_contract 'principal allocation contract is invalid' '"principal-allocation-contract.yaml"'
      ;;
  esac
}

run_validation_preflight() {
  local required_path
  for required_path in \
    .env \
    generated/generation.json \
    generated/prometheus.yml \
    generated/jmx-a.yml \
    generated/jmx-b.yml \
    contracts/metric-contract.yaml \
    workloads/workloads.yaml; do
    [[ -f $required_path ]] || validation_preflight_fail missing_metric "required local file is absent: $required_path"
  done

  uv run python -c 'import pathlib, yaml; payload = yaml.safe_load(pathlib.Path("contracts/metric-contract.yaml").read_text()); assert isinstance(payload, dict)' || \
    validation_preflight_fail wrong_type 'metric contract is not valid mapping YAML'

  local service
  for service in kafka-a kafka-b jmx-a jmx-b prometheus workload-a workload-b; do
    local container_id
    container_id=$("$COMPOSE_COMMAND" ps -q "$service")
    [[ -n $container_id ]] || validation_preflight_fail target_health "service is absent: $service"
    [[ $(docker inspect --format '{{.State.Running}}' "$container_id") == true ]] || \
      validation_preflight_fail target_health "service is not running: $service"
  done
  for service in kafka-a kafka-b; do
    local kafka_container_id
    kafka_container_id=$("$COMPOSE_COMMAND" ps -q "$service")
    [[ $(docker inspect --format '{{.State.Health.Status}}' "$kafka_container_id") == healthy ]] || \
      validation_preflight_fail target_health "Kafka service is not healthy: $service"
  done

  local port
  for port in "${JMX_EXPORTER_A_PORT:-7071}" "${JMX_EXPORTER_B_PORT:-7072}"; do
    curl --fail --silent --show-error "http://127.0.0.1:${port}/metrics" >/dev/null || \
      validation_preflight_fail target_health "JMX exporter is not scrapeable on port $port"
  done
  curl --fail --silent --show-error \
    "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1/query?query=up" >/dev/null || \
    validation_preflight_fail target_health 'Prometheus query API is not ready'
}

run_prereq() {
  command -v docker >/dev/null 2>&1 || fail 2 'prereq_failed:' 'docker is not installed'
  command -v "$COMPOSE_COMMAND" >/dev/null 2>&1 || fail 2 'prereq_failed:' "$COMPOSE_COMMAND is not installed"
  command -v uv >/dev/null 2>&1 || fail 2 'prereq_failed:' 'uv is not installed'
  docker info >/dev/null 2>&1 || fail 2 'prereq_failed:' 'Docker daemon is not accessible'
  "$COMPOSE_COMMAND" version >/dev/null 2>&1 || fail 2 'prereq_failed:' 'standalone Compose is not usable'
  printf 'prerequisites_ok\n'
}

generate_config() {
  uv run python scripts/generate_local_config.py || fail 3 'config_generation_failed:' 'could not create local configuration'
}

run_start() {
  run_prereq
  generate_config
  "$COMPOSE_COMMAND" up -d --build kafka-a kafka-b jmx-a jmx-b prometheus || \
    fail 4 'kafka_setup_failed:' 'core services did not start'
  "$COMPOSE_COMMAND" run --rm kafka-setup || fail 4 'kafka_setup_failed:' 'topics, users, or quotas were not configured'
  "$COMPOSE_COMMAND" up -d workload-a workload-b || fail 5 'workload_failed:' 'workload services did not start'
}

run_ready() {
  [[ -f .env && -f generated/generation.json ]] || fail 3 'config_generation_failed:' 'generated configuration is absent'
  "$COMPOSE_COMMAND" ps --status running kafka-a kafka-b jmx-a jmx-b prometheus workload-a workload-b >/dev/null || \
    fail 4 'kafka_setup_failed:' 'one or more services are not running'
  local service
  for service in kafka-a kafka-b; do
    [[ $("$COMPOSE_COMMAND" ps -q "$service") ]] || fail 4 'kafka_setup_failed:' "$service is absent"
    [[ $(docker inspect --format '{{.State.Health.Status}}' "$("$COMPOSE_COMMAND" ps -q "$service")") == healthy ]] || \
      fail 4 'kafka_setup_failed:' "$service is not healthy"
  done
  local port
  for port in "${JMX_EXPORTER_A_PORT:-7071}" "${JMX_EXPORTER_B_PORT:-7072}"; do
    curl --fail --silent --show-error "http://127.0.0.1:${port}/metrics" >/dev/null || \
      fail 4 'kafka_setup_failed:' "JMX exporter on port $port is not scrapeable"
  done
  curl --fail --silent --show-error \
    "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1/query?query=up" >/dev/null || \
    fail 4 'kafka_setup_failed:' 'Prometheus API is not ready'
  printf 'lab_ready\n'
}

run_workload() {
  local action=${1:-status}
  case "$action" in
    start)
      "$COMPOSE_COMMAND" up -d workload-a workload-b || fail 5 'workload_failed:' 'workloads did not start'
      ;;
    stop)
      "$COMPOSE_COMMAND" stop workload-a workload-b || fail 5 'workload_failed:' 'workloads did not stop'
      ;;
    status)
      "$COMPOSE_COMMAND" ps workload-a workload-b || fail 5 'workload_failed:' 'workload status is unavailable'
      ;;
    *)
      fail 5 'workload_failed:' 'usage: lab.sh workload {start|stop|status}'
      ;;
  esac
}

run_evidence() {
  uv run python scripts/capture_evidence.py "$@" || fail 6 'evidence_capture_failed:' 'live evidence capture failed'
}

run_validate() {
  local window=5m
  local require_recreated=false
  local principal_contract=false
  while (($#)); do
    case "$1" in
      --window)
        (($# >= 2)) || fail 7 'validation_failed:' "missing --window value; details=$VALIDATION_DETAILS"
        window=$2
        shift 2
        ;;
      --require-recreated-state)
        require_recreated=true
        shift
        ;;
      --principal-contract)
        principal_contract=true
        shift
        ;;
      *)
        fail 7 'validation_failed:' "unknown validation argument: $1; details=$VALIDATION_DETAILS"
        ;;
    esac
  done

  if [[ $principal_contract == true ]]; then
    principal_contract_preflight
  fi
  run_validation_preflight
  local capture_args=(--window "$window")
  if [[ $principal_contract == true ]]; then
    capture_args+=(--principal-contract)
  fi
  run_evidence "${capture_args[@]}"
  local args=(--evidence-dir "$LAB_DIR/evidence/latest")
  if [[ $require_recreated == true ]]; then
    args+=(--require-recreated-state)
  fi
  if [[ $principal_contract == true ]]; then
    args+=(--principal-contract)
  fi
  uv run python scripts/validate_evidence.py "${args[@]}" || \
    fail 7 'validation_failed:' "evidence did not satisfy the contract; details=$VALIDATION_DETAILS"
  printf 'validation_ok: %s\n' "$VALIDATION_DETAILS"
}

run_stop() {
  "$COMPOSE_COMMAND" stop workload-a workload-b prometheus jmx-a jmx-b kafka-a kafka-b || \
    fail 8 'cleanup_failed:' 'services did not stop'
}

run_cleanup() {
  local latest=""
  if [[ -L evidence/latest ]]; then
    latest=$(readlink -f evidence/latest)
  fi
  if [[ -f generated/generation.json ]]; then
    cp generated/generation.json .restart-state.json || fail 8 'cleanup_failed:' 'restart marker could not be recorded'
    chmod 600 .restart-state.json
  fi
  "$COMPOSE_COMMAND" down --volumes --remove-orphans || fail 8 'cleanup_failed:' 'containers or volumes remain'
  rm -rf -- "$LAB_DIR/generated"
  rm -f -- "$LAB_DIR/.env"
  if [[ -n $latest && -d $latest ]]; then
    printf '{"status":"complete","recorded_at":"%s","generated_secrets_removed":true,"containers_removed":true}\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$latest/cleanup-result.json" || \
      fail 8 'cleanup_failed:' 'cleanup evidence could not be recorded'
  fi
  printf 'cleanup_complete\n'
}

case "${1:-}" in
  prereq)
    run_prereq
    ;;
  start)
    run_start
    ;;
  ready)
    run_ready
    ;;
  workload)
    shift
    run_workload "$@"
    ;;
  validate)
    shift
    run_validate "$@"
    ;;
  evidence)
    shift
    run_evidence "$@"
    ;;
  stop)
    run_stop
    ;;
  cleanup)
    run_cleanup
    ;;
  *)
    printf 'usage: %s {prereq|start|ready|workload|validate|evidence|stop|cleanup}\n' "$0" >&2
    exit 2
    ;;
esac
