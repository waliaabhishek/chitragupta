#!/usr/bin/env bash
set -euo pipefail

readonly KAFKA_BIN=${KAFKA_BIN:-/opt/kafka/bin}
readonly GENERATED=${GENERATED:-/opt/lab/generated}

source "$GENERATED/runtime-plan.sh"

wait_for_cluster() {
  local bootstrap=$1
  local admin_properties=$2
  local attempt
  for attempt in $(seq 1 60); do
    if "$KAFKA_BIN/kafka-broker-api-versions.sh" \
      --bootstrap-server "$bootstrap" \
      --command-config "$admin_properties" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  printf 'cluster did not become ready: %s\n' "$bootstrap" >&2
  return 1
}

ensure_topics() {
  local bootstrap=$1
  local admin_properties=$2
  local topic
  for topic in "${LAB_TOPIC_NAMES[@]}"; do
    "$KAFKA_BIN/kafka-topics.sh" \
      --bootstrap-server "$bootstrap" \
      --command-config "$admin_properties" \
      --create --if-not-exists \
      --topic "$topic" \
      --partitions "$LAB_TOPIC_PARTITIONS" \
      --replication-factor 1 \
      --config "retention.ms=${LAB_TOPIC_RETENTION_MS}" \
      --config "retention.bytes=${LAB_TOPIC_RETENTION_BYTES}"
  done
}

set_quota() {
  local bootstrap=$1
  local admin_properties=$2
  local quota_limit=$3
  shift 3
  "$KAFKA_BIN/kafka-configs.sh" \
    --bootstrap-server "$bootstrap" \
    --command-config "$admin_properties" \
    --alter \
    --add-config "producer_byte_rate=${quota_limit},consumer_byte_rate=${quota_limit}" \
    "$@"
}

ensure_quotas() {
  local bootstrap=$1
  local admin_properties=$2
  local index
  for index in "${!LAB_PROFILE_NAMES[@]}"; do
    local scope=${LAB_PROFILE_SCOPES[$index]}
    local user=${LAB_PROFILE_USERS[$index]}
    local client_id=${LAB_PROFILE_CLIENT_IDS[$index]}
    local quota_limit=${LAB_PROFILE_QUOTA_LIMITS[$index]}
    case "$scope" in
      user)
        set_quota "$bootstrap" "$admin_properties" "$quota_limit" \
          --entity-type users --entity-name "$user"
        ;;
      client-id)
        set_quota "$bootstrap" "$admin_properties" "$quota_limit" \
          --entity-type clients --entity-name "$client_id"
        ;;
      user-client)
        set_quota "$bootstrap" "$admin_properties" "$quota_limit" \
          --entity-type users --entity-name "$user" \
          --entity-type clients --entity-name "$client_id"
        ;;
      *)
        printf 'unsupported quota scope in runtime plan: %s\n' "$scope" >&2
        return 1
        ;;
    esac
  done
}

configure_cluster() {
  local cluster=$1
  local bootstrap="kafka-${cluster}:9092"
  local admin_properties="$GENERATED/admin-${cluster}.properties"

  wait_for_cluster "$bootstrap" "$admin_properties"
  ensure_topics "$bootstrap" "$admin_properties"
  ensure_quotas "$bootstrap" "$admin_properties"

  "$KAFKA_BIN/kafka-topics.sh" \
    --bootstrap-server "$bootstrap" \
    --command-config "$admin_properties" \
    --describe >/dev/null
  "$KAFKA_BIN/kafka-configs.sh" \
    --bootstrap-server "$bootstrap" \
    --command-config "$admin_properties" \
    --describe --entity-type users >/dev/null
}

configure_cluster a
configure_cluster b
