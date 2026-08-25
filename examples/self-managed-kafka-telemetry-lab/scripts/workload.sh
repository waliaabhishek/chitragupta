#!/usr/bin/env bash
set -euo pipefail

readonly LAB_CLUSTER=${1:?lab cluster is required}
readonly BOOTSTRAP=${2:?bootstrap server is required}
readonly GENERATED=${3:?generated config directory is required}
readonly KAFKA_BIN=${KAFKA_BIN:-/opt/kafka/bin}
readonly WORK_DIR=${WORK_DIR:-/tmp}
readonly CLUSTER_SUFFIX=${LAB_CLUSTER#cluster-}

source "$GENERATED/runtime-plan.sh"

declare -a CHILD_PIDS=()

stop_children() {
  if ((${#CHILD_PIDS[@]} > 0)); then
    kill "${CHILD_PIDS[@]}" 2>/dev/null || true
    wait "${CHILD_PIDS[@]}" 2>/dev/null || true
  fi
}
trap stop_children EXIT TERM INT

client_config() {
  local identity=$1
  local client_id=$2
  local output=$3
  cp "$GENERATED/${identity}-${CLUSTER_SUFFIX}.properties" "$output"
  printf 'client.id=%s\n' "$client_id" >>"$output"
}

producer_loop() {
  local topic=$1
  local messages_per_second=$2
  local properties=$3
  while true; do
    "$KAFKA_BIN/kafka-producer-perf-test.sh" \
      --topic "$topic" \
      --num-records "$((messages_per_second * LAB_PRODUCER_BATCH_SECONDS))" \
      --record-size "$LAB_RECORD_SIZE_BYTES" \
      --throughput "$messages_per_second" \
      --bootstrap-server "$BOOTSTRAP" \
      --command-config "$properties" >/dev/null
  done
}

consumer_loop() {
  local topic=$1
  local group=$2
  local properties=$3
  while true; do
    "$KAFKA_BIN/kafka-consumer-perf-test.sh" \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$topic" \
      --group "$group" \
      --num-records "$LAB_CONSUMER_RECORDS_PER_RUN" \
      --command-config "$properties" \
      --timeout "$LAB_CONSUMER_TIMEOUT_MS" >/dev/null || true
  done
}

run_profile() {
  local topic=$1
  local identity=$2
  local client_id=$3
  local group=$4
  local messages_per_second=$5
  local producer_properties="$WORK_DIR/${topic}-producer.properties"
  local consumer_properties="$WORK_DIR/${topic}-consumer.properties"
  client_config "$identity" "$client_id" "$producer_properties"
  client_config "$identity" "$client_id" "$consumer_properties"
  printf 'buffer.memory=%s\ndelivery.timeout.ms=%s\nmax.block.ms=%s\n' \
    "$LAB_PRODUCER_BUFFER_MEMORY_BYTES" \
    "$LAB_PRODUCER_DELIVERY_TIMEOUT_MS" \
    "$LAB_PRODUCER_MAX_BLOCK_MS" >>"$producer_properties"
  producer_loop "$topic" "$messages_per_second" "$producer_properties" &
  CHILD_PIDS+=("$!")
  consumer_loop "$topic" "$group" "$consumer_properties" &
  CHILD_PIDS+=("$!")
}

for index in "${!LAB_PROFILE_NAMES[@]}"; do
  rate_bytes_per_second=${LAB_PROFILE_RATES_BYTES_PER_SECOND[$index]}
  run_profile \
    "${LAB_PROFILE_TOPICS[$index]}" \
    "${LAB_PROFILE_USERS[$index]}" \
    "${LAB_PROFILE_CLIENT_IDS[$index]}" \
    "${LAB_PROFILE_GROUPS[$index]}" \
    "$((rate_bytes_per_second / LAB_RECORD_SIZE_BYTES))"
done

wait -n "${CHILD_PIDS[@]}"
printf 'workload child exited unexpectedly for %s\n' "$LAB_CLUSTER" >&2
exit 1
