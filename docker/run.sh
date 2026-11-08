#!/usr/bin/env bash
set -euo pipefail

ROLE="${ROLE:-shard}"

case "$ROLE" in
  shard)
    exec python -m platform.shard_server \
      --host 0.0.0.0 --port "${SHARD_PORT:-8080}" \
      --shard "${SHARD:?SHARD is required for a shard replica}" \
      --db "${SHARD_DB:-/data/shard-${SHARD}.db}" \
      --cluster "${CLUSTER:-compose}" \
      ${SEED_BALANCE:+--seed-balance "$SEED_BALANCE"} \
      ${SEED_STEP:+--seed-step "$SEED_STEP"} \
      ${NO_SEED:+--no-seed}
    ;;
  gateway)
    exec python -m platform.gateway \
      --host 0.0.0.0 --port "${GATEWAY_PORT:-8000}" \
      --registry-db "${REGISTRY_DB_PATH:-/data/registry.db}" \
      --replicas "${SHARD_REPLICAS:-}" \
      --coordinator "${COORDINATOR_URL:-}" \
      --strategy "${REPLICA_STRATEGY:-least_connections}"
    ;;
  coordinator)
    exec python -m platform.coordinator \
      --host 0.0.0.0 --port "${COORDINATOR_PORT:-8100}" \
      --gateway "${GATEWAY_URL:?GATEWAY_URL is required}" \
      --db "${COORDINATOR_DB_PATH:-/data/coordinator.db}"
    ;;
  shardmap)
    exec python -m platform.shardmap_service \
      --host 0.0.0.0 --port "${SHARDMAP_PORT:-8500}" \
      --db "${REGISTRY_DB_PATH:-/data/registry.db}" \
      --ranges "${SHARD_RANGES:-A:A0001-A1000,B:A1001-A2000}" \
      --replicas "${SHARD_REPLICAS:-}" \
      --gateway "${GATEWAY_URL:-}"
    ;;
  selfhealer)
    exec python -m platform.selfhealing \
      --registry-db "${REGISTRY_DB_PATH:-/data/registry.db}" \
      --desired "${DESIRED_REPLICAS:-2}" \
      --interval "${HEAL_INTERVAL:-2.0}"
    ;;
  *)
    echo "unknown ROLE=$ROLE (want shard|gateway|coordinator|shardmap|selfhealer)" >&2
    exit 2
    ;;
esac
