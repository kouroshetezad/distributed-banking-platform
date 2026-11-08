#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

GW=http://localhost:8000
SM=http://localhost:8500
PY=python3

echo "==> before: who owns A1800?"
curl -fsS "$GW/shard-map" | "$PY" -m json.tool | sed -n '1,20p'

echo
echo "==> starting a background transfer load so banking continues during the move"
( for _ in $(seq 1 400); do
    curl -fsS -X POST "$GW/deposit" \
      -H 'content-type: application/json' \
      -d '{"account":"A1800","amount":0.01}' >/dev/null 2>&1 || true
  done ) &
LOAD=$!

echo "==> registering Shard C (two replicas on a new volume)"
docker compose -f docker-compose.yml -f docker-compose.migrate.yml up -d shard-c-1 shard-c-2

echo "==> waiting for Shard C replicas to become reachable"
c_ready=""
for _ in $(seq 1 60); do
  if docker compose exec -T shardmap python - <<'PY' >/dev/null 2>&1
import urllib.request
for url in ("http://shard-c-1:8080/health", "http://shard-c-2:8080/health"):
    urllib.request.urlopen(url, timeout=2).read()
PY
  then
    c_ready="yes"
    break
  fi
  sleep 1
done
if [ -z "$c_ready" ]; then
  echo "!! shard C replicas never answered /health; not starting the migration" >&2
  docker compose -f docker-compose.yml -f docker-compose.migrate.yml ps >&2
  exit 1
fi

curl -fsS -X POST "$SM/shards/register" -H 'content-type: application/json' \
  -d '{"shard":"C","replicas":["http://shard-c-1:8080","http://shard-c-2:8080"]}' \
  | "$PY" -m json.tool | sed -n '1,8p'

echo "==> migrating A1501-A2000 from B to C"
curl -fsS -X POST "$SM/migration/start" -H 'content-type: application/json' \
  -d '{"source_shard":"B","target_shard":"C","low":"A1501","high":"A2000"}' \
  | "$PY" -m json.tool | sed -n '1,12p'

echo "==> releasing the old owner's rows"
curl -fsS -X POST "$SM/migration/cleanup" >/dev/null

wait "$LOAD" || true

echo
echo "==> after: A1800 is now served by"
curl -fsS "$GW/debug/route?account=A1800" | "$PY" -m json.tool | sed -n '1,10p'
echo "==> shard-map history"
curl -fsS "$SM/shard-map/history" | "$PY" -m json.tool
