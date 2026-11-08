#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> building and starting the stack"
docker compose up --build -d

echo "==> waiting for the shard-map supervisor"
shardmap_ready=""
for _ in $(seq 1 60); do
  if curl -fsS http://localhost:8500/health >/dev/null 2>&1; then
    shardmap_ready="yes"
    break
  fi
  sleep 1
done
if [ -z "$shardmap_ready" ]; then
  echo "!! the shard-map supervisor never answered on http://localhost:8500/health" >&2
  echo "   check it is running and that docker-compose.yml publishes 8500:" >&2
  docker compose ps >&2
  exit 1
fi

echo "==> waiting for the gateway"
gateway_ready=""
for _ in $(seq 1 60); do
  if curl -fsS http://localhost:8000/health >/dev/null 2>&1; then
    gateway_ready="yes"
    break
  fi
  sleep 1
done
if [ -z "$gateway_ready" ]; then
  echo "!! the gateway never answered on http://localhost:8000/health" >&2
  docker compose ps >&2
  exit 1
fi

echo "==> registering shard replicas"
register_shard() {
  local shard="$1" url1="$2" url2="$3"
  if ! curl -fsS -X POST http://localhost:8500/shards/register \
       -H 'Content-Type: application/json' \
       -d "{\"shard\":\"$shard\",\"replicas\":[\"$url1\",\"$url2\"]}" >/dev/null; then
    echo "!! could not register shard $shard with the supervisor" >&2
    echo "   either the registry has no usable map (a volume left over from an" >&2
    echo "   interrupted run), or the containers are running an older image." >&2
    echo "   The platform source is copied into the image at build time, so a" >&2
    echo "   code change needs a rebuild - 'docker compose restart' will not do it." >&2
    echo "   Try, in order:" >&2
    echo "     docker compose up --build -d shardmap gateway   # pick up new code" >&2
    echo "     docker compose down -v && bash scripts/up.sh    # discard the volume" >&2
    exit 1
  fi
}
register_shard A http://shard-a-1:8080 http://shard-a-2:8080
register_shard B http://shard-b-1:8080 http://shard-b-2:8080

echo "==> pushing the shard map to every replica"
curl -fsS -X POST http://localhost:8500/shard-map/push >/dev/null || \
  echo "!! pushing the map to replicas failed; they may need a moment more" >&2

echo "==> verifying the gateway sees the replicas"
stats=$(curl -fsS http://localhost:8000/gateway/stats 2>/dev/null || true)
if [ -z "$stats" ]; then
  echo "    (the gateway did not answer /gateway/stats; try: curl -s http://localhost:8000/gateway/stats)" >&2
else
  python3 - "$stats" <<'PY'
import json, sys
try:
    d = json.loads(sys.argv[1])
    parts = []
    for shard, info in sorted(d.get("shards", {}).items()):
        parts.append("%s:%d" % (shard, len(info.get("replicas") or [])))
    print("    replicas registered: " + (",".join(parts) if parts else "(none)"))
except Exception as exc:
    print("    (could not parse /gateway/stats: %s)" % exc, file=sys.stderr)
PY
fi

echo "==> stack is up"
echo "    gateway     http://localhost:8000"
echo "    coordinator http://localhost:8100"
echo "    shard-map   http://localhost:8500"
docker compose ps
