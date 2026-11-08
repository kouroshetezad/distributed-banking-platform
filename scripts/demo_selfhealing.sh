#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

SM=http://localhost:8500
GW=http://localhost:8000
PY=python3

snapshot() {
  curl -fsS "$SM/health/deep" 2>/dev/null | "$PY" -c '
import sys, json
d = json.load(sys.stdin)["replicas"]
for shard, urls in sorted(d.items()):
    down = [u for u, s in urls.items() if s == "down"]
    up = [u for u, s in urls.items() if s == "up"]
    print("%s up=%d down=%d down_urls=%s" % (shard, len(up), len(down), ",".join(down)))
'
}

show() {
  echo "--- $1"
  snapshot || true
  echo "    compose-managed containers:"
  docker compose ps --format 'table {{.Name}}\t{{.State}}' | sed -n '1,12p'
  echo "    controller-spawned replacements (not compose-managed):"
  docker ps --filter 'name=bank-healer' --format 'table {{.Names}}\t{{.Status}}'
}

echo "==> reset: restoring Compose's two replicas for Shard B"
docker compose stop selfhealer >/dev/null
docker compose up -d shard-b-2 >/dev/null
for _ in $(seq 1 20); do
  state="$(docker inspect -f '{{.State.Running}}' bank-shard-b-2-1 2>/dev/null || echo false)"
  [ "$state" = "true" ] && break
  sleep 1
done

old_healers="$(docker ps -aq --filter 'name=bank-healer')"
if [ -n "$old_healers" ]; then
  echo "    retiring replacement containers from an earlier run"
  printf '%s\n' "$old_healers" | xargs -r docker rm -f >/dev/null
fi

curl -fsS -X POST "$SM/shards/set" -H 'Content-Type: application/json' \
  -d '{"shard":"B","replicas":["http://shard-b-1:8080","http://shard-b-2:8080"]}' \
  >/dev/null
docker compose start selfhealer >/dev/null

ready=0
for _ in $(seq 1 20); do
  line="$(snapshot 2>/dev/null || true)"
  b="$(printf '%s\n' "$line" | grep '^B ' || true)"
  down=$(printf '%s' "$b" | sed -n 's/.*down=\([0-9]*\).*/\1/p')
  up=$(printf '%s' "$b" | sed -n 's/.*up=\([0-9]*\).*/\1/p')
  if [ "${down:-x}" = "0" ] && [ "${up:-0}" -ge 2 ]; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" != "1" ]; then
  echo "!! Shard B did not reach two live replicas during reset" >&2
  echo "   check: docker compose ps -a ; curl -s $SM/health/deep" >&2
  exit 1
fi

echo
echo "==> before: every shard has two live replicas"
show "before: every shard has two replicas"

echo
echo "==> killing one replica of Shard B (no restart policy is in play)"
docker compose kill shard-b-2
docker compose rm -f shard-b-2 >/dev/null 2>&1 || true

echo
echo "==> waiting for the controller to heal Shard B"
echo "    (watching the supervisor's replica count: down must go 1 -> 0)"
HEALED=0
for i in $(seq 1 30); do
  if ! line=$(snapshot 2>/dev/null) || [ -z "$line" ]; then
    echo "    [$i] supervisor not answering yet"
    sleep 2
    continue
  fi
  b="$(printf '%s\n' "$line" | grep '^B ' || true)"
  down=$(printf '%s' "$b" | sed -n 's/.*down=\([0-9]*\).*/\1/p')
  up=$(printf '%s' "$b" | sed -n 's/.*up=\([0-9]*\).*/\1/p')
  echo "    [$i] shard B: up=${up:-?} down=${down:-?}"
  if [ "${down:-x}" = "0" ] && [ "${up:-0}" -ge 2 ]; then
    HEALED=1
    break
  fi
  sleep 2
done

echo
if [ "$HEALED" = "1" ]; then
  echo "==> Shard B is back to two live replicas (the controller replaced the dead one)"
else
  echo "==> WARNING: Shard B did not return to two live replicas within the window" >&2
  echo "    inspect with: docker compose logs --tail=40 selfhealer" >&2
fi

show "after: the controller restored the count"

echo
echo "==> banking still works on the healed shard"
curl -fsS -X POST "$GW/deposit" -H 'content-type: application/json' \
  -d '{"account":"A1500","amount":1.00}' | "$PY" -m json.tool | sed -n '1,8p'

echo
echo "==> self-healing claims (durable record of every replacement)"
curl -fsS "$SM/events" 2>/dev/null | "$PY" -m json.tool | sed -n '1,30p' || \
  docker compose logs selfhealer | tail -20
