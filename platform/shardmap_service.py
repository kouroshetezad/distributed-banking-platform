"""Shard assigner / platform supervisor.

The registry (``platform/storage/registry.py``) owns the durable shard map and
the migration state machine.  This service is the HTTP door to it, and it does
the two jobs that need a process rather than a library:

* **publish + push.**  When the map changes, every replica of every shard is
  told, so the shard-side fence is armed before the change is advertised as
  done.  The coordinator and the gateway read the map from here, and the
  self-healing controller registers new replicas here.
* **drive migrations.**  The live re-sharding bonus needs a component that can
  take a crash in the middle and pick the work back up.  On start-up it calls
  ``resume_migrations`` and continues from the recorded phase.

It is deliberately a *thin* supervisor: it holds no balances and makes no
routing decisions, so it can be restarted at any time without stopping banking
traffic.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from typing import Any, Dict, List, Optional

from .http_service import AppError, JsonService, configure_logging, run_service
from .migration import MigrationEngine
from .sharding.shard_map import ShardMap, ShardMapError, default_accounts
from .storage.registry import ShardMapRegistry
from .targets import env, env_int, http_json, split_csv, url_for

LOG = configure_logging("bank.shardmap")


class ShardMapService(JsonService):
    def __init__(self, host: str, port: int, db_path: str, *,
                 initial_ranges: Optional[List[tuple]] = None,
                 initial_actors: Optional[Dict[str, List[str]]] = None,
                 gateway_url: str = "", epoch: int = 1):
        self.gateway_url = gateway_url.rstrip("/")
        ranges = initial_ranges or [("A", "A0001", "A1000"), ("B", "A1001", "A2000")]
        initial = ShardMap.from_ranges(epoch, ranges, default_accounts(),
                                       note="initial map")
        self.registry = ShardMapRegistry(db_path, initial=initial, actors=initial_actors or {})
        self._ensure_map(initial, initial_actors or {})
        self.engine = MigrationEngine(self.registry, push=self.push_to_every_replica,
                                      health=self._health)
        super().__init__(host, port, "shardmap", self.route)


    def _ensure_map(self, initial: ShardMap, actors: Dict[str, List[str]]) -> None:
        """Make sure the registry actually holds a readable map.

        The registry lives on a volume that outlives the container, so it can be
        left inconsistent by an earlier run that stopped mid-way (for example
        while a lower-level resource was exhausted).  The dangerous case is a
        ``current_map`` row that points at an epoch with no ``shard_maps`` row:
        the service looks healthy but every registration and every route that
        reads the map fails, which is what produced a bare HTTP 500 from
        ``/shards/register``.  Detect that and republish from configuration, so
        a bad volume heals instead of needing a manual ``down -v``.
        """
        try:
            body = self.registry.current_body()
        except ShardMapError:
            body = None
        if body is not None and body.get("ranges"):
            return
        repaired = ShardMap.from_ranges(max(initial.epoch, self.registry.current_epoch() + 1),
                                        [r for r in initial.ranges], default_accounts(),
                                        note="startup repair: registry had no usable map")
        LOG.warning("registry has no usable map; republishing ranges from configuration")
        self.registry.publish(repaired, actor=actors, created_by="startup_repair")

    def describe(self) -> Dict[str, Any]:
        body = super().describe()
        body.update({"epoch": self.registry.current_epoch(),
                     "shards": list(self.registry.actors().keys()),
                     "gateway": self.gateway_url})
        return body

    def close(self) -> None:
        super().close()
        try:
            self.registry.close()
        except Exception:
            pass

    def release_thread_resources(self) -> None:
        try:
            self.registry.release_thread()
        except Exception:
            pass

    def register_shard(self, shard: str, replicas: List[str]) -> Dict[str, Any]:
        """Called by compose at boot (and by self-healing when it adds a replica)."""
        actors = dict(self.registry.actors())
        shard = shard.upper()
        merged = list(dict.fromkeys([u.rstrip("/") for u in actors.get(shard, [])] +
                                    [u.rstrip("/") for u in replicas]))
        body = self.registry.set_actors({shard: merged})
        self.push_to_every_replica()
        return {"shard": shard, "epoch": body["epoch"],
                "fingerprint": body["fingerprint"], "actors": body["actors"]}

    def push_to_every_replica(self) -> Dict[str, Any]:
        """Push the current map to every registered replica."""
        body = self.registry.current_body()
        if not body:
            return {}
        payload = {"epoch": body["epoch"], "ranges": body["ranges"],
                   "accounts": body["accounts"], "note": body["note"]}
        results: Dict[str, Any] = {}
        for shard, urls in body["actors"].items():
            per: Dict[str, Any] = {}
            for url in urls:
                try:
                    reply = http_json(url_for(url, "/internal/shard-map"), payload, timeout=3.0)
                    per[url] = reply.get("epoch")
                except Exception as exc:
                    per[url] = f"unreachable: {exc}"
            results[shard] = per
        return results

    def _health(self, url: str) -> bool:
        try:
            http_json(url_for(url, "/health"), method="GET", timeout=2.0)
            return True
        except Exception:
            return False


    def start_migration(self, body: Dict[str, Any]) -> Dict[str, Any]:
        source = (body.get("source_shard") or "B").upper()
        target = (body.get("target_shard") or "C").upper()
        low = (body.get("low") or "A1501").upper()
        high = (body.get("high") or "A2000").upper()
        return self.engine.run(source, target, low, high)

    def resume_migrations(self) -> Dict[str, Any]:
        return self.engine.resume_all()


    def route(self, method: str, path: str, body: Dict[str, Any], request_id: str):
        try:
            if method == "GET" and path == "/shard-map":
                return 200, self._current()
            if method == "GET" and path == "/shard-map/batch":
                return 200, {"status": "success", "epoch": self.registry.current_epoch(),
                             "maps": {str(e): self.registry.get(e)["ranges"]
                                      for e in [b["epoch"] for b in self.registry.history()]}}
            if method == "GET" and path == "/shard-map/history":
                return 200, {"status": "success", "epoch": self.registry.current_epoch(),
                             "history": self.registry.history()}
            if method == "GET" and path == "/shards":
                return 200, self._shards()
            if method == "POST" and path == "/shard-map":
                return 200, self._publish(body)
            if method == "POST" and path == "/shards/register":
                shard = body.get("shard")
                replicas = body.get("replicas") or []
                if not shard or not replicas:
                    raise AppError("missing_field", "shard and replicas are required",
                                   status=422)
                return 200, {"status": "success", **self.register_shard(shard, replicas)}
            if method == "POST" and path == "/shards/set":
                shard = (body.get("shard") or "").upper()
                replicas = [str(u).rstrip("/") for u in (body.get("replicas") or [])]
                if not shard or not replicas:
                    raise AppError("missing_field", "shard and replicas are required",
                                   status=422)
                body_out = self.registry.set_actors({shard: replicas})
                self.push_to_every_replica()
                return 200, {"status": "success", "shard": shard,
                             "epoch": body_out["epoch"], "actors": body_out["actors"]}
            if method == "POST" and path == "/shard-map/push":
                return 200, {"status": "success", "pushed": self.push_to_every_replica()}
            if method == "GET" and path == "/migrations":
                return 200, {"status": "success", "migrations": self.registry.migrations()}
            if method == "GET" and path == "/migration":
                mig_id = body.get("id")
                if not mig_id:
                    return 200, {"status": "success",
                                 "migration": self.registry.open_migrations()[-1]
                                 if self.registry.open_migrations() else None}
                return 200, {"status": "success",
                             "migration": self.registry.get_migration(mig_id)}
            if method == "POST" and path == "/migration/start":
                return 200, self.start_migration(body)
            if method == "POST" and path == "/migration/resume":
                return 200, self.resume_migrations()
            if method == "POST" and path == "/migration/cleanup":
                return 200, self.engine.cleanup_completed()
            if method == "GET" and path == "/events":
                return 200, {"status": "success", "events": self.registry.events()}
            if method == "GET" and path == "/health/deep":
                return 200, {"status": "success", "replicas": self._deep_health()}
            return None
        except ShardMapError as exc:
            raise AppError("shard_map_error", str(exc), status=409) from exc

    def _current(self) -> Dict[str, Any]:
        body = self.registry.current_body()
        if not body:
            raise AppError("shard_map_unavailable", "no map published", status=503)
        return {"status": "success", "epoch": body["epoch"],
                "fingerprint": body["fingerprint"], "ranges": body["ranges"],
                "accounts": body["accounts"], "actors": body["actors"],
                "note": body["note"]}

    def _shards(self) -> Dict[str, Any]:
        body = self.registry.current_body() or {"actors": {}}
        mapping = self.registry.current()
        accounts_per_shard = {s: len(mapping.accounts_of(s)) for s in mapping.shards()} \
            if mapping else {}
        return {"status": "success", "epoch": self.registry.current_epoch(),
                "shards": [{"shard": s, "replicas": urls,
                            "accounts": accounts_per_shard.get(s, 0)}
                           for s, urls in body["actors"].items()]}

    def _publish(self, body: Dict[str, Any]) -> Dict[str, Any]:
        epoch = int(body.get("epoch", 0))
        mapping = ShardMap.from_ranges(epoch, body.get("ranges") or [],
                                       body.get("accounts") or default_accounts(),
                                       note=body.get("note", ""))
        out = self.registry.publish(mapping, actor=body.get("actors"),
                                    created_by=body.get("created_by", "api"))
        pushed = self.push_to_every_replica()
        return {"status": "success", "epoch": out["epoch"],
                "fingerprint": out["fingerprint"], "ranges": out["ranges"],
                "pushed": pushed}

    def _deep_health(self) -> Dict[str, Any]:
        body = self.registry.current_body() or {"actors": {}}
        report: Dict[str, Any] = {}
        for shard, urls in body["actors"].items():
            report[shard] = {u: ("up" if self._health(u) else "down") for u in urls}
        return report


def build_from_env() -> ShardMapService:
    ranges = []
    for item in split_csv(env("SHARD_RANGES", "A:A0001-A1000,B:A1001-A2000")):
        owner, _, span = item.partition(":")
        low, _, high = span.partition("-")
        ranges.append((owner.strip().upper(), low.strip().upper(), high.strip().upper()))
    actors: Dict[str, List[str]] = {}
    for item in split_csv(env("SHARD_REPLICAS", "")):
        shard, sep, urls = item.partition("=")
        shard = shard.strip().upper()
        for url in split_csv(urls.replace("|", ",")):
            actors.setdefault(shard, []).append(url)
    return ShardMapService(
        env("SHARDMAP_HOST", "0.0.0.0"), env_int("SHARDMAP_PORT", 8500),
        env("REGISTRY_DB_PATH", "data/registry.db"),
        initial_ranges=ranges, initial_actors=actors,
        gateway_url=env("GATEWAY_URL", ""),
        epoch=env_int("INITIAL_EPOCH", 1),
    )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Shard map registry + migration engine + platform supervisor")
    parser.add_argument("--host", default=env("SHARDMAP_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=env_int("SHARDMAP_PORT", 8500))
    parser.add_argument("--db", default=env("REGISTRY_DB_PATH", "data/registry.db"))
    parser.add_argument("--ranges", default=env("SHARD_RANGES", "A:A0001-A1000,B:A1001-A2000"))
    parser.add_argument("--replicas", default=env("SHARD_REPLICAS", ""))
    parser.add_argument("--gateway", default=env("GATEWAY_URL", ""))
    parser.add_argument("--epoch", type=int, default=env_int("INITIAL_EPOCH", 1))
    parser.add_argument("--interval", type=float, default=env_float_default())
    args = parser.parse_args(argv)

    ranges = []
    for item in split_csv(args.ranges):
        owner, _, span = item.partition(":")
        low, _, high = span.partition("-")
        ranges.append((owner.strip().upper(), low.strip().upper(), high.strip().upper()))
    actors: Dict[str, List[str]] = {}
    for item in split_csv(args.replicas):
        shard, sep, urls = item.partition("=")
        for url in split_csv(urls.replace("|", ",")):
            actors.setdefault(shard.strip().upper(), []).append(url)

    service = ShardMapService(args.host, args.port, args.db, initial_ranges=ranges,
                              initial_actors=actors, gateway_url=args.gateway,
                              epoch=args.epoch)
    service.registry.recover()
    service.engine.start_background_resume()
    run_service(service)
    return 0


def env_float_default() -> float:
    from .targets import env_float
    return env_float("MIGRATION_RESUME_INTERVAL", 3.0)


if __name__ == "__main__":
    raise SystemExit(main())
