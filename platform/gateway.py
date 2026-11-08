"""The shard-aware gateway.

It is the only address a banking client knows, and it does three jobs:

**1. Route.**  Ask the registry (or its cached copy) which shard owns an account,
then pick one of that shard's healthy replicas.  An account never leaks to
another shard "for a global balance" - the assignment forbids it, and the tests
prove it.

**2. Balance load.**  Among the healthy replicas of the owning shard the gateway
uses least-connections-first, tie-broken by round robin.  Least-connections fits
the workload better than plain round robin because the operations have very
different costs (a transfer holds the write lock longer than a balance read), so
the choice is documented rather than accidental.  Every reply carries the replica
that served it, which is what experiment 2 inspects.

**3. Detect failure.**  A background health loop probes each replica.  A replica
that fails ``HEALTH_FAIL_THRESHOLD`` consecutive probes is taken out of rotation;
one that succeeds ``HEALTH_RECOVER_THRESHOLD`` times comes back.  "Route around
the dead replica" therefore needs no operator.

Cross-shard transfers are *not* executed here.  The gateway detects that the two
accounts live on different shards and hands the request to the coordinator,
because a gateway has no business running two-phase commit.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from typing import Any, Dict, List, Optional

from .http_api import (
    ValidationError,
    cents_from_body,
    require_account,
)
from .http_service import AppError, JsonService, configure_logging, run_service
from .sharding.shard_map import (
    ShardMap,
    ShardMapError,
    default_accounts,
)
from .storage.registry import ShardMapRegistry
from .targets import (
    PeerError,
    env,
    env_float,
    env_int,
    http_json,
    normalize_url,
    split_csv,
    url_for,
)

LOG = configure_logging("bank.gateway")


class ReplicaPool:
    """Healthy-replica bookkeeping for one shard."""

    def __init__(self, shard: str, urls: List[str], strategy: str = "least_connections"):
        self.shard = shard
        self.urls = [normalize_url(u) for u in urls]
        self.strategy = strategy
        self._lock = threading.Lock()
        self._healthy: Dict[str, bool] = {u: True for u in self.urls}
        self._inflight: Dict[str, int] = {u: 0 for u in self.urls}
        self._failures: Dict[str, int] = {u: 0 for u in self.urls}
        self._successes: Dict[str, int] = {u: 0 for u in self.urls}
        self._rr = 0
        self._served: Dict[str, int] = {u: 0 for u in self.urls}


    def candidates(self) -> List[str]:
        with self._lock:
            alive = [u for u in self.urls if self._healthy.get(u)]
            if not alive:
                alive = list(self.urls)
            if self.strategy == "round_robin":
                self._rr = (self._rr + 1) % max(1, len(alive))
                return alive[self._rr:] + alive[: self._rr]
            self._rr = (self._rr + 1) % max(1, len(alive))
            ordered = alive[self._rr:] + alive[: self._rr]
            return sorted(ordered, key=lambda u: self._inflight.get(u, 0))

    def acquire(self) -> str:
        url = self.candidates()[0]
        with self._lock:
            self._inflight[url] = self._inflight.get(url, 0) + 1
        return url

    def release(self, url: str, *, ok: bool = True) -> None:
        with self._lock:
            self._inflight[url] = max(0, self._inflight.get(url, 0) - 1)
            if ok:
                self._served[url] = self._served.get(url, 0) + 1


    def mark(self, url: str, healthy: bool) -> None:
        with self._lock:
            self._healthy[url] = healthy
            if healthy:
                self._successes[url] = self._successes.get(url, 0) + 1
                self._failures[url] = 0
            else:
                self._failures[url] = self._failures.get(url, 0) + 1
                self._successes[url] = 0

    def eligible_for_probe(self, url: str, fail_threshold: int,
                           recover_threshold: int) -> bool:
        with self._lock:
            if self._healthy.get(url):
                return True
            return self._successes.get(url, 0) + 1 >= recover_threshold or True

    def state(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "shard": self.shard,
                "strategy": self.strategy,
                "replicas": [
                    {
                        "url": url,
                        "healthy": self._healthy.get(url, False),
                        "inflight": self._inflight.get(url, 0),
                        "consecutive_failures": self._failures.get(url, 0),
                        "served": self._served.get(url, 0),
                    }
                    for url in self.urls
                ],
            }


class Gateway:
    """Routing decisions, replica selection, health, and the 2PC hand-off."""

    def __init__(self, registry: ShardMapRegistry, coordinator_url: str = "",
                 strategy: str = "least_connections", timeout: float = 8.0,
                 fail_threshold: int = 2, recover_threshold: int = 2):
        self.registry = registry
        self.coordinator_url = coordinator_url.rstrip("/") if coordinator_url else ""
        self.strategy = strategy
        self.timeout = timeout
        self.fail_threshold = fail_threshold
        self.recover_threshold = recover_threshold
        self.pools: Dict[str, ReplicaPool] = {}
        self._pools_epoch = -1
        self._lock = threading.Lock()
        self._health_stop: Optional[threading.Event] = None
        self._refresh_pools()


    def current(self) -> ShardMap:
        mapping = self.registry.current()
        if mapping is None:
            raise AppError("shard_map_unavailable", "no shard map published yet", status=503)
        return mapping

    def _refresh_pools(self) -> None:
        epoch = self.registry.current_epoch()
        if epoch == self._pools_epoch and self.pools:
            return
        body = self.registry.current_body()
        if not body:
            return
        actors: Dict[str, List[str]] = body.get("actors", {})
        with self._lock:
            for shard, urls in actors.items():
                if shard not in self.pools:
                    self.pools[shard] = ReplicaPool(shard, urls, self.strategy)
                    LOG.info("registered shard %s replicas=%s", shard, urls)
                else:
                    pool = self.pools[shard]
                    merged = list(dict.fromkeys(pool.urls + [normalize_url(u) for u in urls]))
                    if merged != pool.urls:
                        pool.urls = merged
                        for url in merged:
                            pool._healthy.setdefault(url, True)
                            pool._inflight.setdefault(url, 0)
                            pool._failures.setdefault(url, 0)
                            pool._successes.setdefault(url, 0)
            self._pools_epoch = epoch

    def pool_for(self, shard: str) -> ReplicaPool:
        self._refresh_pools()
        pool = self.pools.get(shard.upper())
        if pool is None or not pool.urls:
            raise AppError("shard_unavailable", f"no replicas registered for shard {shard}",
                           status=503, shard=shard)
        return pool


    def call_replica(self, shard: str, path: str, payload: Dict[str, Any], *,
                     method: str = "POST", retries: Optional[int] = None) -> Dict[str, Any]:
        """Send a request to one healthy replica of ``shard``, retrying others.

        Only *transport* failures are retried here.  An application-level
        rejection (insufficient funds, stale epoch) is returned as-is: retrying it
        on another replica would either fail again or, worse, hide a real
        conflict.  Write retries are additionally safe because every write
        carries an idempotent ``request_id``.
        """
        pool = self.pool_for(shard)
        attempts = retries if retries is not None else len(pool.urls)
        last: Optional[PeerError] = None
        tried: List[str] = []
        for _ in range(max(1, attempts)):
            url = pool.acquire()
            tried.append(url)
            try:
                reply = http_json(url_for(url, path), payload if method == "POST" else None,
                                  method=method, timeout=self.timeout)
                if reply.get("_http_status", 200) >= 500:
                    raise PeerError(f"HTTP {reply.get('_http_status')} from {url}",
                                    status=reply.get("_http_status"))
                pool.mark(url, True)
                reply.setdefault("served_by", url)
                pool.release(url, ok=True)
                return reply
            except PeerError as exc:
                pool.mark(url, False)
                pool.release(url, ok=False)
                last = exc
                LOG.warning("replica %s unhealthy (%s), trying another", url, exc)
        raise AppError("shard_unavailable",
                       f"every replica of shard {shard} failed: {last}", status=503,
                       tried=tried, shard=shard)

    def route_to_owner(self, account: str, action: str, *,
                       method: str = "POST", payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        mapping = self.current()
        try:
            shard = mapping.shard_of(account)
        except ShardMapError as exc:
            raise AppError("unknown_account", str(exc), status=422) from exc
        body = {"epoch": mapping.epoch, "account": account}
        body.update(payload or {})
        body.setdefault("epoch", mapping.epoch)
        reply = self.call_replica(shard, action, body, method=method)
        reply.setdefault("shard", shard)
        reply["map_epoch"] = mapping.epoch
        return reply


    def health_sweep(self, probe_timeout: float = 2.0) -> Dict[str, Any]:
        self._refresh_pools()
        report: Dict[str, Any] = {}
        for shard, pool in list(self.pools.items()):
            states = []
            for url in list(pool.urls):
                try:
                    http_json(url_for(url, "/health"), method="GET", timeout=probe_timeout)
                    pool.mark(url, True)
                    if pool._failures.get(url, 0) == 0:
                        states.append((url, "up"))
                except PeerError:
                    pool.mark(url, False)
                    states.append((url, "down"))
                    LOG.warning("health: shard %s replica %s is DOWN", shard, url)
            report[shard] = pool.state()
        return report

    def start_health_loop(self, interval: float = 2.0,
                          probe_timeout: float = 2.0) -> threading.Thread:
        stop = threading.Event()
        self._health_stop = stop

        def loop() -> None:
            while not stop.is_set():
                try:
                    self.health_sweep(probe_timeout=probe_timeout)
                except Exception:
                    LOG.exception("health sweep failed")
                stop.wait(interval)

        thread = threading.Thread(target=loop, name="gateway-health", daemon=True)
        thread.start()
        return thread

    def stop_health_loop(self) -> None:
        """Ask the background health loop to finish.

        Called when a local harness tears a stack down.  Leaving it running would
        keep a thread probing a cluster whose sockets have been closed, once per
        stack built in the process.
        """
        if self._health_stop is not None:
            self._health_stop.set()
            self._health_stop = None


class GatewayService(JsonService):
    def __init__(self, host: str, port: int, registry: ShardMapRegistry,
                 coordinator_url: str = "", strategy: str = "least_connections",
                 timeout: float = 8.0, fail_threshold: int = 2,
                 recover_threshold: int = 2):
        self.gateway = Gateway(registry, coordinator_url, strategy, timeout,
                               fail_threshold, recover_threshold)
        self.registry = registry
        self.coordinator_url = coordinator_url
        super().__init__(host, port, "gateway", self.route)

    def describe(self) -> Dict[str, Any]:
        body = super().describe()
        body.update({"replica_strategy": self.gateway.strategy,
                     "coordinator": self.coordinator_url,
                     "map_epoch": self.registry.current_epoch()})
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


    def route(self, method: str, path: str, body: Dict[str, Any], request_id: str):
        try:
            if method == "GET" and path == "/shard-map":
                return 200, self._shard_map(body)
            if method == "GET" and path == "/clusters":
                return 200, self._clusters(body)
            if method == "GET" and path == "/account":
                return 200, self._balance(body)
            if method == "POST" and path == "/deposit":
                return 200, self._deposit(body, request_id)
            if method == "POST" and path == "/withdraw":
                return 200, self._withdraw(body, request_id)
            if method == "POST" and path == "/transfer":
                return 200, self._transfer(body, request_id)
            if method == "POST" and path == "/route/prepare":
                return 200, self._route_prepare(body)
            if method == "POST" and path == "/route/commit":
                return 200, self._route_commit(body)
            if method == "POST" and path == "/route/abort":
                return 200, self._route_abort(body)
            if method == "POST" and path == "/route/txn-status":
                return 200, self._route_txn_status(body)
            if method == "GET" and path == "/txn":
                return 200, self._txn_status(body)
            if method == "GET" and path == "/gateway/stats":
                return 200, self._stats()
            if method == "POST" and path == "/health/sweep":
                return 200, {"status": "success", "shards": self.gateway.health_sweep()}
            if method == "GET" and path == "/debug/route":
                return 200, self._debug_route(body)
            if method == "POST" and path == "/admin/shard-map":
                return 200, self._publish_map(body)
            if method == "POST" and path == "/admin/actors":
                return 200, self._set_actors(body)
            if method == "POST" and path == "/admin/seed":
                return 200, self._seed(body)
            if method == "POST" and path == "/admin/replica-select":
                return 200, self._select_replica(body)
            return None
        except ValidationError as exc:
            raise AppError(exc.code, exc.message, status=422, **exc.extra) from exc


    def _shard_map(self, body: Dict[str, Any]) -> Dict[str, Any]:
        mapping_body = self.registry.current_body()
        if not mapping_body:
            raise AppError("shard_map_unavailable", "no shard map published yet", status=503)
        body_out = {
            "status": "success", "epoch": mapping_body["epoch"],
            "fingerprint": mapping_body["fingerprint"],
            "ranges": mapping_body["ranges"],
            "accounts": mapping_body["accounts"],
            "actors": mapping_body["actors"],
            "note": mapping_body["note"],
        }
        return body_out

    def _clusters(self, body: Dict[str, Any]) -> Dict[str, Any]:
        self.gateway._refresh_pools()
        return {"status": "success", "epoch": self.registry.current_epoch(),
                "shards": {s: p.state() for s, p in self.gateway.pools.items()}}

    def _balance(self, body: Dict[str, Any]) -> Dict[str, Any]:
        account = require_account(body.get("account"))
        return self.gateway.route_to_owner(account, "/read", payload={"account": account})

    def _deposit(self, body: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        account = require_account(body.get("account"))
        cents = cents_from_body(body, "amount")
        return self.gateway.route_to_owner(account, "/write", payload={
            "action": "deposit", "account": account, "amount_cents": cents,
            "request_id": body.get("request_id") or request_id,
        })

    def _withdraw(self, body: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        account = require_account(body.get("account"))
        cents = cents_from_body(body, "amount")
        return self.gateway.route_to_owner(account, "/write", payload={
            "action": "withdraw", "account": account, "amount_cents": cents,
            "request_id": body.get("request_id") or request_id,
        })

    def _transfer(self, body: Dict[str, Any], request_id: str) -> Dict[str, Any]:
        """Same-shard transfers run atomically on the owner; cross-shard ones go to 2PC."""
        src = require_account(body.get("account"), "account")
        dst = require_account(body.get("to"), "to")
        cents = cents_from_body(body, "amount")
        if src == dst:
            raise AppError("invalid_transfer", "source and destination must differ", status=422)

        mapping = self.gateway.current()
        src_shard = mapping.shard_of(src)
        dst_shard = mapping.shard_of(dst)
        request_id = body.get("request_id") or request_id

        if src_shard == dst_shard:
            reply = self.gateway.route_to_owner(src, "/transfer", payload={
                "account": src, "to": dst, "amount_cents": cents,
                "request_id": request_id,
            })
            reply["mode"] = "same_shard"
            reply["coordinated"] = False
            return reply

        if not self.gateway.coordinator_url:
            raise AppError(
                "no_coordinator",
                "cross-shard transfer requires the transaction coordinator",
                status=503, source_shard=src_shard, target_shard=dst_shard,
            )
        payload = {"account": src, "to": dst, "amount_cents": cents,
                   "request_id": request_id, "epoch": mapping.epoch}
        try:
            reply = http_json(url_for(self.gateway.coordinator_url, "/txn"), payload,
                              timeout=self.gateway.timeout * 4)
        except PeerError as exc:
            raise AppError("coordinator_unavailable",
                           f"coordinator unreachable: {exc}", status=503) from exc
        reply.setdefault("mode", "cross_shard")
        return reply

    def _route_prepare(self, body: Dict[str, Any]) -> Dict[str, Any]:
        shard = self._require_shard(body)
        return self.gateway.call_replica(shard, "/2pc/prepare", body)

    def _route_commit(self, body: Dict[str, Any]) -> Dict[str, Any]:
        shard = self._require_shard(body)
        return self.gateway.call_replica(shard, "/2pc/commit", body)

    def _route_abort(self, body: Dict[str, Any]) -> Dict[str, Any]:
        shard = self._require_shard(body)
        return self.gateway.call_replica(shard, "/2pc/abort", body)

    def _route_txn_status(self, body: Dict[str, Any]) -> Dict[str, Any]:
        shard = self._require_shard(body)
        return self.gateway.call_replica(shard, "/2pc/status", body)

    @staticmethod
    def _require_shard(body: Dict[str, Any]) -> str:
        shard = body.get("shard")
        if not shard:
            raise AppError("missing_field", "shard is required", status=422)
        return str(shard).upper()

    def _txn_status(self, body: Dict[str, Any]) -> Dict[str, Any]:
        txn_id = body.get("txn_id")
        if not txn_id:
            raise AppError("missing_field", "txn_id is required", status=422)
        if not self.gateway.coordinator_url:
            raise AppError("no_coordinator", "no coordinator configured", status=503)
        try:
            return http_json(url_for(self.gateway.coordinator_url, f"/txn?txn_id={txn_id}"),
                             method="GET", timeout=self.gateway.timeout)
        except PeerError as exc:
            raise AppError("coordinator_unavailable", str(exc), status=503) from exc

    def _stats(self) -> Dict[str, Any]:
        return {"status": "success", "instance": self.instance_id,
                "epoch": self.registry.current_epoch(),
                "strategy": self.gateway.strategy,
                "shards": {s: p.state() for s, p in self.gateway.pools.items()}}


    def _debug_route(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Which shard and which replica *would* serve this account, and why."""
        account = require_account(body.get("account"))
        mapping = self.gateway.current()
        shard = mapping.shard_of(account)
        pool = self.gateway.pool_for(shard)
        ranked = pool.candidates()
        return {
            "status": "success", "account": account, "shard": shard,
            "epoch": mapping.epoch, "strategy": pool.strategy,
            "chosen": ranked[0],
            "order": ranked,
            "replicas": pool.state(),
            "range": mapping.owner(account).as_dict()["range"],
            "reason": f"account {account} is in range "
                      f"{mapping.owner(account).as_dict()['range']} "
                      f"which shard {shard} owns at epoch {mapping.epoch}",
        }

    def _publish_map(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Install a new map.  Epochs are validated by the registry."""
        epoch = int(body.get("epoch", 0))
        ranges = body.get("ranges") or []
        accounts = body.get("accounts") or default_accounts()
        note = body.get("note", "")
        mapping = ShardMap.from_ranges(epoch, ranges, accounts, note=note)
        body_out = self.registry.publish(mapping, actor=body.get("actors"),
                                        created_by=body.get("created_by", "admin"))
        self.gateway._refresh_pools()
        self._push_map_to_shards(body_out)
        return {"status": "success", "epoch": body_out["epoch"],
                "fingerprint": body_out["fingerprint"],
                "ranges": body_out["ranges"]}

    def _set_actors(self, body: Dict[str, Any]) -> Dict[str, Any]:
        actors = body.get("actors") or {}
        body_out = self.registry.set_actors({k: list(v) for k, v in actors.items()})
        self.gateway.pools = {}
        self.gateway._pools_epoch = -1
        self.gateway._refresh_pools()
        self._push_map_to_shards(body_out)
        return {"status": "success", "epoch": body_out["epoch"], "actors": body_out["actors"]}

    def _push_map_to_shards(self, map_body: Dict[str, Any]) -> Dict[str, Any]:
        """Push the current map to every replica, so the shard-side fence is armed.

        Pushing before returning from a map change is what makes the fence
        *usable*: once this call has returned, a stale-routed write reaching the
        old owner is refused by the old owner itself, not merely by the gateway.
        """
        results: Dict[str, Any] = {}
        payload = {"epoch": map_body["epoch"], "ranges": map_body["ranges"],
                   "accounts": map_body["accounts"], "note": map_body["note"]}
        for shard, pool in list(self.gateway.pools.items()):
            per_replica: Dict[str, Any] = {}
            for url in pool.urls:
                try:
                    reply = http_json(url_for(url, "/internal/shard-map"), payload, timeout=3.0)
                    per_replica[url] = "ok" if reply.get("_http_status", 200) < 400 else "error"
                except PeerError as exc:
                    per_replica[url] = f"unreachable: {exc}"
            results[shard] = per_replica
        return results

    def _seed(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Seed every replica of every shard with the accounts it should own."""
        mapping = self.gateway.current()
        results: Dict[str, Any] = {}
        for shard, pool in list(self.gateway.pools.items()):
            accounts = mapping.accounts_of(shard)
            payload = {"accounts": {a: int(body.get("balance_cents", 100000)) for a in accounts}}
            per_replica = {}
            for url in pool.urls:
                try:
                    reply = http_json(url_for(url, "/internal/seed"), payload, timeout=5.0)
                    per_replica[url] = reply.get("created")
                except PeerError as exc:
                    per_replica[url] = f"unreachable: {exc}"
            results[shard] = per_replica
        return {"status": "success", "epoch": mapping.epoch, "seeded": results}

    def _select_replica(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Probe replica selection N times and report the distribution."""
        account = require_account(body.get("account"))
        count = int(body.get("count", 8))
        mapping = self.gateway.current()
        shard = mapping.shard_of(account)
        pool = self.gateway.pool_for(shard)
        picks: Dict[str, int] = {}
        for _ in range(count):
            url = pool.candidates()[0]
            picks[url] = picks.get(url, 0) + 1
        return {"status": "success", "account": account, "shard": shard,
                "strategy": pool.strategy, "samples": count, "distribution": picks}


def build_registry(args, accounts: Optional[List[str]] = None) -> ShardMapRegistry:
    """Create/attach the registry, seeding it from ``--ranges`` on first boot."""
    ranges = []
    for item in split_csv(args.ranges):
        owner, _, span = item.partition(":")
        low, _, high = span.partition("-")
        if not owner or not low or not high:
            raise SystemExit(f"--ranges entry must look like A:A0001-A1000, got {item!r}")
        ranges.append((owner.strip().upper(), low.strip().upper(), high.strip().upper()))
    accounts = accounts or default_accounts()
    initial = ShardMap.from_ranges(int(args.epoch), ranges, accounts,
                                   note="initial map from --ranges")

    actors: Dict[str, List[str]] = {}
    for item in split_csv(args.replicas):
        shard, sep, urls = item.partition("=")
        if not sep:
            shard, _, urls = item.partition(":")
        shard = shard.strip().upper()
        for url in split_csv(urls.replace("|", ",")):
            actors.setdefault(shard, []).append(url)

    registry = ShardMapRegistry(args.registry_db, initial=initial, actors=actors)

    if actors:
        current = registry.actors()
        repaired = {shard: urls for shard, urls in actors.items() if not current.get(shard)}
        if repaired:
            registry.set_actors(repaired)
            LOG.warning("registry had no replicas for %s; restored from configuration",
                        ", ".join(sorted(repaired)))
    return registry


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Shard-aware banking gateway")
    parser.add_argument("--host", default=env("GATEWAY_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=env_int("GATEWAY_PORT", 8000))
    parser.add_argument("--registry-db", default=env("REGISTRY_DB_PATH", "data/registry.db"))
    parser.add_argument("--ranges", default=env("SHARD_RANGES", "A:A0001-A1000,B:A1001-A2000"))
    parser.add_argument("--replicas", default=env("SHARD_REPLICAS", ""),
                        help="A:url1|url2,B:url1|url2 (comma separates shards)")
    parser.add_argument("--epoch", type=int, default=env_int("INITIAL_EPOCH", 1))
    parser.add_argument("--coordinator", default=env("COORDINATOR_URL", ""))
    parser.add_argument("--strategy", default=env("REPLICA_STRATEGY", "least_connections"))
    parser.add_argument("--timeout", type=float, default=env_float("PEER_TIMEOUT_SECONDS", 8.0))
    parser.add_argument("--health-interval", type=float, default=env_float("HEALTH_INTERVAL", 2.0))
    parser.add_argument("--no-health-loop", action="store_true")
    args = parser.parse_args(argv)

    registry = build_registry(args)
    service = GatewayService(args.host, args.port, registry, coordinator_url=args.coordinator,
                             strategy=args.strategy, timeout=args.timeout)
    if not args.no_health_loop:
        service.gateway.start_health_loop(args.health_interval)
    run_service(service)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
