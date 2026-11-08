"""Boot a whole platform in one process.

The Docker Compose stack is the deliverable, but a full stack is slow to start
and awkward to poke at from a test.  This harness starts the same services -
two shards with two replicas each, the registry, the gateway and the coordinator -
using the very same classes and the very same SQLite files, on loopback ports.

It exists so ``tests/`` can assert protocol behaviour quickly, and so a reviewer
can run ``python -m platform.local_stack`` and get a working cluster without
Docker.  It is *not* a replacement for the compose file: it has no containers to
kill, so the crash tests under ``tests/`` use it for logic and the compose stack
for the real thing.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional

from .coordinator import Coordinator, CoordinatorJournal, CoordinatorService
from .gateway import GatewayService, build_registry
from .shard_server import ShardService, build_seed, mapping_from_env
from .sharding.shard_map import ShardMap, default_accounts
from .storage.registry import ShardMapRegistry
from .targets import PeerError, http_json, url_for


class LocalStack:
    """A running platform: shards (with replicas), gateway, coordinator."""

    def __init__(self, root: Optional[str] = None, accounts: Optional[List[str]] = None,
                 ranges: Optional[List[str]] = None, base_cents: int = 100000,
                 step_cents: int = 100, replicas_per_shard: int = 2,
                 strategy: str = "least_connections"):
        self.root = root or tempfile.mkdtemp(prefix="bank-stack-")
        self.accounts = accounts or default_accounts()
        self.ranges = ranges or [("A", "A0001", "A1000"), ("B", "A1001", "A2000")]
        self.base_cents = base_cents
        self.step_cents = step_cents
        self.replicas_per_shard = replicas_per_shard
        self._services: List[Any] = []
        self._threads: List[threading.Thread] = []
        self.ports: Dict[str, int] = {}
        self.shard_urls: Dict[str, List[str]] = {}
        self.gateway_url = ""
        self.coordinator_url = ""

        initial = ShardMap.from_ranges(1, self.ranges, self.accounts, note="local stack")

        existing_registry = os.path.join(self.root, "registry.db")
        self._adopted = None
        if os.path.exists(existing_registry):
            try:
                probe = ShardMapRegistry(existing_registry)
                self._adopted = probe.current()
                probe.close()
            except Exception:
                self._adopted = None
        active = self._adopted or initial
        self._active = active

        for shard in active.shards():
            self.shard_urls[shard] = []
            db_path = os.path.join(self.root, f"shard-{shard}.db")
            seed = build_seed(shard, active, base_cents, step_cents)
            for index in range(1, self.replicas_per_shard + 1):
                service = ShardService("127.0.0.1", 0, shard, db_path, seed=seed,
                                       cluster="local-stack")
                url = self._start(service, f"shard-{shard}-{index}")
                http_json(url_for(url, "/internal/shard-map"),
                          {"epoch": active.epoch,
                           "ranges": [r.as_dict() for r in active.ranges],
                           "accounts": active.accounts,
                           "note": active.note})
                self.shard_urls[shard].append(url)

        registry_db = os.path.join(self.root, "registry.db")
        self.registry: ShardMapRegistry = ShardMapRegistry(
            registry_db, initial=initial, actors=self.shard_urls)
        self.registry.set_actors(self.shard_urls)

        self._gateway = GatewayService("127.0.0.1", 0, self.registry,
                                       strategy=strategy, timeout=5.0)
        self.gateway_url = self._start(self._gateway, "gateway")
        self._gateway.gateway.start_health_loop(interval=0.5, probe_timeout=0.5)

        coord_db = os.path.join(self.root, "coordinator.db")
        self._coordinator = CoordinatorService("127.0.0.1", 0, self.gateway_url,
                                               coord_db, timeout=5.0)
        self.coordinator_url = self._start(self._coordinator, "coordinator")
        self._gateway.gateway.coordinator_url = self.coordinator_url


    def _start(self, service, label: str) -> str:
        port = service.server_address[1]
        thread = service.start_background()
        self._services.append(service)
        self._threads.append(thread)
        self.ports[label] = port
        return f"http://127.0.0.1:{port}"

    def stop(self) -> None:
        try:
            self._gateway.gateway.stop_health_loop()
        except Exception:
            pass
        for service in reversed(self._services):
            try:
                service.shutdown()
            except Exception:
                pass
            try:
                service.close()
            except Exception:
                pass
        try:
            self.registry.close()
        except Exception:
            pass

    def cleanup(self, remove_files: bool = True) -> None:
        self.stop()
        if remove_files:
            shutil.rmtree(self.root, ignore_errors=True)

    def __enter__(self) -> "LocalStack":
        return self

    def __exit__(self, *exc) -> None:
        self.cleanup()


    def get(self, path: str, **params: Any) -> Dict[str, Any]:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = url_for(self.gateway_url, path + (f"?{query}" if query else ""))
        return http_json(url, method="GET", timeout=15.0)

    def post(self, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return http_json(url_for(self.gateway_url, path), payload or {}, timeout=15.0)

    def coordinator_get(self, path: str, **params: Any) -> Dict[str, Any]:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = url_for(self.coordinator_url, path + (f"?{query}" if query else ""))
        return http_json(url, method="GET", timeout=15.0)

    def balance(self, account: str) -> int:
        reply = self.get("/account", account=account)
        if "balance_cents" not in reply:
            raise AssertionError(f"unexpected balance reply: {reply}")
        return int(reply["balance_cents"])

    def total_cents(self) -> int:
        total = 0
        for shard, urls in self.shard_urls.items():
            reply = http_json(url_for(self.live_url(shard), "/stats"),
                              method="GET", timeout=10.0)
            total += int(reply["total_cents"])
        return total

    def live_url(self, shard: str) -> str:
        """A replica of ``shard`` that is answering right now.

        After a heal the list may still begin with a replica that was stopped, so
        callers that just want "some replica" ask here rather than indexing.
        """
        candidates = self.shard_urls.get(shard, [])
        for url in candidates:
            try:
                http_json(url_for(url, "/health"), method="GET", timeout=2.0)
                return url
            except Exception:
                continue
        raise PeerError(f"no live replica of shard {shard}: {candidates}")

    def shard_of(self, account: str) -> str:
        return self.registry.current().shard_of(account)

    def stop_replica(self, shard: str, index: int = 0) -> None:
        """Stop one replica of a shard but leave the shard database alive."""
        label = f"shard-{shard}-{index + 1}"
        for service in self._services:
            if getattr(service, "service_name", "") == f"shard-{shard.lower()}" and \
                    self.ports.get(label) == service.server_address[1]:
                service.shutdown()
                return
        raise KeyError(label)

    def add_replica(self, shard: str) -> str:
        """Start an extra replica of an existing shard on the same database."""
        index = len(self.shard_urls.get(shard, [])) + 1
        db_path = os.path.join(self.root, f"shard-{shard}.db")
        mapping = self.registry.current()
        service = ShardService("127.0.0.1", 0, shard, db_path,
                               seed=build_seed(shard, mapping, self.base_cents,
                                               self.step_cents),
                               cluster="local-stack")
        url = self._start(service, f"shard-{shard}-{index}")
        http_json(url_for(url, "/internal/shard-map"),
                  {"epoch": mapping.epoch,
                   "ranges": [r.as_dict() for r in mapping.ranges],
                   "accounts": mapping.accounts, "note": mapping.note})
        self.shard_urls.setdefault(shard, []).append(url)
        self.registry.set_actors({shard: self.shard_urls[shard]})
        self._gateway.gateway.pools = {}
        self._gateway.gateway._pools_epoch = -1
        self._gateway.gateway._refresh_pools()
        return url

    def add_shard(self, shard: str, ranges: Optional[List[tuple]] = None,
                  accounts: Optional[List[str]] = None) -> List[str]:
        """Add a brand-new shard with its own database and replicas.

        This is the "add Shard C at runtime" step of the bonus.  The shard is
        registered with the registry (so the gateway knows its replicas) but the
        map still routes its range to the old owner until the migration publishes
        a new epoch.
        """
        shard = shard.upper()
        mapping = self.registry.current()
        ranges = ranges or []
        accounts = accounts or mapping.accounts
        provisional = ShardMap.from_ranges(mapping.epoch, ranges or
                                           [(r.shard, r.low, r.high) for r in mapping.ranges],
                                           accounts, note="provisional")
        db_path = os.path.join(self.root, f"shard-{shard}.db")
        urls: List[str] = []
        for index in range(1, self.replicas_per_shard + 1):
            service = ShardService("127.0.0.1", 0, shard, db_path,
                                   seed=build_seed(shard, provisional, self.base_cents,
                                                   self.step_cents),
                                   cluster="local-stack")
            url = self._start(service, f"shard-{shard}-{index}")
            http_json(url_for(url, "/internal/shard-map"),
                      {"epoch": mapping.epoch,
                       "ranges": [r.as_dict() for r in mapping.ranges],
                       "accounts": mapping.accounts, "note": mapping.note})
            urls.append(url)
        self.shard_urls[shard] = urls
        self.registry.set_actors({shard: urls})
        self._gateway.gateway.pools = {}
        self._gateway.gateway._pools_epoch = -1
        self._gateway.gateway._refresh_pools()
        return urls

    def migrate(self, source: str, target: str, low: str, high: str) -> Dict[str, Any]:
        """Run a live migration through the same engine the service uses."""
        from .migration import MigrationEngine

        engine = MigrationEngine(self.registry, push=self._push_all)
        result = engine.run(source, target, low, high)
        engine.cleanup_completed()
        return self.registry.get_migration(result["id"])

    def controller(self, desired: Any = 2, interval: float = 0.5, **kwargs: Any):
        """A self-healing controller wired to this stack.

        The spawner is the in-process one, so a healed replica is a real
        ``ShardService`` on the shard's existing database - the same healing
        action the compose stack performs by starting a container, minus Docker.
        """
        from .selfhealing import SelfHealingController, in_process_spawner

        return SelfHealingController(self.registry, in_process_spawner(self),
                                     desired=desired, interval=interval, **kwargs)

    def _push_all(self) -> Dict[str, Any]:
        body = self.registry.current_body() or {}
        payload = {"epoch": body.get("epoch"), "ranges": body.get("ranges", []),
                   "accounts": body.get("accounts", []), "note": body.get("note", "")}
        pushed: Dict[str, Any] = {}
        for shard, urls in self.shard_urls.items():
            for url in urls:
                try:
                    http_json(url_for(url, "/internal/shard-map"), payload, timeout=3.0)
                    pushed[url] = "ok"
                except Exception:
                    pushed[url] = "unreachable"
        return pushed


def main() -> int:
    """Run a local stack and print the URLs until interrupted."""
    import argparse

    parser = argparse.ArgumentParser(description="Run the whole platform locally (no Docker)")
    parser.add_argument("--root", default=None)
    parser.add_argument("--replicas", type=int, default=2)
    parser.add_argument("--strategy", default="least_connections")
    args = parser.parse_args()

    stack = LocalStack(root=args.root, replicas_per_shard=args.replicas,
                       strategy=args.strategy)
    print("platform is up:")
    print(f"  gateway     {stack.gateway_url}")
    print(f"  coordinator {stack.coordinator_url}")
    for shard, urls in stack.shard_urls.items():
        print(f"  shard {shard}     {', '.join(urls)}")
    print(f"  data        {stack.root}")
    print("\ntry:")
    print(f"  python3 -m platform.client --gateway {stack.gateway_url} balance A0500")
    print(f"  python3 -m platform.client --gateway {stack.gateway_url} transfer A0500 A1500 300")
    print("\nCtrl-C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        stack.cleanup(remove_files=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
