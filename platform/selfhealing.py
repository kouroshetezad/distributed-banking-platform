"""Expert Bonus II: an autonomous, self-healing replica controller.

The controller keeps a **desired replica count** per shard and makes reality
match it, with no Kubernetes, no Docker Swarm, no Docker restart policy and no
prebuilt supervisor.  It is a plain Python process that:

1. sweeps every registered replica of every shard and asks it ``/health``;
2. for a shard that has fallen below its desired count, starts a replacement
   replica on the *same* shard database and registers it with the shard map.

Two properties make it safe rather than merely convenient:

* **No divergent state.**  A replacement is a fresh front end onto the shard's
  existing database, not a copy, so a healed shard never has two disagreeing
  balances and the replacement never needs a data transfer.
* **No duplicate healing.**  Every replacement is claimed through a durable
  record in the registry before it is started, keyed by shard and generation.
  A controller that crashes mid-heal and restarts therefore resumes that claim
  instead of starting a second replica for the same gap, which is what keeps the
  count from creeping upward.

The *spawn* step is injected (``spawner``) so the same controller drives the
in-process :class:`~platform.local_stack.LocalStack` in the tests and a real
cluster in the compose stack, where a spawner shells out to ``docker run`` on the
shared volume rather than constructing a ``ShardService`` in process.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from .storage.registry import ShardMapRegistry
from .targets import http_json, url_for

try:
    from .http_service import configure_logging

    LOG = configure_logging("bank.selfhealing")
except Exception:
    import logging

    LOG = logging.getLogger("bank.selfhealing")


Spawner = Callable[[str, int], str]


class SelfHealingController:
    """Watch replicas and replace the ones that stop answering.

    Parameters
    ----------
    registry:
        The shard-map registry.  It is both the source of truth for which
        replicas should exist and the durable store for healing claims.
    spawner:
        Called to create one replacement replica of a shard.  See :data:`Spawner`.
    desired:
        Replicas wanted per shard.  A scalar applies to every shard; a mapping
        overrides individual shards.  Defaults to two, the assignment's floor.
    interval:
        Seconds between sweeps of the background loop.
    health_timeout:
        Per-replica health probe timeout.
    on_change:
        Optional callback fired after a successful heal or scale-up, mainly for
        tests and for logging in the compose stack.
    """

    def __init__(self, registry: ShardMapRegistry, spawner: Spawner,
                 desired: Any = 2, interval: float = 1.0,
                 health_timeout: float = 1.5,
                 on_change: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.registry = registry
        self.spawner = spawner
        self.interval = float(interval)
        self.health_timeout = float(health_timeout)
        self.on_change = on_change
        if isinstance(desired, dict):
            self._desired_map = {str(k).upper(): int(v) for k, v in desired.items()}
            self._desired_default = 2
        else:
            self._desired_map = {}
            self._desired_default = int(desired)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.stats: Dict[str, int] = {"sweeps": 0, "heals": 0, "scaled_up": 0,
                                      "duplicates_suppressed": 0, "failures": 0}
        self.actions: List[Dict[str, Any]] = []


    def desired_for(self, shard: str) -> int:
        return self._desired_map.get(shard.upper(), self._desired_default)


    def _healthy(self, url: str) -> bool:
        try:
            reply = http_json(url_for(url, "/health"), method="GET",
                              timeout=self.health_timeout)
            return reply.get("status") in ("ok", "success")
        except Exception:
            return False

    def survey(self) -> Dict[str, Dict[str, Any]]:
        """Which replicas of which shards answer right now.

        The registry is read fresh each sweep so a replica another controller
        registered - or a shard that was added at runtime - is seen immediately.
        """
        actors = self.registry.actors()
        report: Dict[str, Dict[str, Any]] = {}
        for shard, urls in sorted(actors.items()):
            live = [url for url in urls if self._healthy(url)]
            report[shard] = {"registered": list(urls), "live": live,
                             "dead": [u for u in urls if u not in live],
                             "desired": self.desired_for(shard)}
        return report


    def reconcile_once(self) -> List[Dict[str, Any]]:
        """One pass: replace every dead replica, up to the desired count.

        Returns the list of actions taken.  Runs the whole pass under a lock so
        two sweeps (or a sweep and an explicit call) cannot both decide to heal
        the same gap and overshoot the desired count.
        """
        with self._lock:
            report = self.survey()
            actions: List[Dict[str, Any]] = []
            for shard, state in report.items():
                missing = state["desired"] - len(state["live"])
                for _ in range(max(0, missing)):
                    action = self._heal(shard, state)
                    if action is not None:
                        actions.append(action)
            self.stats["sweeps"] += 1
            if actions and self.on_change:
                for action in actions:
                    try:
                        self.on_change(action)
                    except Exception:
                        LOG.warning("self-healing on_change callback failed", exc_info=True)
            return actions

    def _heal(self, shard: str, state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Claim a slot for one replacement replica and start it."""
        claim = self.registry.claim_replica_slot(shard)
        if not claim["granted"]:
            self.stats["duplicates_suppressed"] += 1
            LOG.info("self-healing: a replacement for shard %s is already claimed (%s)",
                     shard, claim["reason"])
            return None
        generation = claim["generation"]
        LOG.warning("self-healing: shard %s is down to %d/%d replicas, replacing (%s)",
                    shard, len(state["live"]), state["desired"], claim["reason"])
        try:
            url = self.spawner(shard, generation)
        except Exception as exc:
            self.registry.release_replica_claim(shard, generation)
            self.stats["failures"] += 1
            LOG.error("self-healing: could not start a replacement for shard %s: %s",
                      shard, exc)
            return None

        actors = self.registry.actors()
        urls = list(actors.get(shard, []))
        new_url = url.rstrip("/")
        urls = [u for u in urls if u != new_url
                and (u in state["live"] or self._healthy(u))]
        urls.append(new_url)
        self.registry.set_actors({shard: urls})
        self.registry.complete_replica_claim(shard, generation, url)
        self.stats["heals"] += 1
        action = {"action": "replace", "shard": shard, "url": url.rstrip("/"),
                  "generation": generation, "live_before": len(state["live"]),
                  "desired": state["desired"], "at": time.time()}
        self.actions.append(action)
        LOG.warning("self-healing: shard %s healed with %s", shard, url)
        return action

    def ensure_desired(self) -> List[Dict[str, Any]]:
        """Grow a shard above its desired count, for shards with no dead replicas.

        A brand-new shard registered with one replica should reach its desired
        count without a fake failure, so this path exists separately from
        :meth:`_heal` and goes through the same durable claim.
        """
        with self._lock:
            report = self.survey()
            actions: List[Dict[str, Any]] = []
            for shard, state in report.items():
                deficit = state["desired"] - len(state["live"])
                for _ in range(max(0, deficit)):
                    action = self._heal(shard, state)
                    if action is not None:
                        actions.append(action)
            self.stats["scaled_up"] += len(actions)
            return actions


    def start(self) -> "SelfHealingController":
        if self._thread is not None and self._thread.is_alive():
            return self
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="self-healing",
                                        daemon=True)
        self._thread.start()
        return self

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.reconcile_once()
            except Exception:
                self.stats["failures"] += 1
                LOG.exception("self-healing sweep failed")
            self._stop.wait(self.interval)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def __enter__(self) -> "SelfHealingController":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()



def in_process_spawner(stack) -> Spawner:
    """A spawner that adds a replica inside a :class:`LocalStack`.

    This is what the tests use: it is the cheapest faithful replacement, because
    ``add_replica`` starts the *real* ``ShardService`` on the shard's existing
    database, exactly as a container would.
    """

    def spawn(shard: str, generation: int) -> str:
        url = stack.add_replica(shard)
        LOG.info("self-healing: spawned in-process replica for shard %s at %s",
                 shard, url)
        return url

    return spawn


def compose_spawner(project: str = "bank", image: str = "",
                    network: str = "", port: int = 8080) -> Spawner:
    """A spawner that starts a fresh container on the shard's shared volume.

    Used by the compose deployment.  It deliberately does **not** use a Docker
    restart policy, Swarm or Kubernetes: the controller itself decides when a
    replica is dead and starts a replacement, which is the assignment's
    requirement.  The container engine is spoken to over its HTTP API with
    ``urllib``, so no Docker SDK is needed inside the controller image; if only
    the CLI is available, it falls back to ``docker run``.
    """
class DockerEngineClient:
    """Talk to the Docker Engine API over its Unix socket, nothing else.

    Kept as its own class so the HTTP handling can be tested against a fake
    socket without Docker, and so the fallback to the ``docker`` CLI is decided
    **once**, by whether the socket exists, rather than by catching every
    exception - catching everything is what once turned a clear "name already in
    use" rejection into a confusing ``FileNotFoundError: 'docker'``.
    """

    def __init__(self, sock_path: str = "/var/run/docker.sock", timeout: float = 30.0):
        self.sock_path = sock_path
        self.timeout = timeout

    def available(self) -> bool:
        """True when the Engine socket is present (the preferred transport)."""
        import os

        return os.path.exists(self.sock_path)

    def request(self, method: str, path_query: str, payload: Any = None) -> Any:
        """One API call, returning the decoded body or raising a clear error.

        The HTTP status line is checked: a 4xx/5xx becomes a ``RuntimeError``
        carrying Docker's own ``message``, so a rejected create is reported as
        that rejection.  The body is read via ``Content-Length`` because Docker
        keeps an HTTP/1.1 connection open rather than closing it per request.
        """
        import json as _json
        import socket

        body = _json.dumps(payload).encode() if payload is not None else b""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        try:
            sock.connect(self.sock_path)
            head = (f"{method} {path_query} HTTP/1.1\r\n"
                    f"Host: docker\r\n"
                    f"Content-Type: application/json\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    f"Connection: close\r\n\r\n").encode()
            sock.sendall(head + body)
            chunks = []
            while True:
                data = sock.recv(65536)
                if not data:
                    break
                chunks.append(data)
        finally:
            sock.close()

        raw = b"".join(chunks)
        header_bytes, _, payload_bytes = raw.partition(b"\r\n\r\n")
        header = header_bytes.decode("latin-1")
        status_line = header.split("\r\n", 1)[0]
        parts = status_line.split(" ", 2)
        status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        try:
            reply = _json.loads(payload_bytes.decode() or "null")
        except ValueError:
            reply = None
        if status < 200 or status >= 300:
            message = reply.get("message") if isinstance(reply, dict) else reply
            raise RuntimeError(
                f"docker API {method} {path_query} -> {status_line.strip()}: "
                f"{message or payload_bytes[:200]!r}")
        return reply

    def cli(self, args: List[str]) -> str:
        """Run a ``docker`` CLI command, or explain if the binary is absent."""
        import subprocess

        try:
            done = subprocess.run(["docker", *args], capture_output=True, text=True)
        except FileNotFoundError as exc:
            raise RuntimeError(
                "no Docker socket and no `docker` CLI in this container; mount "
                "the socket at /var/run/docker.sock (see docker-compose.yml, "
                "service `selfhealer`)") from exc
        if done.returncode != 0:
            raise RuntimeError(f"docker {' '.join(args)} failed: {done.stderr.strip()}")
        return done.stdout.strip()

    def is_running(self, name: str) -> Optional[bool]:
        """``True``/``False`` for a running/stopped container, ``None`` if absent.

        The distinction matters: a stopped leftover may be cleared and its name
        reused, but a *running* container must never be removed.
        """
        if self.available():
            try:
                info = self.request("GET", f"/containers/{name}/json")
            except RuntimeError:
                return None
            return bool((info or {}).get("State", {}).get("Running"))
        import subprocess

        shown = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", name],
                               capture_output=True, text=True)
        if shown.returncode != 0:
            return None
        return shown.stdout.strip() == "true"

    def remove(self, name: str) -> None:
        """Best effort: remove a container by name, ignoring its absence."""
        try:
            if self.available():
                self.request("DELETE", f"/containers/{name}?force=true")
            else:
                import subprocess

                subprocess.run(["docker", "rm", "-f", name],
                               capture_output=True, text=True)
        except Exception:
            pass


def compose_spawner(project: str = "bank", image: str = "",
                    network: str = "", port: int = 8080,
                    engine: Optional[DockerEngineClient] = None) -> Spawner:
    """A spawner that starts a fresh container on the shard's shared volume.

    Used by the compose deployment.  It deliberately does **not** use a Docker
    restart policy, Swarm or Kubernetes: the controller itself decides when a
    replica is dead and starts a replacement, which is the assignment's
    requirement.  The Engine API is spoken to over its Unix socket with the
    standard library; the ``docker`` CLI is used only when there is genuinely no
    socket.
    """
    import os

    client = engine or DockerEngineClient(os.environ.get("DOCKER_SOCK",
                                                         "/var/run/docker.sock"))
    net = network or f"{project}_banknet"
    img = image or f"{project}/shard:latest"

    def pick_name(shard: str, generation: int) -> str:
        """A container name this replacement can use without a collision.

        The name is built from a **reserved prefix** (``<project>-healer-``) that
        Compose never produces, so a replacement can never be confused with, or
        collide with, a Compose-managed replica: Compose names its containers
        ``<project>-<service>-<index>`` (for example ``bank-shard-b-1-1``), and a
        replacement called ``bank-shard-b-1`` would sit one character away from
        that on the shared network - resolved to the wrong container the moment
        anything asked for the Compose alias.  The generation number only
        distinguishes successive replacements of one shard; if a name is somehow
        already held by a *running* container it is skipped, and a stopped one is
        cleared first.
        """
        candidate = generation
        for _ in range(1000):
            name = f"{project}-healer-{shard.lower()}-{candidate}"
            if client.is_running(name) is not True:
                client.remove(name)
                return name
            candidate += 1
        raise RuntimeError(f"no free container name for shard {shard}")

    def spawn(shard: str, generation: int) -> str:
        shard = shard.upper()
        volume = f"{project}_shard_{shard.lower()}"
        name = pick_name(shard, generation)
        env = ["ROLE=shard", "CLUSTER=compose", f"SHARD={shard}",
               f"SHARD_DB=/data/shard-{shard}.db", f"SHARD_PORT={port}"]

        if client.available():
            config = {
                "Image": img,
                "Env": env,
                "HostConfig": {
                    "Binds": [f"{volume}:/data"],
                    "NetworkMode": net,
                },
            }
            reply = client.request("POST", f"/containers/create?name={name}", config)
            cid = (reply or {}).get("Id")
            if not cid:
                raise RuntimeError(f"docker create returned no Id: {reply}")
            client.request("POST", f"/containers/{cid}/start")
        else:
            args = ["run", "-d", "--name", name, "--network", net]
            for item in env:
                args += ["-e", item]
            args += ["-v", f"{volume}:/data", img]
            client.cli(args)
        return f"http://{name}:{port}"

    return spawn



def build_from_env() -> SelfHealingController:
    """Build a controller wired to the shared registry and the Docker socket."""
    from .storage.registry import ShardMapRegistry
    from .targets import env, env_int, split_csv

    registry = ShardMapRegistry(env("REGISTRY_DB_PATH", "data/registry.db"))
    project = env("COMPOSE_PROJECT", "bank")
    desired = env_int("DESIRED_REPLICAS", 2)
    return SelfHealingController(
        registry,
        compose_spawner(project=project, image=env("SHARD_IMAGE", f"{project}/shard:latest"),
                        network=env("BANK_NETWORK", f"{project}_banknet")),
        desired=desired,
        interval=float(env("HEAL_INTERVAL", "2.0")),
        health_timeout=float(env("HEAL_TIMEOUT", "2.0")),
    )


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Autonomous replica controller (Expert Bonus II)")
    parser.add_argument("--registry-db", default=None)
    parser.add_argument("--desired", type=int, default=None)
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--once", action="store_true",
                        help="run a single sweep and exit (for a demo or a cron)")
    args = parser.parse_args(argv)

    if args.registry_db:
        os.environ["REGISTRY_DB_PATH"] = args.registry_db
    if args.desired is not None:
        os.environ["DESIRED_REPLICAS"] = str(args.desired)
    if args.interval is not None:
        os.environ["HEAL_INTERVAL"] = str(args.interval)

    controller = build_from_env()
    if args.once:
        actions = controller.reconcile_once()
        print(json.dumps({"actions": actions, "stats": controller.stats}, indent=2))
        return 0
    LOG.info("self-healing controller up: desired=%s interval=%ss",
             controller.desired_for("A"), controller.interval)
    controller.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        controller.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
