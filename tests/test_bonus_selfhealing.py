"""Expert Bonus II: an autonomous self-healing replica controller (+10%).

The graded property is that the platform repairs itself when a replica dies,
without Kubernetes, without Docker Swarm, without Docker restart policies and
without a prebuilt supervisor.  The controller under test is a plain Python
process (:mod:`platform.selfhealing`) that watches replicas and starts
replacements itself; these tests drive it against a real cluster of
``ShardService`` processes sharing a shard database.

Four things are checked, matching the four graded parts:

    1 maintains the configured replica count   test_maintains_replica_count
    2 replaces a dead replica                  test_replaces_dead_replica
    3 controller crash makes no duplicates     test_controller_crash_no_duplicates
    4 correctness under concurrent load        test_correctness_under_load

The disqualifiers are treated as failures: a restart policy that simply exhumes
the same dead process, two replacas fighting over one shard, or a heal that
loses money all fail here.
"""

from __future__ import annotations

import os
import socket
import threading
import time

from platform.client import BankClient
from platform.selfhealing import DockerEngineClient, SelfHealingController
from platform.local_stack import LocalStack
from platform.targets import http_json, url_for

from .helpers import TrafficGenerator, check, check_eq


def _live_count(controller: SelfHealingController, shard: str) -> int:
    return len(controller.survey()[shard]["live"])


def _kill_one_live(stack: LocalStack, shard: str) -> None:
    """Stop one replica of ``shard`` that is currently answering.

    Indexing ``shard_urls`` directly is not safe after a heal, because the list
    keeps the URL of the replica that was replaced; the controller's own survey is
    the honest view of who is up.
    """
    for index, url in enumerate(list(stack.shard_urls[shard])):
        try:
            http_json(url_for(url, "/health"), method="GET", timeout=2.0)
        except Exception:
            continue
        stack.stop_replica(shard, index)
        return
    raise AssertionError(f"no live replica of shard {shard} to kill")



def test_maintains_replica_count() -> str:
    """With every replica up, the controller changes nothing.

    A controller that "heals" a healthy cluster is worse than none: it would
    churn replicas forever.  This also pins the desired count to what is running,
    so a permanent scale-down is not mistaken for a repair.
    """
    with LocalStack(replicas_per_shard=2) as stack:
        controller = stack.controller(desired=2, interval=0.2)
        before = controller.survey()
        check_eq(sorted(before), ["A", "B"], "both shards should be surveyed")
        for shard, state in before.items():
            check_eq(len(state["live"]), 2, f"shard {shard} should have 2 live replicas")

        actions = controller.reconcile_once()
        check_eq(actions, [], "a healthy cluster must produce no healing actions")

        controller.start()
        try:
            time.sleep(1.0)
        finally:
            controller.stop()
        for shard in ("A", "B"):
            check_eq(_live_count(controller, shard), 2,
                     f"shard {shard} count drifted while the controller ran")
        check_eq(controller.stats["heals"], 0, "no replica should have been replaced")
        return "2 replicas per shard held steady across several sweeps"



def test_replaces_dead_replica() -> str:
    """Kill a replica; the controller starts a fresh one on the same database.

    The replacement is a new front end onto the *existing* shard database, so it
    serves the same balances with no copy step.  The test proves both that the
    count is restored and that the healed replica actually banks.
    """
    with LocalStack(replicas_per_shard=2) as stack:
        controller = stack.controller(desired=2, interval=0.2)

        account = "A1750"
        starting = stack.balance(account)
        stack.post("/deposit", {"account": account, "amount": 3.0,
                                "request_id": "before-heal"})
        expected = starting + 300

        dead_url = stack.shard_urls["B"][0]
        stack.stop_replica("B", 0)
        time.sleep(0.3)
        check_eq(_live_count(controller, "B"), 1, "the killed replica should read as down")

        actions = controller.reconcile_once()
        check_eq(len(actions), 1, "exactly one replacement should have been started")
        action = actions[0]
        check_eq(action["shard"], "B", "the wrong shard was healed")
        check_eq(action["action"], "replace", "the action should be a replacement")
        check(action["url"] != dead_url, "the replacement must be a new replica, not the dead one")

        check_eq(_live_count(controller, "B"), 2, "the replica count was not restored")
        actors = stack.registry.actors()["B"]
        check(dead_url not in actors, "the dead replica is still advertised to the gateway")

        check_eq(stack.balance(account), expected, "the healed replica changed a balance")
        stack.post("/deposit", {"account": account, "amount": 2.0,
                                "request_id": "after-heal"})
        check_eq(stack.balance(account), expected + 200,
                 "the healed replica did not accept a write")

        check_eq(controller.reconcile_once(), [],
                 "the controller kept healing a cluster that is already whole")
        return (f"replaced {dead_url} with {action['url']}; "
                f"balances intact at {expected}c")



def test_controller_crash_no_duplicates() -> str:
    """A controller that dies mid-heal must not leave two replacements behind.

    The claim that a slot is being filled is written to the registry *before* the
    replacement starts, so a fresh controller instance - the one that takes over
    after the crash - sees the unfinished claim and waits instead of spawning a
    second replica for the same gap.
    """
    with LocalStack(replicas_per_shard=2) as stack:
        first = stack.controller(desired=2, interval=0.2)
        claim = stack.registry.claim_replica_slot("B")
        check_eq(claim["granted"], True, "the first claimant should be granted the slot")

        spawned: list = []

        def counting_spawner(shard: str, generation: int) -> str:
            spawned.append((shard, generation))
            return stack.add_replica(shard)

        second = SelfHealingController(stack.registry, counting_spawner,
                                       desired=2, interval=0.2)
        stack.stop_replica("B", 0)
        time.sleep(0.3)

        actions = second.reconcile_once()
        check_eq(actions, [], "a phantom claim must suppress the replacement")
        check_eq(spawned, [], "no replica should have been started behind a live claim")
        check(second.stats["duplicates_suppressed"] >= 1,
              "the suppression should have been counted")

        stack.registry.release_replica_claim("B", claim["generation"])
        actions = second.reconcile_once()
        check_eq(len(actions), 1, "the released slot should now be filled")
        check_eq(len(spawned), 1, "exactly one replacement should exist")
        check_eq(_live_count(second, "B"), 2, "the count should be restored once")

        claims = stack.registry.replica_claims("B")
        live = [c for c in claims if c["state"] == "live"]
        check_eq(len(live), 1, f"expected one completed claim, have {len(live)}")
        return "phantom claim suppressed the duplicate; one replacement after release"



def test_correctness_under_load() -> str:
    """Kill replicas repeatedly while banking; no money may be lost or created.

    This is the property that separates a self-healing controller from one that
    merely keeps a process count: the cluster must keep serving and stay
    conservative throughout.
    """
    with LocalStack(replicas_per_shard=2) as stack:
        controller = stack.controller(desired=2, interval=0.1, health_timeout=0.5)
        controller.start()

        accounts = ["A0500", "A1500", "A1750"]
        baseline = stack.total_cents()
        traffic = TrafficGenerator(BankClient(stack.gateway_url, timeout=15, retries=4),
                                   accounts, amount=1.0, delay=0.001)
        traffic.start(workers=4)
        try:
            for round_index in range(3):
                for shard in ("A", "B"):
                    _kill_one_live(stack, shard)
                    deadline = time.time() + 25.0
                    while time.time() < deadline and _live_count(controller, shard) < 2:
                        time.sleep(0.1)
                    check_eq(_live_count(controller, shard), 2,
                             f"shard {shard} was not healed in round {round_index} "
                             f"(controller stats: {controller.stats})")
        finally:
            controller.stop()
            traffic.stop_and_join(timeout=20)

        summary = traffic.summary()
        check(summary["ok"] > 0, "no banking traffic got through")
        check(controller.stats["heals"] >= 3,
              f"the controller did not heal the killed replicas ({controller.stats})")

        after = stack.total_cents()
        check_eq(after - baseline, summary["confirmed_cents"],
                 "money was lost or created during self-healing")
        return (f"{summary['ok']} ops across {controller.stats['heals']} heals, "
                f"conserved {summary['confirmed_cents']}c")



def _fake_engine(script) -> tuple:
    """Start a Unix-socket server that answers Docker API calls from ``script``.

    ``script(reply)`` is called for each accepted connection and must write one
    raw HTTP/1.1 response.  Returns ``(client, server_socket, thread, path)``.
    """
    import tempfile

    path = os.path.join(tempfile.mkdtemp(), "docker.sock")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(path)
    server.listen(8)

    def serve() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            with conn:
                try:
                    conn.recv(65536)
                    conn.sendall(script())
                except OSError:
                    pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return DockerEngineClient(path), server, thread, path


def test_docker_api_reports_rejections() -> str:
    """A Docker rejection must surface as that rejection, not be swallowed.

    The failure this pins: the Engine API returned ``409 Conflict`` ("name
    already in use") for a replacement, but the old spawner caught *every*
    exception and fell back to the ``docker`` CLI - which does not exist inside
    the controller image - so every heal died with a misleading
    ``FileNotFoundError: 'docker'`` while the real cause was never logged.  The
    status line is now checked and Docker's own message is raised.

    Needs ``socket.AF_UNIX``, so it skips on Windows and runs on Linux (the VM).
    """
    if not hasattr(socket, "AF_UNIX"):
        return "skipped: this platform has no AF_UNIX sockets (run on Linux)"

    def conflict() -> bytes:
        body = b'{"message":"Conflict. The container name is already in use"}'
        return (b"HTTP/1.1 409 Conflict\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + body)

    client, server, _thread, path = _fake_engine(conflict)
    try:
        check_eq(client.available(), True, "the fake socket should be found")
        raised = None
        try:
            client.request("POST", "/containers/create?name=x", {"Image": "y"})
        except RuntimeError as exc:
            raised = str(exc)
        check(raised is not None, "a 409 must raise, not return a body")
        check("409" in raised and "already in use" in raised,
              f"the rejection message was lost: {raised!r}")
    finally:
        server.close()

    def missing() -> bytes:
        body = b'{"message":"No such container: nope"}'
        return (b"HTTP/1.1 404 Not Found\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)

    client2, server2, _t2, path2 = _fake_engine(missing)
    try:
        check_eq(client2.is_running("nope"), None, "404 should read as absent")
    finally:
        server2.close()

    return "409 surfaces Docker's message; 404 reads as absent (name reusable)"


def test_spawner_mounts_the_compose_volume() -> str:
    """A replacement must mount the shard's real Compose volume, not a new one.

    The bug this pins: the volume was built from the upper-cased shard letter
    (``bank_shard-B``), but Compose names the volume from the service key
    (``bank_shard_b``).  Docker silently creates the missing volume, so every
    healed replica mounted a *fresh, empty* database and served default balances
    instead of the shard's real ones - a data-correctness fault that no in-process
    test could see, because they share the database in memory.

    This drives the real spawner against a fake engine, captures the create
    request, and asserts the bind and environment are exactly what Compose uses.
    """
    if not hasattr(socket, "AF_UNIX"):
        return "skipped: this platform has no AF_UNIX sockets (run on Linux)"

    import json as _json

    captured = {}

    def capture() -> bytes:
        return b"HTTP/1.1 201 Created\r\nContent-Length: 12\r\n\r\n{\"Id\":\"abc\"}"

    import tempfile

    path = os.path.join(tempfile.mkdtemp(), "docker.sock")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(path)
    server.listen(8)

    def serve() -> None:
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            with conn:
                try:
                    data = conn.recv(65536)
                    _, _, body = data.partition(b"\r\n\r\n")
                    if body.strip():
                        captured["body"] = body
                    conn.sendall(capture())
                except OSError:
                    pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    from platform.selfhealing import compose_spawner

    engine = DockerEngineClient(path)
    spawn = compose_spawner(project="bank", image="bank/shard:latest",
                            network="bank_banknet", engine=engine)
    try:
        url = spawn("B", 1)
        check(url.endswith(":8080"), f"the replacement URL looks wrong: {url}")
        check("bank-healer-b" in url, f"expected a reserved-prefix name, got {url}")

        config = _json.loads(captured["body"])
        binds = config["HostConfig"]["Binds"]
        check("bank_shard_b:/data" in binds,
              f"the replacement did not mount the Compose volume (binds={binds})")
        check(config["HostConfig"]["NetworkMode"] == "bank_banknet",
              "the replacement must join the platform network")
        env = config["Env"]
        for want in ("ROLE=shard", "CLUSTER=compose", "SHARD=B",
                     "SHARD_DB=/data/shard-B.db", "SHARD_PORT=8080"):
            check(want in env, f"the replacement environment is missing {want!r}: {env}")
        check("RestartPolicy" not in config["HostConfig"],
              "a restart policy is forbidden by the assignment")
    finally:
        server.close()

    return "replacement mounts bank_shard_b, joins the network, no restart policy"
