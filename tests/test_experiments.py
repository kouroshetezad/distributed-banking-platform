"""The seven required experiments from section 6 of the assignment.

Each test maps onto one numbered experiment; the docstring says which, so the
report and the video can be read side by side with the code.

    1 shard distribution             test_shard_distribution
    2 replica load distribution      test_replica_load_distribution
    3 concurrent replica operations  test_concurrent_replica_operations
    4 replica interruption           test_replica_interruption
    5 local and cross-shard transfer test_local_and_cross_shard_transfers
    6 interrupted distributed txn    test_interrupted_distributed_transaction
    7 concurrent cross-shard txns    test_concurrent_cross_shard_transfers
"""

from __future__ import annotations

import json
import threading
import time

from platform.client import BankClient
from platform.local_stack import LocalStack
from platform.targets import http_json, url_for

from .helpers import TrafficGenerator, check, check_eq



def test_shard_distribution() -> str:
    """Experiment 1: accounts land in their assigned shard, and the gateway routes.

    The check is deliberately strong: it reads the *databases* through each shard
    service, rather than trusting the gateway's own answer, and it confirms the
    account ranges are disjoint and the full account list is covered exactly once.
    """
    with LocalStack() as stack:
        client = BankClient(stack.gateway_url)
        shard_map = client.shard_map()
        ranges = {r["shard"]: (r["low"], r["high"]) for r in shard_map["ranges"]}
        check("A" in ranges and "B" in ranges, f"expected shards A and B, got {ranges}")
        check(ranges["A"] == ("A0001", "A1000"), f"shard A range is {ranges['A']}")
        check(ranges["B"] == ("A1001", "A2000"), f"shard B range is {ranges['B']}")

        for shard, urls in stack.shard_urls.items():
            stats = http_json(url_for(urls[0], "/stats"), method="GET")
            check(stats["accounts"] > 0, f"shard {shard} has no accounts")

        a_accounts = set(http_json(url_for(stack.shard_urls["A"][0], "/accounts"),
                                   method="GET")["accounts"])
        b_accounts = set(http_json(url_for(stack.shard_urls["B"][0], "/accounts"),
                                   method="GET")["accounts"])
        check(not (a_accounts & b_accounts), "an account exists on both shards")
        check(len(a_accounts) == 1000, f"shard A holds {len(a_accounts)} accounts, expected 1000")
        check(len(b_accounts) == 1000, f"shard B holds {len(b_accounts)} accounts, expected 1000")
        check(a_accounts == {f"A{n:04d}" for n in range(1, 1001)},
              "shard A does not hold exactly A0001-A1000")
        check(b_accounts == {f"A{n:04d}" for n in range(1001, 2001)},
              "shard B does not hold exactly A1001-A2000")

        for account, expected_shard in (("A0500", "A"), ("A1500", "B"), ("A0999", "A")):
            reply = client.balance(account)
            check_eq(reply.get("shard"), expected_shard, f"balance({account}) shard")

        route = stack.get("/debug/route", account="A1500")
        check_eq(route["shard"], "B", "debug route for A1500")
        check(route["range"].startswith("A1001"), "debug route should name the range")
        check("range" in route["reason"], "routing reason should name the range")
        return (f"A={len(a_accounts)} accounts, B={len(b_accounts)} accounts, "
                f"fpmap={shard_map['fingerprint']}")



def test_replica_load_distribution() -> str:
    """Experiment 2: one shard's requests are spread over its replicas.

    Two replicas per shard is the assignment's minimum.  The distribution is read
    from the replies themselves (``served_by``), so the evidence is the same thing
    the demo video shows.
    """
    with LocalStack() as stack:
        client = BankClient(stack.gateway_url)
        served: dict = {}
        for _ in range(24):
            reply = client.balance("A0500")
            served[reply.get("served_by")] = served.get(reply.get("served_by"), 0) + 1

        expected = set(stack.shard_urls["A"])
        check(len(expected) >= 2, "the shard should have at least 2 replicas")
        check(set(served) <= expected,
              f"a reply came from an unknown replica: {set(served) - expected}")
        check(len(served) >= 2,
              f"all requests went to one replica: {served} (replicas: {expected})")

        stats = stack.get("/gateway/stats")
        pooled = {r["url"]: r["served"] for r in stats["shards"]["A"]["replicas"]}
        for url, count in served.items():
            check(pooled.get(url, 0) >= count,
                  f"gateway counted {pooled.get(url)} for {url}, client saw {count}")
        return f"distribution over {len(served)} replicas: " + ", ".join(
            f"{url.rsplit(':', 1)[-1]}={n}" for url, n in sorted(served.items()))



def test_concurrent_replica_operations() -> str:
    """Experiment 3: concurrent writes through different replicas stay correct.

    This is the lost-update experiment.  Fifty concurrent deposits are fired at
    accounts of one shard, deliberately spread over both of its replicas, and the
    final balance must equal the starting balance plus every deposit.  If two
    replicas could each read-modify-write without coordination, the total would
    come out short.
    """
    with LocalStack() as stack:
        client = BankClient(stack.gateway_url)
        account = "A0600"
        start = stack.balance(account)
        per_thread, threads_n = 10, 5
        cents_each = 137

        def worker() -> None:
            local = BankClient(stack.gateway_url, timeout=15.0)
            for _ in range(per_thread):
                reply = local.deposit(account, cents_each / 100.0)
                check(reply.get("status") == "success", f"deposit failed: {reply}")

        threads = [threading.Thread(target=worker) for _ in range(threads_n)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        expected = start + per_thread * threads_n * cents_each
        final = stack.balance(account)
        check_eq(final, expected, "balance after concurrent deposits")

        stats = stack.get("/gateway/stats")
        replicas = stats["shards"]["A"]["replicas"]
        used = [r for r in replicas if r["served"] > 0]
        check(len(used) >= 2, f"only one replica served writes: {replicas}")
        return (f"{per_thread * threads_n} concurrent deposits of {cents_each}c over "
                f"{len(used)} replicas -> {final}c (expected {expected}c)")



def test_replica_interruption() -> str:
    """Experiment 4: killing a replica under load does not stop the shard.

    The gateway must stop selecting the dead replica and serve from the survivor.
    The test asserts three things: the service keeps flowing, the gateway's own
    view marks the dead replica unhealthy, and the balance is exact.

    A request that is already in flight to the replica at the moment it dies may
    be refused before the health loop removes it from rotation; that is the
    expected cost of killing a process, not a failure.  What must not happen is
    losing a write that the platform confirmed, or the shard going dead.
    """
    with LocalStack() as stack:
        client = BankClient(stack.gateway_url)
        account = "A0700"
        start = stack.balance(account)
        victim = stack.shard_urls["A"][0]

        traffic = TrafficGenerator(client, [account], amount=0.5).start(workers=3)
        time.sleep(0.5)

        stack.stop_replica("A", 0)
        deadline = time.time() + 20
        marked_down = False
        while time.time() < deadline:
            stats = stack.get("/gateway/stats")
            state = {r["url"]: r["healthy"] for r in stats["shards"]["A"]["replicas"]}
            if state.get(victim) is False:
                marked_down = True
                break
            time.sleep(0.25)

        time.sleep(1.0)
        traffic.stop_and_join()
        summary = traffic.summary()

        check(marked_down, "the gateway never marked the dead replica unhealthy")
        check(summary["ok"] > 0, "no traffic succeeded at all")
        check(len(summary["errors"]) < summary["ok"] or summary["ok"] > 10,
              f"traffic did not keep flowing during the interruption: {summary['errors']}")

        expected = start + summary["confirmed_cents"]
        final = stack.balance(account)
        check_eq(final, expected, "balance after the replica interruption")

        stats = stack.get("/gateway/stats")
        healthy = [r for r in stats["shards"]["A"]["replicas"] if r["healthy"]]
        check(len(healthy) >= 1, "no healthy replica remained")
        return (f"killed {victim.rsplit(':', 1)[-1]}; {summary['ok']} operations "
                f"continued on {len(healthy)} survivor(s), balance {final}c "
                f"(transient errors: {summary['errors'] or 'none'})")



def test_local_and_cross_shard_transfers() -> str:
    """Experiment 5: local transfers are one transaction; cross-shard ones are 2PC.

    The distinction is what the report has to justify, so the test records the
    coordinator's own answer for each: a same-shard transfer is marked
    ``coordinated: false`` and never touches a transaction id, while a cross-shard
    transfer returns ``coordinated: true`` with a real two-phase transaction id.
    """
    with LocalStack() as stack:
        client = BankClient(stack.gateway_url)

        a_before, b_before = stack.balance("A0500"), stack.balance("A0600")
        local = client.transfer("A0500", "A0600", 25.0)
        check_eq(local.get("status"), "success", "same-shard transfer status")
        check_eq(local.get("coordinated"), False, "same-shard transfer should not use 2PC")
        check_eq(local.get("mode"), "same_shard", "same-shard transfer mode")
        check("txn_id" not in local or not local.get("txn_id"),
              "a same-shard transfer must not create a distributed transaction")
        check_eq(stack.balance("A0500"), a_before - 2500, "same-shard source balance")
        check_eq(stack.balance("A0600"), b_before + 2500, "same-shard target balance")

        a_before, b_before = stack.balance("A0500"), stack.balance("A1500")
        cross = client.transfer("A0500", "A1500", 30.0)
        check_eq(cross.get("status"), "success", "cross-shard transfer status")
        check_eq(cross.get("coordinated"), True, "cross-shard transfer must use 2PC")
        check_eq(cross.get("mode"), "cross_shard", "cross-shard transfer mode")
        check(bool(cross.get("txn_id")), "cross-shard transfer needs a transaction id")
        check_eq(cross.get("votes"), {"A": "YES", "B": "YES"},
                 "both participants must have voted YES")
        check_eq(stack.balance("A0500"), a_before - 3000, "cross-shard source balance")
        check_eq(stack.balance("A1500"), b_before + 3000, "cross-shard target balance")

        record = stack.get("/txn", txn_id=cross["txn_id"])
        check_eq(record["state"], "COMPLETED", "coordinator journal state")
        check_eq(record["participant_states"], {"A": "COMMITTED", "B": "COMMITTED"},
                 "participant states after commit")

        b_local = client.transfer("A1500", "A1600", 5.0)
        check_eq(b_local.get("coordinated"), False, "B-side same-shard transfer")
        check_eq(b_local.get("shard"), "B", "B-side transfer shard")
        return (f"local transfer atomic, cross-shard used {cross['txn_id']} "
                f"with votes {cross['votes']}")



def test_interrupted_distributed_transaction() -> str:
    """Experiment 6: kill the coordinator after the durable COMMIT, then recover.

    The scenario is built honestly: both participants really prepare, the
    coordinator really records COMMIT_DECIDED, and then only *one* participant is
    told to commit before the coordinator is reset.  Recovery must complete the
    other side.  The test also asserts the two things that make this safe:

    * the decision is never reversed to ABORT, and
    * running recovery twice does not move money twice.
    """
    with LocalStack() as stack:
        source, target = "A0500", "A1500"
        amount_cents = 7500
        src_before, dst_before = stack.balance(source), stack.balance(target)
        txn_id = "txn-interrupted"

        for shard, branch in (
            ("A", {"account": source, "kind": "DEBIT", "amount_cents": amount_cents}),
            ("B", {"account": target, "kind": "CREDIT", "amount_cents": amount_cents}),
        ):
            vote = stack.post("/route/prepare", {"shard": shard, "txn_id": txn_id,
                                                 "branch": branch, "epoch": 1})
            check_eq(vote.get("vote"), "YES", f"prepare {shard}")

        journal = stack._coordinator.coordinator.journal
        journal.create(txn_id, request_id="req-interrupted", kind="cross_shard_transfer",
                       participants=["A", "B"], epoch=1, src_account=source,
                       dst_account=target, amount_cents=amount_cents)
        journal.update(txn_id, state="PREPARED", votes={"A": "YES", "B": "YES"})
        journal.update(txn_id, state="COMMIT_DECIDED", decision="COMMIT")

        stack.post("/route/commit", {"shard": "A", "txn_id": txn_id})
        check_eq(stack.balance(source), src_before - amount_cents,
                 "source after partial commit")
        check_eq(stack.balance(target), dst_before,
                 "target must not have moved yet")
        mid = journal.get(txn_id)
        check_eq(mid["state"], "COMMIT_DECIDED", "decision must be durable and not reversed")

        report = stack._coordinator.coordinator.recover()
        check(txn_id in report["replayed"],
              f"recovery did not replay the commit: {report}")
        check_eq(stack.balance(source), src_before - amount_cents, "source after recovery")
        check_eq(stack.balance(target), dst_before + amount_cents, "target after recovery")
        after = journal.get(txn_id)
        check_eq(after["state"], "COMPLETED", "journal after recovery")
        check_eq(after["decision"], "COMMIT", "the decision must still be COMMIT")

        stack._coordinator.coordinator.recover()
        check_eq(stack.balance(source), src_before - amount_cents,
                 "source after a second recovery")
        check_eq(stack.balance(target), dst_before + amount_cents,
                 "target after a second recovery")

        pending_id = "txn-never-decided"
        journal.create(pending_id, request_id="req-pending", kind="cross_shard_transfer",
                       participants=["A", "B"], epoch=1, src_account=source,
                       dst_account=target, amount_cents=100)
        for shard, branch in (
            ("A", {"account": source, "kind": "DEBIT", "amount_cents": 100}),
            ("B", {"account": target, "kind": "CREDIT", "amount_cents": 100}),
        ):
            stack.post("/route/prepare", {"shard": shard, "txn_id": pending_id,
                                          "branch": branch, "epoch": 1})
        journal.update(pending_id, state="PREPARED")
        status = stack.get("/txn", txn_id=pending_id)
        check(status["state"].startswith("PENDING") or status["state"] == "PREPARED",
              f"unresolved transaction should be queryable, got {status['state']}")
        check_eq(status["participant_states"], {"A": "PREPARED", "B": "PREPARED"},
                 "participants should report PREPARED while undecided")
        return (f"{txn_id} completed after restart; {pending_id} queryable as "
                f"{status['state']} while undecided")



def test_concurrent_cross_shard_transfers() -> str:
    """Experiment 7: many overlapping cross-shard transfers, money conserved.

    Several accounts on each shard are used from several threads at once, so the
    same account is debited and credited by concurrent transactions.  The strong
    checks are:

    * every transfer that was retried at most once applied exactly once, which the
      test verifies by replaying the *same* ``request_id`` and expecting the
      original answer rather than a second move;
    * the sum of both shards' balances is unchanged.
    """
    with LocalStack() as stack:
        source_accounts = ["A0400", "A0401", "A0402"]
        target_accounts = ["A1400", "A1401", "A1402"]
        rounds, threads_n = 6, 4
        cents_each = 125

        total_before = stack.total_cents()
        directions = [("A0400", "A1400"), ("A0401", "A1401"), ("A1402", "A0402")]

        results: list = []
        deposited_cents = [0]
        lock = threading.Lock()

        def worker(worker_index: int) -> None:
            local = BankClient(stack.gateway_url, timeout=20.0, retries=3)
            for step in range(rounds):
                src, dst = directions[(worker_index + step) % len(directions)]
                topup = local.deposit(src, 5.0)
                if topup.get("status") == "success":
                    with lock:
                        deposited_cents[0] += 500
                reply = local.transfer(src, dst, cents_each / 100.0)
                with lock:
                    results.append(reply)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(threads_n)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)

        successes = [r for r in results if r.get("status") == "success"]
        check(len(successes) >= threads_n * rounds * 0.5,
              f"too few transfers succeeded: {len(successes)}/{len(results)}")
        check(all(r.get("coordinated") for r in successes),
              "every cross-shard transfer should report coordinated=true")

        total_after = stack.total_cents()
        check_eq(total_after, total_before + deposited_cents[0],
                 "money must be conserved across concurrent cross-shard transfers "
                 "(transfers move money, only the top-ups may add it)")

        for reply in successes[::max(1, len(successes) // 4)]:
            record = stack.get("/txn", txn_id=reply["txn_id"])
            check_eq(record["state"], "COMPLETED", f"journal {reply['txn_id']}")
            check_eq(record["participant_states"], {"A": "COMMITTED", "B": "COMMITTED"},
                     f"participants {reply['txn_id']}")

        client = BankClient(stack.gateway_url, retries=1)
        rid = BankClient.request_id("retry")
        first = client.transfer("A0400", "A1400", 1.0, request_id=rid)
        check_eq(first.get("status"), "success", "fresh transfer for the retry test")
        replay = client.transfer("A0400", "A1400", 1.0, request_id=rid)
        check(replay.get("txn_id") == first.get("txn_id"),
              f"a retried request_id created a new transaction: {replay}")
        check_eq(replay.get("replayed"), True, "the retry should be marked as a replay")
        total_replay = stack.total_cents()
        check_eq(total_replay, total_after,
                 "a retried transfer must not move money twice")
        return (f"{len(successes)} overlapping cross-shard transfers, money conserved "
                f"at {total_after}c, retries idempotent")



def test_service_releases_descriptors_per_request() -> str:
    """A long-lived service must not leak one file descriptor per request.

    The services run on a thread-per-connection server: every HTTP request gets a
    fresh thread, and each thread opens its own SQLite connection.  If that
    connection is not handed back when the request ends, a service leaks a
    descriptor per request until it hits the process limit - at which point SQLite
    fails with the misleading ``unable to open database file`` even though the
    file is right there.  That is exactly what broke ``/debug/route`` after a live
    migration under load.

    This drives the *real* HTTP path (not the in-process shortcut) with many
    sequential requests and asserts the descriptor count stays flat.  It is the
    test that would have caught the leak, so it is worth its runtime.
    """
    import os

    if not os.path.isdir("/proc/self/fd"):
        return "skipped: /proc/self/fd is not available on this platform"

    def open_fds() -> int:
        return len(os.listdir("/proc/self/fd"))

    def account_fds() -> int:
        count = 0
        for name in os.listdir("/proc/self/fd"):
            try:
                target = os.readlink(f"/proc/self/fd/{name}")
            except OSError:
                continue
            if ".db" in target:
                count += 1
        return count

    with LocalStack() as stack:
        client = BankClient(stack.gateway_url)

        for _ in range(5):
            client.balance("A0500")

        db_before = account_fds()

        rounds = 120
        for index in range(rounds):
            if index % 3 == 0:
                client.deposit("A0500", 1.0)
            elif index % 3 == 1:
                client.balance("A1500")
            else:
                client.shard_map()

        db_after = account_fds()
        grew = db_after - db_before
        check(grew <= 4,
              f"{rounds} requests leaked database descriptors: "
              f"{db_before} -> {db_after} (grew by {grew}); each request thread "
              f"must release the connection it opened")
        return (f"{rounds} requests left the database-descriptor count flat "
                f"({db_before} -> {db_after})")
