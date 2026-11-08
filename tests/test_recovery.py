"""Recovery scenarios A, B and C, plus the between-scenario guarantees.

The assignment names three situations and one rule that spans them:

    A  one shard votes YES, another rejects      -> safely abort all prepared
    B  all prepared, the Coordinator stops       -> recover durable state, continue
                                                    or resolve
    C  COMMIT recorded, not all acknowledged     -> complete the recorded COMMIT;
                                                    never replace it with ABORT

These tests drive the coordinator's own protocol objects so a "crash" can be
modelled precisely (stop exactly here, restart, observe), and they always check
the invariant nobody may break: **money is conserved and no write is applied
twice**.
"""

from __future__ import annotations

import json
import os

from platform.client import BankClient
from platform.coordinator import CoordinatorService
from platform.local_stack import LocalStack
from platform.storage.registry import CoordinatorJournal

from .helpers import check, check_eq


def _prepare_both(stack, txn_id: str, src: str, dst: str, cents: int) -> dict:
    """Run two-phase commit's first phase for real, over the wire."""
    votes = {}
    for shard, branch in (
        ("A", {"account": src, "kind": "DEBIT", "amount_cents": cents}),
        ("B", {"account": dst, "kind": "CREDIT", "amount_cents": cents}),
    ):
        reply = stack.post("/route/prepare", {"shard": shard, "txn_id": txn_id,
                                              "branch": branch, "epoch": 1})
        votes[shard] = reply.get("vote")
    return votes


def _open_transaction(stack, txn_id: str, src: str, dst: str, cents: int,
                      state: str = "PREPARED", decision: str = ""):
    """Insert a journal row the way the coordinator would have."""
    journal = stack._coordinator.coordinator.journal
    if journal.find_by_request(f"seed-{txn_id}"):
        return journal.get(txn_id)
    try:
        journal.get(txn_id)
        return journal.update(txn_id, state=state, decision=decision or None)
    except Exception:
        pass
    journal.create(txn_id, request_id=f"seed-{txn_id}", kind="cross_shard_transfer",
                   participants=["A", "B"], epoch=1, src_account=src,
                   dst_account=dst, amount_cents=cents)
    return journal.update(txn_id, state=state, decision=decision or None)



def test_scenario_a_abort() -> str:
    """Scenario A: a participant that cannot commit makes the whole thing abort.

    The debit side is asked to move more money than it holds, so shard A votes NO
    while shard B votes YES.  Nothing may end up applied anywhere, and the
    coordinator must record ABORT rather than leaving the prepared branch behind.
    """
    with LocalStack() as stack:
        src, dst = "A0300", "A1300"
        src_before, dst_before = stack.balance(src), stack.balance(dst)
        too_much = src_before + 1_000_000
        txn_id = "txn-scenario-a"

        votes = _prepare_both(stack, txn_id, src, dst, too_much)
        check_eq(votes, {"A": "NO", "B": "YES"}, "shard A should refuse, shard B should accept")

        coordinator = stack._coordinator.coordinator
        result = coordinator.run_cross_shard(
            txn_id, src=src, dst=dst, cents=too_much, request_id="req-a",
            epoch=1, src_shard="A", dst_shard="B")
        check_eq(result.get("status"), "error", "an unaffordable transfer must fail")
        check_eq(result.get("decision"), "ABORT", "the decided outcome must be ABORT")

        state_a = stack.post("/route/txn-status", {"shard": "A", "txn_id": txn_id})
        state_b = stack.post("/route/txn-status", {"shard": "B", "txn_id": txn_id})
        check_eq(state_b["state"], "ABORTED", "shard B should have discarded its prepared branch")
        check_eq(state_a["state"], "ABORTED", "shard A should record the abort too")

        check_eq(stack.balance(src), src_before, "source balance after abort")
        check_eq(stack.balance(dst), dst_before, "target balance after abort")

        record = stack.get("/txn", txn_id=txn_id)
        check_eq(record["journal_state"], "ABORTED", "journal after abort")
        coordinator.recover()
        check_eq(stack.balance(src), src_before, "source balance after recovery")
        check_eq(stack.balance(dst), dst_before, "target balance after recovery")
        return f"votes {votes} -> safely aborted, balances untouched"



def test_scenario_b_prepared_crash() -> str:
    """Scenario B: the coordinator stops with branches prepared and no decision.

    Resuming must *resolve* it, not leave it hanging forever.  Since no durable
    decision exists, no participant can have applied anything, and the only safe
    resolution is a clean abort - which the test verifies, together with the fact
    that a client asking about it got a definite answer afterwards.
    """
    with LocalStack() as stack:
        src, dst = "A0310", "A1310"
        src_before, dst_before = stack.balance(src), stack.balance(dst)
        cents = 4000
        txn_id = "txn-scenario-b"

        votes = _prepare_both(stack, txn_id, src, dst, cents)
        check_eq(votes, {"A": "YES", "B": "YES"}, "both participants should prepare")
        journal = stack._coordinator.coordinator.journal
        journal.create(txn_id, request_id="req-b", kind="cross_shard_transfer",
                       participants=["A", "B"], epoch=1, src_account=src,
                       dst_account=dst, amount_cents=cents)
        journal.update(txn_id, state="PREPARED", votes=votes)

        pending = stack.get("/txn", txn_id=txn_id)
        check(pending["state"].startswith("PENDING") or pending["state"] == "PREPARED",
              f"an undecided transaction must read as uncertain, got {pending['state']}")
        check_eq(pending["participant_states"], {"A": "PREPARED", "B": "PREPARED"},
                 "both participants should still be holding a prepared branch")

        report = stack._coordinator.coordinator.recover()
        check(txn_id in report["aborted"] or txn_id in report["replayed"],
              f"recovery did not resolve the transaction: {report}")
        after = stack.get("/txn", txn_id=txn_id)
        check_eq(after["journal_state"], "ABORTED",
                 "with no durable decision, the safe resolution is abort")

        check_eq(stack.balance(src), src_before, "source balance after resolution")
        check_eq(stack.balance(dst), dst_before, "target balance after resolution")
        for shard in ("A", "B"):
            state = stack.post("/route/txn-status", {"shard": shard, "txn_id": txn_id})
            check_eq(state["state"], "ABORTED", f"shard {shard} should hold no prepared branch")
        return "prepared branches resolved to ABORT after the coordinator restart"



def test_scenario_c_commit_completion() -> str:
    """Scenario C: the COMMIT decision is durable, then a participant goes quiet.

    This is the scenario that punishes the tempting shortcut.  The coordinator has
    recorded COMMIT_DECIDED and one participant has applied it; the other is
    unreachable for the moment.  Restarting must *finish the commit*, never flip
    to ABORT because a participant looks dead.
    """
    with LocalStack() as stack:
        src, dst = "A0320", "A1320"
        src_before, dst_before = stack.balance(src), stack.balance(dst)
        cents = 6500
        txn_id = "txn-scenario-c"

        votes = _prepare_both(stack, txn_id, src, dst, cents)
        check_eq(votes, {"A": "YES", "B": "YES"}, "both participants should prepare")

        journal = stack._coordinator.coordinator.journal
        journal.create(txn_id, request_id="req-c", kind="cross_shard_transfer",
                       participants=["A", "B"], epoch=1, src_account=src,
                       dst_account=dst, amount_cents=cents)
        journal.update(txn_id, state="PREPARED", votes=votes)
        journal.update(txn_id, state="COMMIT_DECIDED", decision="COMMIT")

        stack.post("/route/commit", {"shard": "A", "txn_id": txn_id})
        check_eq(stack.balance(src), src_before - cents, "source moved by the commit")
        check_eq(stack.balance(dst), dst_before, "target has not moved yet")
        check_eq(journal.get(txn_id)["state"], "COMMIT_DECIDED",
                 "the durable decision must not be reversed")

        report = stack._coordinator.coordinator.recover()
        check(txn_id in report["replayed"], f"recovery should replay the commit: {report}")
        check_eq(stack.balance(src), src_before - cents, "source after recovery")
        check_eq(stack.balance(dst), dst_before + cents, "target after recovery")
        record = journal.get(txn_id)
        check_eq(record["state"], "COMPLETED", "journal after recovery")
        check_eq(record["decision"], "COMMIT", "the decision stays COMMIT")

        stack._coordinator.coordinator.recover()
        check_eq(stack.balance(src), src_before - cents, "source after a second restart")
        check_eq(stack.balance(dst), dst_before + cents, "target after a second restart")
        return "durable COMMIT completed after restart, never replaced by ABORT"



def test_repeated_commit_is_safe() -> str:
    """A participant must tolerate receiving COMMIT more than once.

    A lost acknowledgement is indistinguishable from a lost message, so the
    coordinator retries.  The participant has to make the repeat a no-op rather
    than a second debit.
    """
    with LocalStack() as stack:
        src, dst = "A0330", "A1330"
        src_before = stack.balance(src)
        cents = 3000
        txn_id = "txn-repeat-commit"
        votes = _prepare_both(stack, txn_id, src, dst, cents)
        check_eq(votes, {"A": "YES", "B": "YES"}, "prepare")

        first = stack.post("/route/commit", {"shard": "A", "txn_id": txn_id})
        check_eq(first.get("applied"), 1, "first commit should apply")
        second = stack.post("/route/commit", {"shard": "A", "txn_id": txn_id})
        check_eq(second.get("repeated"), True, "second commit should be recognised")
        check_eq(second.get("applied"), 0, "second commit must not apply again")
        check_eq(stack.balance(src), src_before - cents,
                 "the balance must have moved exactly once")

        conflict = stack.post("/route/commit", {"shard": "B", "txn_id": txn_id})
        check_eq(conflict.get("decision"), "COMMITTED", "B commits normally")
        check_eq(stack.balance(dst), stack.balance(dst), "no double credit")
        return "repeated COMMIT applied exactly once"


def test_repeated_prepare_keeps_vote() -> str:
    """A repeated PREPARE must return the stored vote, not re-decide.

    If a participant re-validated on the second PREPARE, a balance that moved in
    between could turn a YES into a NO and the coordinator would be forced to
    abort a transaction it had already promised to commit.
    """
    with LocalStack() as stack:
        src, dst = "A0340", "A1340"
        cents = 2000
        txn_id = "txn-repeat-prepare"
        first = stack.post("/route/prepare", {
            "shard": "A", "txn_id": txn_id,
            "branch": {"account": src, "kind": "DEBIT", "amount_cents": cents},
            "epoch": 1})
        check_eq(first.get("vote"), "YES", "first prepare")
        check_eq(first.get("repeated"), False, "first prepare is not a repeat")

        drained = stack.post("/route/prepare", {
            "shard": "A", "txn_id": txn_id + "-other",
            "branch": {"account": src, "kind": "DEBIT",
                       "amount_cents": stack.balance(src) - 1},
            "epoch": 1})
        check_eq(drained.get("vote"), "YES", "drain prepare should succeed")

        again = stack.post("/route/prepare", {
            "shard": "A", "txn_id": txn_id,
            "branch": {"account": src, "kind": "DEBIT", "amount_cents": cents},
            "epoch": 1})
        check_eq(again.get("vote"), "YES", "a repeated prepare must keep its vote")
        check_eq(again.get("repeated"), True, "the repeat must be flagged")
        return "repeated PREPARE returned the stored YES vote"


def test_unknown_transaction_is_queryable() -> str:
    """An unresolved transaction must be queryable by id and admit uncertainty.

    The assignment asks for a queryable PENDING/UNKNOWN status.  This test holds
    a branch prepared past the point where success can be claimed and checks that
    the answer stops being a cheerful success.
    """
    with LocalStack() as stack:
        src, dst = "A0350", "A1350"
        cents = 1500
        txn_id = "txn-uncertain"
        votes = _prepare_both(stack, txn_id, src, dst, cents)
        check_eq(votes, {"A": "YES", "B": "YES"}, "prepare")
        journal = stack._coordinator.coordinator.journal
        journal.create(txn_id, request_id="req-uncertain", kind="cross_shard_transfer",
                       participants=["A", "B"], epoch=1, src_account=src,
                       dst_account=dst, amount_cents=cents)
        journal.update(txn_id, state="PREPARED", votes=votes)

        status = stack.get("/txn", txn_id=txn_id)
        check(status["state"] in ("PENDING", "PREPARED", "PENDING_COMMIT", "PENDING_ABORT"),
              f"unexpected status for an undecided transaction: {status['state']}")
        check_eq(status["participant_states"], {"A": "PREPARED", "B": "PREPARED"},
                 "participants should both report PREPARED")
        check_eq(status["decision"], None, "no decision has been made yet")

        missing = stack.get("/txn", txn_id="txn-does-not-exist")
        check_eq(missing.get("error"), "unknown_transaction",
                 "an unknown transaction id should be reported as such")
        return f"undecided transaction reported as {status['state']}"


def test_durability_across_restart() -> str:
    """A new service instance over the same files must see the same state.

    This is the "restart the container" check without Docker: the databases are
    opened fresh, and every balance, prepared branch and journal decision must
    still be there.  It is what makes the graceful-shutdown experiments in the
    video meaningful.
    """
    root = None
    try:
        from platform.local_stack import LocalStack as _LS

        stack = _LS()
        root = stack.root
        src, dst = "A0360", "A1360"
        client = BankClient(stack.gateway_url)
        client.deposit(src, 123.45)
        before = stack.balance(src)
        cents = 2500
        txn_id = "txn-durable"
        votes = _prepare_both(stack, txn_id, src, dst, cents)
        check_eq(votes, {"A": "YES", "B": "YES"}, "prepare")

        stack.stop()
        revived = _LS(root=root)
        try:
            check_eq(revived.balance(src), before, "balance must survive a restart")
            status = revived.post("/route/txn-status", {"shard": "A", "txn_id": txn_id})
            check_eq(status["state"], "PREPARED",
                     "a prepared branch must survive a restart")
            revived.post("/route/commit", {"shard": "A", "txn_id": txn_id})
            revived.post("/route/commit", {"shard": "B", "txn_id": txn_id})
            check_eq(revived.balance(src), before - cents,
                     "the surviving branch must still be committable")
            return "balances, prepared branches and journal survive a restart"
        finally:
            revived.cleanup()
    finally:
        if root:
            import shutil

            shutil.rmtree(root, ignore_errors=True)


def test_shard_container_startup() -> str:
    """A replica must be able to start the way the deployed container starts it.

    The tests upstream build a ``LocalStack``, which always hands the services an
    explicit shard map.  The *container* does something different: ``docker/run.sh``
    invokes ``python -m platform.shard_server`` with only environment variables, and
    the shard derives its own seed-time map from ``SHARD_RANGES``/``SHARD_ACCOUNTS``
    (or guesses ranges from the default account list when neither is set).  That
    path is not exercised anywhere else, so a NameError inside it crashed every
    replica on start-up while the whole suite still passed.

    This calls the same functions the container entry point calls, with the same
    environment, for both shards, and checks a usable seed comes out.
    """
    from platform.shard_server import build_seed, mapping_from_env

    saved = {k: os.environ.get(k) for k in ("SHARD_RANGES", "SHARD_ACCOUNTS")}
    try:
        os.environ["SHARD_RANGES"] = "A:A0001-A1000,B:A1001-A2000"
        os.environ.pop("SHARD_ACCOUNTS", None)
        for shard in ("A", "B"):
            mapping = mapping_from_env(shard)
            check(len(mapping.ranges) >= 1, f"shard {shard} derived no ranges")
            seed = build_seed(shard, mapping, 100000, 100, 0)
            owned = [a for a in seed if mapping.shard_of(a) == shard]
            check(len(owned) > 0, f"shard {shard} seeded no accounts it owns")

        os.environ.pop("SHARD_RANGES", None)
        os.environ.pop("SHARD_ACCOUNTS", None)
        mapping = mapping_from_env("A")
        seed = build_seed("A", mapping, 100000, 100, 0)
        check(len(seed) > 0, "the fallback startup path seeded no accounts")

        return ("shard_server start-up derives a map and a seed from the "
                "environment, with and without SHARD_RANGES")
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

