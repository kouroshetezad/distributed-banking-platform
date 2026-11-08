"""The two-phase-commit coordinator.

Responsibilities, straight from the assignment:

1. assign a unique transaction id;
2. run prepare/commit;
3. **durably record the decision before it is sent**;
4. recover unresolved transactions after a restart.

The happy path
--------------
``POST /txn`` with a cross-shard transfer::

    INITIATED -> PREPARING -> PREPARED -> COMMIT_DECIDED -> COMPLETED
                                     \\-> ABORT_DECIDED -> ABORTED

Only after every participant acknowledges COMMIT does the client see a success,
and the two-phase sequence is not faked with two sequential updates: the debit
and the credit are held as *prepared branches* on their own shards and are only
applied when the coordinator says COMMIT.  A participant that never hears the
decision finds it in its own ``prepared_decisions`` table on restart.

Why the decision is written first
---------------------------------
Scenario C is the one that tempts people into a bug.  If the coordinator commits
one shard, crashes, restarts and then sees a participant that is *down*, the
tempting move is "the other side never committed, so abort".  That would lose or
duplicate money.  Instead:

* the state ``COMMIT_DECIDED`` is written to the journal **before** any COMMIT
  message leaves;
* recovery reads that state and only ever replays COMMIT (never flips it);
* a participant that is unreachable is retried forever, and the transaction is
  reported to clients as PENDING rather than as a failure.

Scenario A (one shard votes NO) and scenario B (coordinator dies while prepared)
fall out of the same machinery; the tests in ``tests/test_recovery.py`` exercise
all three.

The coordinator is deliberately stateless about *which* replica serves a shard:
it is given a participant URL at prepare time, but every later message goes
through the gateway's ``/route`` so a replica that died mid-transaction does not
strand the decision.
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

from .http_api import (
    ValidationError,
    cents_from_body,
    require_account,
)
from .http_service import AppError, JsonService, configure_logging, run_service
from .sharding.shard_map import ShardMap, ShardMapError, parse_account
from .storage.registry import CoordinatorJournal
from .targets import PeerError, env, env_float, env_int, http_json, url_for

LOG = configure_logging("bank.coordinator")

UNKNOWN_AFTER_SECONDS = env_float("PENDING_TIMEOUT_SECONDS", 10.0)


class ParticipantError(Exception):
    def __init__(self, message: str, *, unreachable: bool = False, code: str = "",
                 body: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.unreachable = unreachable
        self.code = code
        self.body = body or {}


class GatewayClient:
    """Talks to shards the way a request would: through the gateway's router.

    Routing every follow-up message through the gateway (instead of remembering
    a container address) is what makes coordinator recovery independent of
    replica failures: ``/route`` re-resolves the shard and picks a live replica.
    """

    def __init__(self, gateway_url: str, timeout: float = 8.0):
        self.gateway_url = gateway_url.rstrip("/")
        self.timeout = timeout

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = url_for(self.gateway_url, path)
        try:
            body = http_json(url, payload, timeout=self.timeout)
        except PeerError as exc:
            raise ParticipantError(str(exc), unreachable=exc.unreachable) from exc
        if body.get("_http_status", 200) >= 400 or body.get("status") == "error":
            raise ParticipantError(body.get("message", "participant error"),
                                   code=body.get("error", ""), body=body)
        return body

    def prepare(self, shard: str, txn_id: str, branch: Dict[str, Any],
                epoch: int) -> Dict[str, Any]:
        return self._post("/route/prepare", {
            "shard": shard, "txn_id": txn_id, "branch": branch, "epoch": epoch,
        })

    def commit(self, shard: str, txn_id: str) -> Dict[str, Any]:
        return self._post("/route/commit", {"shard": shard, "txn_id": txn_id})

    def abort(self, shard: str, txn_id: str) -> Dict[str, Any]:
        return self._post("/route/abort", {"shard": shard, "txn_id": txn_id})

    def status(self, shard: str, txn_id: str) -> Dict[str, Any]:
        return self._post("/route/txn-status", {"shard": shard, "txn_id": txn_id})


class Coordinator:
    """The protocol itself.  The HTTP server is a thin wrapper around this."""

    def __init__(self, gateway_url: str, journal: CoordinatorJournal,
                 shard_maps: Optional[Dict[str, Any]] = None,
                 timeout: float = 8.0):
        self.gateway = GatewayClient(gateway_url, timeout=timeout)
        self.journal = journal
        self.timeout = timeout
        self._cached_map: Optional[Dict[str, Any]] = shard_maps or {}


    def fetch_map(self) -> Dict[str, Any]:
        try:
            body = http_json(url_for(self.gateway.gateway_url, "/shard-map"),
                             method="GET", timeout=self.timeout)
            epoch = int(body.get("epoch", 0))
            mapping = ShardMap.from_ranges(epoch, body.get("ranges", []),
                                           body.get("accounts", []))
            self._cached_map = {"epoch": epoch, "map": mapping,
                               "fingerprint": body.get("fingerprint", "")}
            return self._cached_map
        except (PeerError, ShardMapError) as exc:
            if self._cached_map:
                LOG.warning("using cached shard map (%s)", exc)
                return self._cached_map
            raise AppError("shard_map_unavailable",
                           f"cannot determine shard ownership: {exc}", status=503) from exc


    def transfer(self, body: Dict[str, Any]) -> Dict[str, Any]:
        src = require_account(body.get("account"), "account")
        dst = require_account(body.get("to"), "to")
        cents = cents_from_body(body, "amount")
        request_id = body.get("request_id") or ""
        if src == dst:
            raise AppError("invalid_transfer", "source and destination must differ", status=422)

        mapping = self.fetch_map()
        shard_map: ShardMap = mapping["map"]
        src_shard = shard_map.shard_of(src)
        dst_shard = shard_map.shard_of(dst)

        if src_shard == dst_shard:
            return {
                "status": "success", "coordinated": False, "mode": "same_shard",
                "shard": src_shard, "epoch": shard_map.epoch,
                "message": "both accounts live on one shard; no 2PC required",
                "hint": "POST /transfer through the gateway to execute it atomically",
            }

        if request_id:
            existing = self.journal.find_by_request(request_id, kind="cross_shard_transfer")
            if existing is not None:
                if existing["state"] == "COMPLETED":
                    result = dict(existing["result"] or {})
                    result["replayed"] = True
                    result["txn_id"] = existing["txn_id"]
                    return result
                return {
                    "status": "pending" if existing["state"] != "ABORTED" else "error",
                    "txn_id": existing["txn_id"], "state": existing["state"],
                    "decision": existing["decision"] or None, "replayed": True,
                    "message": "a transaction for this request_id already exists",
                }

        txn_id = body.get("txn_id") or f"txn-{uuid.uuid4().hex[:16]}"
        return self.run_cross_shard(txn_id, src=src, dst=dst, cents=cents,
                                    request_id=request_id, epoch=shard_map.epoch,
                                    src_shard=src_shard, dst_shard=dst_shard)

    def run_cross_shard(self, txn_id: str, *, src: str, dst: str, cents: int,
                        request_id: str, epoch: int, src_shard: str, dst_shard: str,
                        resume: bool = False) -> Dict[str, Any]:
        """The actual protocol.  ``resume=True`` replays a decision after a crash."""
        if not resume:
            self.journal.create(txn_id, request_id=request_id, kind="cross_shard_transfer",
                                participants=[src_shard, dst_shard], epoch=epoch,
                                src_account=src, dst_account=dst, amount_cents=cents)
            self.journal.update(txn_id, state="PREPARING")

        branches = {
            src_shard: {"account": src, "kind": "DEBIT", "amount_cents": cents},
            dst_shard: {"account": dst, "kind": "CREDIT", "amount_cents": cents},
        }
        participants = [src_shard, dst_shard]

        votes: Dict[str, str] = {}
        reasons: Dict[str, Any] = {}
        self.journal.bump_attempts(txn_id)
        for shard in participants:
            try:
                reply = self.gateway.prepare(shard, txn_id, branches[shard], epoch)
                vote = reply.get("vote", "NO")
                votes[shard] = vote
                if vote != "YES":
                    reasons[shard] = reply.get("reason") or reply.get("code") or "voted NO"
            except ParticipantError as exc:
                votes[shard] = "NO"
                reasons[shard] = f"unreachable: {exc.message}"
        self.journal.update(txn_id, votes=votes)
        self.journal.event(txn_id, "phase1_complete", json.dumps(votes))

        if all(v == "YES" for v in votes.values()):
            self.journal.update(txn_id, state="PREPARED")
        else:
            self.journal.event(txn_id, "phase1_reject", json.dumps(reasons))
            return self._abort(txn_id, votes, reasons, epoch)

        self.journal.update(txn_id, state="COMMIT_DECIDED", decision="COMMIT")
        self.journal.event(txn_id, "commit_decided", "durable decision written")

        return self._finish_commit(txn_id, src=src, dst=dst, cents=cents,
                                   src_shard=src_shard, dst_shard=dst_shard,
                                   epoch=epoch, votes=votes)

    def _finish_commit(self, txn_id: str, *, src: str, dst: str, cents: int,
                       src_shard: str, dst_shard: str, epoch: int,
                       votes: Dict[str, str]) -> Dict[str, Any]:
        acks: Dict[str, Any] = {}
        for shard in (src_shard, dst_shard):
            acks[shard] = self._commit_one(shard, txn_id)

        missing = [s for s, a in acks.items() if not a.get("ok")]
        if missing:
            self.journal.update(txn_id, state="COMMIT_DECIDED",
                                error=f"awaiting ack from {','.join(missing)}")
            return {
                "status": "pending", "txn_id": txn_id, "state": "PENDING",
                "decision": "COMMIT", "awaiting": missing,
                "message": "the commit decision is durable and will be completed by recovery",
                "acks": acks,
            }

        result = {
            "status": "success", "action": "transfer", "mode": "cross_shard",
            "coordinated": True, "txn_id": txn_id, "epoch": epoch,
            "request_id": self.journal.get(txn_id)["request_id"] or None,
            "account": src, "to": dst,
            "amount_cents": cents, "amount": round(cents / 100.0, 2),
            "source_shard": src_shard, "target_shard": dst_shard,
            "votes": votes,
            "source_balance_cents": acks[src_shard].get("balance_cents"),
            "source_balance": acks[src_shard].get("balance"),
            "target_balance_cents": acks[dst_shard].get("balance_cents"),
            "target_balance": acks[dst_shard].get("balance"),
        }
        self.journal.update(txn_id, state="COMPLETED", result=result)
        self.journal.event(txn_id, "completed", "all participants acknowledged COMMIT")
        return result

    def _commit_one(self, shard: str, txn_id: str) -> Dict[str, Any]:
        try:
            reply = self.gateway.commit(shard, txn_id)
            reply["ok"] = True
            return reply
        except ParticipantError as exc:
            LOG.warning("commit ack missing from shard %s txn=%s: %s", shard, txn_id, exc)
            return {"ok": False, "shard": shard, "error": exc.message,
                    "unreachable": exc.unreachable}

    def _abort(self, txn_id: str, votes: Dict[str, str], reasons: Dict[str, Any],
               epoch: int) -> Dict[str, Any]:
        self.journal.update(txn_id, state="ABORT_DECIDED", decision="ABORT",
                            votes=votes, error=json.dumps(reasons))
        self.journal.event(txn_id, "abort_decided", json.dumps(reasons))
        for shard in votes:
            if votes.get(shard) == "YES":
                try:
                    self.gateway.abort(shard, txn_id)
                except ParticipantError as exc:
                    LOG.warning("abort ack missing from %s txn=%s: %s", shard, txn_id, exc)
        failed = [s for s, v in votes.items() if v != "YES"]
        reason = reasons.get(failed[0]) if failed else "a participant voted NO"
        body = {
            "status": "error", "error": "transaction_aborted", "txn_id": txn_id,
            "state": "ABORT_DECIDED", "decision": "ABORT", "votes": votes,
            "message": f"transaction aborted safely: {reason}",
        }
        self.journal.update(txn_id, state="ABORTED", result=body)
        return body


    def status(self, txn_id: str) -> Dict[str, Any]:
        txn = self.journal.get(txn_id)
        age = max(0.0, time.time() - float(txn["created_at"]))
        state = txn["state"]
        reported = state
        if state in ("INITIATED", "PREPARING", "PREPARED", "COMMIT_DECIDED",
                     "ABORT_DECIDED") and age > UNKNOWN_AFTER_SECONDS:
            reported = "PENDING" if not txn["decision"] else f"PENDING_{txn['decision']}"
        body = {
            "status": "success", "txn_id": txn_id, "state": reported,
            "journal_state": state, "decision": txn["decision"] or None,
            "votes": txn["votes"], "participants": txn["participants"],
            "kind": txn["kind"], "epoch": txn["epoch"], "attempts": txn["attempts"],
            "age_seconds": round(age, 3), "error": txn["error"] or None,
            "participant_states": self._participant_states(txn),
        }
        if state == "COMPLETED":
            body["result"] = txn["result"]
        return body

    def _participant_states(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        states: Dict[str, Any] = {}
        for shard in txn["participants"]:
            try:
                states[shard] = self.gateway.status(shard, txn["txn_id"]).get("state")
            except ParticipantError as exc:
                states[shard] = f"unreachable: {exc.code or exc.message}"
        return states


    def recover(self) -> Dict[str, Any]:
        """Finish or resolve everything the journal says is unfinished.

        The rule table is small and the assignment's three scenarios map onto
        it one-to-one:

        ==========================  ==========================================
        journal state                action
        ==========================  ==========================================
        ``COMMIT_DECIDED``           replay COMMIT until every participant acks
        ``ABORT_DECIDED``            replay ABORT
        ``PREPARED``                 the decision was lost, no participant has
                                     applied anything, so abort safely
        ``PREPARING`` / ``INITIATED`` abort safely
        ==========================  ==========================================
        """
        report: Dict[str, Any] = {"replayed": [], "aborted": [], "still_pending": []}
        for txn in self.journal.unresolved():
            state = txn["state"]
            txn_id = txn["txn_id"]
            if state in ("COMMIT_DECIDED",):
                self.journal.bump_attempts(txn_id)
                result = self._finish_commit(
                    txn_id, src=txn["src_account"], dst=txn["dst_account"],
                    cents=txn["amount_cents"],
                    src_shard=txn["participants"][0], dst_shard=txn["participants"][1],
                    epoch=txn["epoch"], votes=txn["votes"],
                )
                if result.get("status") == "success":
                    report["replayed"].append(txn_id)
                else:
                    report["still_pending"].append(txn_id)
            elif state == "ABORT_DECIDED":
                for shard in txn["participants"]:
                    try:
                        self.gateway.abort(shard, txn_id)
                    except ParticipantError:
                        pass
                self.journal.update(txn_id, state="ABORTED")
                report["aborted"].append(txn_id)
            else:
                for shard in txn["participants"]:
                    try:
                        self.gateway.abort(shard, txn_id)
                    except ParticipantError:
                        pass
                self.journal.update(
                    txn_id, state="ABORTED", decision="ABORT",
                    error="aborted during recovery: no durable decision existed",
                )
                report["aborted"].append(txn_id)
        report["recovered"] = bool(report["replayed"] or report["aborted"])
        if report["recovered"]:
            self.journal.event("", "recovery_complete", json.dumps(report))
        return report


class CoordinatorService(JsonService):
    def __init__(self, host: str, port: int, gateway_url: str, db_path: str,
                 timeout: float = 8.0):
        self.coordinator = Coordinator(gateway_url, CoordinatorJournal(db_path),
                                       timeout=timeout)
        self.gateway_url = gateway_url
        self.db_path = db_path
        self._recovery_thread = threading.Thread(target=self._recovery_loop,
                                                 name="coordinator-recovery", daemon=True)
        super().__init__(host, port, "coordinator", self.route)

    def describe(self) -> Dict[str, Any]:
        body = super().describe()
        body.update({"gateway": self.gateway_url, "db_path": self.db_path,
                     "journal": self.coordinator.journal.counts()})
        return body

    def close(self) -> None:
        super().close()
        try:
            self.coordinator.journal.close()
        except Exception:
            pass

    def release_thread_resources(self) -> None:
        try:
            self.coordinator.journal.release_thread()
        except Exception:
            pass

    def start_recovery(self) -> None:
        """Run recovery once at start-up, then keep a low-frequency safety net."""
        report = self.coordinator.recover()
        LOG.info("coordinator recovery on start-up: %s", json.dumps(report))
        self._recovery_thread.start()

    def _recovery_loop(self) -> None:
        interval = env_float("RECOVERY_INTERVAL_SECONDS", 5.0)
        while True:
            time.sleep(interval)
            try:
                report = self.coordinator.recover()
                if report["recovered"] or report["still_pending"]:
                    LOG.info("recovery sweep: %s", json.dumps(report))
            except Exception:
                LOG.exception("recovery sweep failed")

    def route(self, method: str, path: str, body: Dict[str, Any], request_id: str):
        try:
            if method == "POST" and path == "/txn":
                return 200, self._txn(body)
            if method == "GET" and path == "/txn":
                return 200, self._status(body)
            if method == "GET" and path == "/txn/list":
                return 200, self._list(body)
            if method == "GET" and path == "/txn/unresolved":
                return 200, {"status": "success",
                             "unresolved": self.coordinator.journal.unresolved()}
            if method == "POST" and path == "/txn/recover":
                return 200, self.coordinator.recover()
            if method == "GET" and path == "/coordinator/stats":
                return 200, self._stats()
            if method == "GET" and path == "/coordinator/events":
                return 200, {"status": "success", "events": self.coordinator.journal.events()}
            return None
        except ValidationError as exc:
            raise AppError(exc.code, exc.message, status=422, **exc.extra) from exc

    def _txn(self, body: Dict[str, Any]) -> Dict[str, Any]:
        return self.coordinator.transfer(body)

    def _status(self, body: Dict[str, Any]) -> Dict[str, Any]:
        txn_id = body.get("txn_id")
        if not txn_id:
            raise AppError("missing_field", "txn_id is required", status=422)
        try:
            return self.coordinator.status(str(txn_id))
        except ValidationError as exc:
            raise AppError(exc.code, exc.message, status=404) from exc

    def _list(self, body: Dict[str, Any]) -> Dict[str, Any]:
        state = body.get("state")
        return {"status": "success", "transactions": self.coordinator.journal.list(state=state)}

    def _stats(self) -> Dict[str, Any]:
        return {"status": "success", "instance": self.instance_id,
                "journal": self.coordinator.journal.counts(),
                "unresolved": len(self.coordinator.journal.unresolved()),
                "gateway": self.gateway_url}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="2PC transaction coordinator")
    parser.add_argument("--host", default=env("COORDINATOR_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=env_int("COORDINATOR_PORT", 9500))
    parser.add_argument("--gateway", default=env("GATEWAY_URL", "http://gateway:8000"))
    parser.add_argument("--db", default=env("COORDINATOR_DB_PATH", "data/coordinator.db"))
    parser.add_argument("--timeout", type=float, default=env_float("PEER_TIMEOUT_SECONDS", 8.0))
    parser.add_argument("--no-recovery", action="store_true",
                        help="do not resume unresolved transactions on start-up")
    args = parser.parse_args(argv)

    service = CoordinatorService(args.host, args.port, args.gateway, args.db,
                                 timeout=args.timeout)
    if not args.no_recovery:
        service.start_recovery()
    run_service(service)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
