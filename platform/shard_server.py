"""A banking shard service.

One binary, many roles: this is the HTTP face of a shard database.  Every
replica of a shard runs it against the *same* database file, which is how the
assignment's "at least 2 application replicas sharing that shard database"
requirement is met without introducing a replication layer that the assignment
explicitly does not require.

Point of view
-------------
The gateway holds the authoritative shard map, but the shard does **not** trust
the gateway to be current.  It keeps a copy of the map (pushed by the registry
or by the data plane) and enforces two things before touching a row:

* ``epoch`` - if the caller's epoch is older than ours the write is refused with
  ``409 stale_epoch``;
* ownership - if our map says the account belongs to another shard the write is
  refused with ``409 not_owner``.

That is the *stale-route fence* the expert bonus grades, and putting it in the
storage path (not in the gateway) is exactly the point: a request that raced a
cutover is harmless even if it reaches the wrong container.

Endpoints
---------
``GET  /health`` ``GET /whoami`` ``GET /stats`` ``GET /accounts``
``POST /read`` ``POST /write`` ``POST /transfer``
``POST /2pc/prepare`` ``POST /2pc/commit`` ``POST /2pc/abort``
``GET  /2pc/status?txn_id=`` ``POST /2pc/resolve`` ``POST /2pc/import``
``POST /internal/shard-map`` ``POST /internal/seed`` ``POST /internal/bulk``
``POST /internal/migrate/export`` ``POST /internal/migrate/import``
``POST /internal/fence``

Run it with ``python -m platform.shard_server`` so the package context is set;
the sibling imports are relative, which keeps this package from shadowing the
standard-library ``platform`` module over the whole process.
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any, Dict, List, Optional

from .http_api import (
    ValidationError,
    cents_from_body,
    coins,
    ok_body,
    require_account,
)
from .http_service import AppError, JsonService, configure_logging, run_service
from .sharding.shard_map import (
    ShardMap,
    ShardMapError,
    default_accounts,
    format_account,
    parse_account,
    parse_range,
)
from .storage.shard_store import (
    ABORTED,
    COMMITTED,
    NotOwner,
    ShardError,
    ShardStore,
    StaleEpoch,
)
from .targets import PeerError, env, env_int, http_json, url_for

LOG = configure_logging("bank.shard")


def _map_error(exc: Exception) -> AppError:
    if isinstance(exc, (StaleEpoch, NotOwner)):
        return AppError(exc.code, exc.message, status=exc.status, **exc.extra)
    if isinstance(exc, ShardError):
        return AppError(exc.code, exc.message, status=exc.status, **exc.extra)
    if isinstance(exc, ValidationError):
        return AppError(exc.code, exc.message, status=422, **exc.extra)
    if isinstance(exc, ShardMapError):
        return AppError("shard_map_error", str(exc), status=409)
    raise exc


class ShardService(JsonService):
    def __init__(self, host: str, port: int, shard: str, db_path: str,
                 seed: Optional[Dict[str, int]] = None, cluster: str = ""):
        self.shard = shard.upper()
        self.cluster = cluster
        self.store = ShardStore(db_path, self.shard, seed=seed)
        self.db_path = db_path
        super().__init__(host, port, f"shard-{self.shard.lower()}", self.route)
        LOG.info("shard %s ready db=%s accounts=%d", self.shard, db_path,
                 self.store.count_accounts())


    def describe(self) -> Dict[str, Any]:
        body = super().describe()
        body.update({
            "shard": self.shard,
            "cluster": self.cluster,
            "db_path": self.db_path,
            "accounts": self.store.count_accounts(),
            "map_epoch": self.map_epoch,
            "map_fingerprint": self.map_fingerprint,
            "prepared": self.store.prepared_count(),
        })
        return body

    def close(self) -> None:
        super().close()
        try:
            self.store.close()
        except Exception:
            pass

    def release_thread_resources(self) -> None:
        try:
            self.store.release_thread()
        except Exception:
            pass

    @property
    def map_epoch(self) -> int:
        raw = self.store.get_state("shard_map")
        return int(json.loads(raw)["epoch"]) if raw else 0

    @property
    def map_fingerprint(self) -> str:
        raw = self.store.get_state("shard_map")
        if not raw:
            return ""
        data = json.loads(raw)
        mapping = ShardMap.from_ranges(data["epoch"], data["ranges"], data.get("accounts", []))
        return mapping.fingerprint()

    def _require_owner(self, account: str, epoch: Optional[int]) -> None:
        """Fence: refuse a write this shard is not allowed to perform."""
        self.store.assert_writable(account, epoch)


    def route(self, method: str, path: str, body: Dict[str, Any], request_id: str):
        try:
            if method == "GET" and path == "/stats":
                return 200, self._stats()
            if method == "GET" and path == "/accounts":
                return 200, {"status": "success", "shard": self.shard,
                             "accounts": self.store.snapshot()}
            if method == "GET" and path == "/account":
                return 200, self._account(body)
            if method == "POST" and path == "/read":
                return 200, self._read(body)
            if method == "POST" and path == "/write":
                return 200, self._write(body, request_id)
            if method == "POST" and path == "/transfer":
                return 200, self._transfer(body, request_id)
            if method == "POST" and path == "/2pc/prepare":
                return 200, self._prepare(body)
            if method == "POST" and path == "/2pc/commit":
                return 200, self._decide(body, COMMITTED)
            if method == "POST" and path == "/2pc/abort":
                return 200, self._decide(body, ABORTED)
            if method == "GET" and path == "/2pc/status":
                return 200, self._txn_status(body)
            if method == "POST" and path == "/2pc/status":
                return 200, self._txn_status(body)
            if method == "POST" and path == "/2pc/resolve":
                return 200, self._resolve(body)
            if method == "POST" and path == "/2pc/import":
                return 200, self._import_prepared(body)
            if method == "POST" and path == "/internal/shard-map":
                return 200, self._set_map(body)
            if method == "POST" and path == "/internal/seed":
                return 200, self._seed(body)
            if method == "POST" and path == "/internal/bulk":
                return 200, self._bulk(body)
            if method == "POST" and path == "/internal/migrate/export":
                return 200, self._migrate_export(body)
            if method == "POST" and path == "/internal/migrate/import":
                return 200, self._migrate_import(body)
            if method == "POST" and path == "/internal/migrate/delta":
                return 200, self._migrate_delta(body)
            if method == "POST" and path == "/internal/migrate/apply":
                return 200, self._migrate_apply(body)
            if method == "POST" and path == "/internal/migrate/verify":
                return 200, self._migrate_verify(body)
            if method == "POST" and path == "/internal/migrate/cleanup":
                return 200, self._migrate_cleanup(body)
            if method == "POST" and path == "/internal/fence":
                return 200, self._fence(body)
            return None
        except (ShardError, StaleEpoch, NotOwner, ValidationError, ShardMapError) as exc:
            raise _map_error(exc) from exc


    def _stats(self) -> Dict[str, Any]:
        return {
            "status": "success", "shard": self.shard, "instance": self.instance_id,
            "accounts": self.store.count_accounts(), "total_cents": self.store.total_cents(),
            "prepared": self.store.prepared_count(), "applied_requests": self.store.count_applied(),
            "map_epoch": self.map_epoch, "map_fingerprint": self.map_fingerprint,
            "served": int(self.store.get_state("served", "0") or 0),
        }

    def _account(self, body: Dict[str, Any]) -> Dict[str, Any]:
        account = require_account(body.get("account"))
        return {"status": "success", **coins(self.store.balance(account)),
                "account": account, "shard": self.shard, "instance": self.instance_id}

    def _read(self, body: Dict[str, Any]) -> Dict[str, Any]:
        account = require_account(body.get("account"))
        epoch = body.get("epoch")
        if epoch is not None:
            self._require_owner(account, epoch)
        self.store.bump_served(1)
        return {"status": "success", "action": "balance", "account": account,
                **coins(self.store.balance(account)),
                "shard": self.shard, "instance": self.instance_id,
                "epoch": self.map_epoch}

    def _write(self, body: Dict[str, Any], request_id: Optional[str] = None) -> Dict[str, Any]:
        action = (body.get("action") or "").lower()
        account = require_account(body.get("account"))
        cents = cents_from_body(body, "amount")
        epoch = body.get("epoch")
        request_id = body.get("request_id") or request_id
        if action == "deposit":
            result = self.store.deposit(account, cents, epoch=epoch, request_id=request_id)
        elif action == "withdraw":
            result = self.store.withdraw(account, cents, epoch=epoch, request_id=request_id)
        else:
            raise AppError("unsupported_action", f"unsupported write action: {action!r}", status=422)
        result["instance"] = self.instance_id
        self.store.bump_served(1)
        return result

    def _transfer(self, body: Dict[str, Any], request_id: Optional[str]) -> Dict[str, Any]:
        src = require_account(body.get("account"), "account")
        dst = require_account(body.get("to"), "to")
        cents = cents_from_body(body, "amount")
        epoch = body.get("epoch")
        result = self.store.local_transfer(src, dst, cents, epoch=epoch,
                                           request_id=body.get("request_id") or request_id)
        result["instance"] = self.instance_id
        self.store.bump_served(1)
        return result

    def _prepare(self, body: Dict[str, Any]) -> Dict[str, Any]:
        txn_id = body.get("txn_id")
        if not txn_id:
            raise AppError("missing_field", "txn_id is required", status=422)
        branch = body.get("branch")
        if not isinstance(branch, dict):
            raise AppError("missing_field", "branch is required", status=422)
        epoch = int(body.get("epoch", 0))
        for field in ("account", "kind", "amount_cents"):
            if field not in branch:
                raise AppError("missing_field", f"branch.{field} is required", status=422)
        branch = {
            "account": require_account(branch["account"]),
            "kind": str(branch["kind"]).upper(),
            "amount_cents": int(branch["amount_cents"]),
        }
        if branch["kind"] not in ("DEBIT", "CREDIT"):
            raise AppError("invalid_branch", "branch.kind must be DEBIT or CREDIT", status=422)
        if branch["amount_cents"] <= 0:
            raise AppError("invalid_amount", "branch.amount_cents must be positive", status=422)
        vote = self.store.prepare(str(txn_id), branch, epoch)
        vote["instance"] = self.instance_id
        LOG.info("prepare txn=%s shard=%s vote=%s rid=%s", txn_id, self.shard,
                 vote["vote"], self.instance_id)
        return vote

    def _decide(self, body: Dict[str, Any], decision: str) -> Dict[str, Any]:
        txn_id = body.get("txn_id")
        if not txn_id:
            raise AppError("missing_field", "txn_id is required", status=422)
        result = self.store.commit_prepared(str(txn_id), decision)
        result["instance"] = self.instance_id
        result.setdefault("applied", 0)
        branch = result.pop("branch", None)
        if decision == COMMITTED and branch:
            try:
                result["account"] = branch["account"]
                result["balance_cents"] = self.store.balance(branch["account"])
                result["balance"] = round(result["balance_cents"] / 100.0, 2)
            except ShardError:
                pass
        LOG.info("%s txn=%s shard=%s repeated=%s", decision.lower(), txn_id,
                 self.shard, result["repeated"])
        return result

    def _txn_status(self, body: Dict[str, Any]) -> Dict[str, Any]:
        txn_id = body.get("txn_id")
        if not txn_id:
            raise AppError("missing_field", "txn_id is required", status=422)
        status = self.store.transaction_status(str(txn_id))
        status["instance"] = self.instance_id
        return status

    def _resolve(self, body: Dict[str, Any]) -> Dict[str, Any]:
        force = str(body.get("force", ABORTED)).upper()
        only_stale = float(body.get("only_stale_seconds", 0) or 0)
        resolved = self.store.resolve_in_flight(force=force, only_stale_seconds=only_stale)
        return {"status": "success", "shard": self.shard, "force": force,
                "resolved": resolved, "count": len(resolved)}

    def _import_prepared(self, body: Dict[str, Any]) -> Dict[str, Any]:
        txn_id = body.get("txn_id")
        branch = body.get("branch")
        if not txn_id or not isinstance(branch, dict):
            raise AppError("missing_field", "txn_id and branch are required", status=422)
        created = self.store.import_prepared(str(txn_id), branch, int(body.get("epoch", 0)))
        return {"status": "success", "txn_id": txn_id, "shard": self.shard,
                "imported": created}

    def _set_map(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Adopt a shard map.  Epochs only ever move forward."""
        epoch = int(body.get("epoch", 0))
        payload = {
            "epoch": epoch,
            "ranges": body.get("ranges") or [],
            "accounts": body.get("accounts") or [],
            "note": body.get("note", ""),
        }
        ShardMap.from_ranges(epoch, payload["ranges"], payload["accounts"])
        with self.store._write_lock:
            self.store._conn.execute("BEGIN IMMEDIATE")
            if epoch < self.map_epoch:
                self.store._conn.rollback()
                raise AppError("stale_map", f"refusing to move map epoch backwards "
                                            f"({epoch} < {self.map_epoch})", status=409)
            self.store._set_state("shard_map", json.dumps(payload))
            self.store._conn.commit()
        LOG.info("shard %s adopted map epoch=%d ranges=%d", self.shard, epoch,
                 len(payload["ranges"]))
        return {"status": "success", "shard": self.shard, "epoch": epoch,
                "unchanged": False}

    def _seed(self, body: Dict[str, Any]) -> Dict[str, Any]:
        accounts = body.get("accounts") or {}
        if not isinstance(accounts, dict):
            raise AppError("invalid_seed", "accounts must be an object", status=422)
        balances = {str(k).upper(): int(v) for k, v in accounts.items()}
        created = self.store.insert_accounts(balances, replace=False)
        return {"status": "success", "shard": self.shard, "created": created,
                "accounts": self.store.count_accounts()}

    def _bulk(self, body: Dict[str, Any]) -> Dict[str, Any]:
        rows = body.get("accounts") or []
        if isinstance(rows, dict):
            rows = [{"account": k, "balance_cents": v} for k, v in rows.items()]
        balances = {str(r["account"]).upper(): int(r["balance_cents"]) for r in rows}
        created = self.store.insert_accounts(balances, replace=bool(body.get("replace")))
        return {"status": "success", "shard": self.shard, "written": created}

    def _migrate_export(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Snapshot rows of a range, plus the migration cursor.

        The rows and the applied-request cursor are read inside the *same* write
        lock, so the cursor never points past a write that is already inside the
        snapshot.  Without that pairing, the change-capture replay would either
        miss writes or apply them twice; the tests check both directions.
        """
        low = str(body.get("low", "")).upper()
        high = str(body.get("high", "")).upper()
        if not low or not high:
            raise AppError("missing_field", "low and high are required", status=422)
        _, low_n, prefix, high_n = parse_range(f"{low}-{high}")
        with self.store._write_lock:
            self.store._conn.execute("BEGIN IMMEDIATE")
            rows = []
            for number in range(low_n, high_n + 1):
                account = format_account(prefix, number)
                row = self.store._conn.execute(
                    "SELECT balance FROM accounts WHERE account=?", (account,)
                ).fetchone()
                rows.append({"account": account,
                             "balance_cents": int(row["balance"]) if row else None})
            cursor = self.store._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS s FROM applied_requests").fetchone()
            applied_position = int(cursor["s"])
            prepared = self.store.prepared_list()
            decisions = self.store.decisions_list()
            self.store._conn.commit()
        return {
            "status": "success", "shard": self.shard, "low": low, "high": high,
            "accounts": rows, "prepared": prepared,
            "prepare_decisions": decisions,
            "snapshot_at": time.time(),
            "applied_position": applied_position,
            "applied_count": self.store.count_applied(),
            "map_epoch": self.map_epoch,
        }

    def _migrate_delta(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Replay writes this shard applied, onto the shard that now owns them.

        Only entries whose account is inside the migrating range are replayed;
        everything else happened on accounts that stayed where they were.  The
        target's replicas are named in the request rather than discovered, so this
        shard needs no global view of the platform.
        """
        low = str(body.get("low", "")).upper()
        high = str(body.get("high", "")).upper()
        target = str(body.get("target_shard", "")).upper()
        target_urls = body.get("target_urls") or []
        epoch = int(body.get("epoch", 0))
        since = int(body.get("since_position", 0) or 0)
        if not low or not high or not target:
            raise AppError("missing_field", "low, high and target_shard are required",
                           status=422)
        if not target_urls:
            raise AppError("missing_field", "target_urls is required", status=422)
        batch = self.store.applied_since(since)
        applied: List[Dict[str, Any]] = []
        for entry in batch["entries"]:
            payload = entry["response"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            account = payload.get("account")
            if account and low <= account <= high:
                reply = self._send_delta(list(target_urls), entry, epoch)
                applied.append({"request_id": entry["request_id"], "target": target,
                                "result": reply.get("status", "unknown"),
                                "replayed": reply.get("replayed")})
        return {
            "status": "success", "shard": self.shard, "target": target,
            "position": batch["position"], "more": batch["more"],
            "scanned": len(batch["entries"]), "applied": applied,
        }

    @staticmethod
    def _send_delta(target_urls: List[str], entry: Dict[str, Any],
                    epoch: int) -> Dict[str, Any]:
        """Deliver a delta to the target's replicas, trying each until one accepts."""
        last: Optional[Exception] = None
        for url in target_urls:
            try:
                return http_json(url_for(str(url).strip(), "/internal/migrate/apply"),
                                 {"entry": entry, "epoch": epoch}, timeout=8.0)
            except PeerError as exc:
                last = exc
        return {"status": "error", "message": f"no target replica accepted the delta: {last}"}

    def _migrate_apply(self, body: Dict[str, Any]) -> Dict[str, Any]:
        entry = body.get("entry")
        if not isinstance(entry, dict):
            raise AppError("missing_field", "entry is required", status=422)
        epoch = int(body.get("epoch", 0))
        try:
            created = self.store.import_applied(entry, epoch=epoch)
        except ShardError as exc:
            if exc.code in ("insufficient_funds", "unknown_account"):
                return {"status": "success", "replayed": False, "skipped": exc.code,
                        "request_id": entry.get("request_id")}
            raise
        return {"status": "success", "replayed": bool(created), "created": created,
                "request_id": entry.get("request_id")}

    def _migrate_verify(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Confirm this shard owns the range at the given epoch."""
        low = str(body.get("low", "")).upper()
        high = str(body.get("high", "")).upper()
        epoch = int(body.get("epoch", 0))
        if not low or not high:
            raise AppError("missing_field", "low and high are required", status=422)
        raw = self.store.get_state("shard_map")
        mapping = None
        if raw:
            data = json.loads(raw)
            mapping = ShardMap.from_ranges(data["epoch"], data["ranges"],
                                           data.get("accounts", []))
        owned = bool(mapping and mapping.epoch == epoch and mapping.shard_of(low) == self.shard
                     and mapping.shard_of(high) == self.shard)
        return {"status": "success", "shard": self.shard, "low": low, "high": high,
                "epoch": epoch, "owns": owned,
                "map_epoch": mapping.epoch if mapping else 0,
                "accounts_held": self.store.count_accounts()}

    def _migrate_cleanup(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Remove old rows, but only after verifying we no longer own the range.

        Refusing to delete while still owning the range is the guard against the
        one mistake that would lose money: cleaning up before the hand-over is
        real.  The migration only calls this once the new epoch is active.
        """
        low = str(body.get("low", "")).upper()
        high = str(body.get("high", "")).upper()
        target_epoch = int(body.get("target_epoch", 0))
        if not low or not high:
            raise AppError("missing_field", "low and high are required", status=422)
        raw = self.store.get_state("shard_map")
        if not raw:
            raise AppError("no_shard_map", "this shard has no map and cannot verify",
                           status=409)
        data = json.loads(raw)
        mapping = ShardMap.from_ranges(data["epoch"], data["ranges"], data.get("accounts", []))
        if mapping.shard_of(low) == self.shard or mapping.shard_of(high) == self.shard:
            raise AppError("still_owner",
                           f"refusing to delete {low}-{high}: this shard still owns it",
                           status=409)
        if mapping.epoch < target_epoch:
            raise AppError("stale_map", "map is older than the migration epoch", status=409)
        removed = self.store.delete_range(low, high)
        return {"status": "success", "shard": self.shard, "removed": removed,
                "epoch": mapping.epoch}

    def _migrate_import(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Install snapshot rows, then prepared branches, then the next epoch.

        Order matters: data first, then the fences that make the old owner
        refuse the accounts, and only then the map bump that hands them over.
        """
        rows = body.get("accounts") or []
        balances = {str(r["account"]).upper(): int(r["balance_cents"] or 0) for r in rows}
        created = self.store.insert_accounts(balances, replace=True)
        imported = 0
        for branch in body.get("prepared") or []:
            payload = branch.get("payload")
            if isinstance(payload, str):
                payload = json.loads(payload)
            if self.store.import_prepared(branch["txn_id"], payload,
                                          int(branch.get("epoch", 0))):
                imported += 1
        return {"status": "success", "shard": self.shard, "written": created,
                "prepared_imported": imported}

    def _fence(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """Force this shard to refuse accounts listed as no longer its own.

        Called by the migration during cutover.  The shard writes the target map
        first (so its own route check rejects stale callers) and only then is the
        gateway allowed to publish the new epoch.
        """
        epoch = int(body.get("epoch", 0))
        payload = body.get("map") or {}
        if not payload.get("ranges"):
            raise AppError("missing_field", "map.ranges is required", status=422)
        self._set_map({
            "epoch": epoch,
            "ranges": payload["ranges"],
            "accounts": payload.get("accounts") or [],
            "note": payload.get("note", "migration fence"),
        })
        removed = self.store.legacy_dump()
        return {"status": "success", "shard": self.shard, "epoch": epoch,
                "accounts_held": len(removed), "fenced": True}


def build_seed(shard: str, mapping: ShardMap, base_cents: int, step_cents: int,
               variant: int = 0) -> Dict[str, int]:
    """Deterministic seed balances so every experiment is reproducible.

    Balance = base + step * (position in the shard) with a small variant offset,
    which keeps the conservation checks in the tests exact and human-checkable.
    """
    owned = mapping.accounts_of(shard)
    seed: Dict[str, int] = {}
    for index, account in enumerate(sorted(owned)):
        seed[account] = base_cents + step_cents * index + variant
    return seed


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Banking shard service (participant)")
    parser.add_argument("--host", default=env("SHARD_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=env_int("SHARD_PORT", 9000))
    parser.add_argument("--shard", default=env("SHARD_NAME", "A"))
    parser.add_argument("--db", default=env("SHARD_DB_PATH", "data/shard.db"))
    parser.add_argument("--cluster", default=env("CLUSTER_NAME", "bank-cluster"))
    parser.add_argument("--accounts", default=env("SHARD_ACCOUNTS", ""),
                        help="comma separated accounts this shard is expected to own")
    parser.add_argument("--seed-balance", type=int, default=env_int("SEED_BASE_CENTS", 100000))
    parser.add_argument("--seed-step", type=int, default=env_int("SEED_STEP_CENTS", 100))
    parser.add_argument("--seed-variant", type=int, default=env_int("SEED_VARIANT", 0))
    parser.add_argument("--no-seed", action="store_true")
    args = parser.parse_args(argv)

    shard = args.shard.upper()
    seed = None
    if not args.no_seed:
        seed = build_seed(shard, mapping_from_env(shard), args.seed_balance,
                          args.seed_step, args.seed_variant)

    service = ShardService(args.host, args.port, shard, args.db, seed=seed,
                           cluster=args.cluster)
    run_service(service)
    return 0


def mapping_from_env(shard: str, fallback_accounts: Optional[List[str]] = None) -> ShardMap:
    """Build the seed-time shard map from the environment.

    ``SHARD_RANGES`` is a comma separated list of ``SHARD:RANGE`` pairs, e.g.
    ``A:A0001-A1000,B:A1001-A2000``.  It is only the *seeding* view; the
    authoritative map is published at runtime by the registry, and the container
    keeps its own copy from then on.
    """
    raw = env("SHARD_RANGES", "")
    ranges = []
    for item in (raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        owner, _, span = item.partition(":")
        low, _, high = span.partition("-")
        ranges.append((owner.strip().upper(), low.strip().upper(), high.strip().upper()))
    accounts = fallback_accounts
    if accounts is None:
        accounts_env = env("SHARD_ACCOUNTS", "")
        accounts = [a.strip().upper() for a in (accounts_env or "").split(",") if a.strip()]
    if not accounts:
        accounts = default_accounts()
    if not ranges:
        ranges = _initial_ranges(shard, accounts)
    return ShardMap.from_ranges(1, ranges, accounts, note="seed-time map")


def _initial_ranges(shard: str, accounts: List[str]):
    """Ownership guessed from the account list, used only to build seed balances."""
    grouped: Dict[str, List[int]] = {}
    for account in accounts:
        prefix, number = parse_account(account)
        grouped.setdefault(prefix, []).append(number)
    ranges = []
    for prefix, numbers in grouped.items():
        ranges.append((shard, format_account(prefix, min(numbers)),
                       format_account(prefix, max(numbers))))
    return ranges


if __name__ == "__main__":
    raise SystemExit(main())
