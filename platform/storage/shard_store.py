"""Durable state of a single shard.

One database file per shard, shared by all application replicas of that shard.
The replicas hold no authoritative balances themselves - everything below lives
in the database, which is what makes "replicas hold no independent authoritative
balances" literally true: kill every replica and the state is still there.

Concurrency control
-------------------
SQLite in WAL mode gives one writer at a time and lets readers proceed, so the
store leans on that instead of inventing a lock:

* every write runs inside ``BEGIN IMMEDIATE``, which takes the write lock up
  front.  Two replicas writing the same account therefore serialise at the
  database, not at the application, and a lost update is impossible.
* ``busy_timeout`` makes a replica wait its turn instead of failing, which is
  what turns a burst of concurrent withdrawals from "some fail with SQLITE_BUSY"
  into "all of them apply, in some order".
* balances are ``CHECK (balance >= 0)`` plus an explicit read inside the same
  transaction, so an overdraft is refused by the transaction that would have
  caused it.

Assignments still talk about a mutex.  ``BEGIN IMMEDIATE`` gives exactly that
mutex, at the storage layer, shared by *all* replicas of the shard.  The design
notes in ``docs/ARCHITECTURE.md`` explain why that is stronger than a per
process lock and why the report marks it as the concurrency-control mechanism.

Durability
----------
``synchronous=FULL`` + an explicit commit before the reply.  A replica that
answers ``committed`` has already fsynced, so the answer survives its own death.

Two-phase commit support
-----------------------
* a *prepare* runs on the same connection that will later commit, so the
  coordinator is not asking a different process to remember a decision;
* the pending branch itself is stored in the same transaction boundary as the
  balance change, so a prepared transaction that changes nothing else is
  impossible;
* ``prepared_decisions`` keeps COMMIT/ABORT decisions that participants have not
  yet acknowledged.  A participant that was asked to commit but died before
  acknowledging finds the decision on restart and finishes the job, which is the
  requirement "participants must handle repeated COMMIT/ABORT safely" plus
  recovery scenario C;
* ``prepare_decisions`` keeps the last vote per transaction so a repeated
  PREPARE returns the *stored* vote instead of re-running (and possibly
  contradicting) the first one.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

from ..sharding.shard_map import format_account, parse_account, parse_range

PREPARED = "PREPARED"
COMMITTED = "COMMITTED"
ABORTED = "ABORTED"

UNKNOWN_AFTER_SECONDS = 30.0


class ShardError(Exception):
    """A storage-level rejection; the HTTP layer maps the code to a status."""

    def __init__(self, code: str, message: str, *, status: int = 422, **extra: Any):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.extra = extra

    def as_body(self) -> Dict[str, Any]:
        body = {"status": "error", "error": self.code, "message": self.message}
        body.update(self.extra)
        return body


class StaleEpoch(ShardError):
    def __init__(self, message: str, **extra: Any):
        super().__init__("stale_epoch", message, status=409, **extra)


class NotOwner(ShardError):
    def __init__(self, message: str, **extra: Any):
        super().__init__("not_owner", message, status=409, **extra)


class ShardStore:
    """Account store for one shard, safe to open from several replicas."""

    def __init__(self, db_path: str, shard: str, *, seed: Optional[Dict[str, int]] = None):
        self.db_path = db_path
        self.shard = shard.upper()
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".", exist_ok=True)
        self._local = threading.local()
        self._write_lock = threading.RLock()
        self._all_conns: List[sqlite3.Connection] = []
        self._all_conns_lock = threading.Lock()
        self._configure(self._conn)
        self._init_schema(self._conn)
        if seed:
            self.insert_accounts(seed, replace=False)


    @property
    def _conn(self) -> sqlite3.Connection:
        """One SQLite connection per thread.

        Sharing a single connection across handler threads is not safe here: the
        C library serialises individual calls, but a thread that is midway through
        stepping a cursor can still have its statement disturbed by another
        thread's ``execute`` on the same connection, which surfaces as
        ``InterfaceError: bad parameter or other API misuse`` under load.  A
        connection per thread removes the problem entirely, and WAL mode means the
        readers never block the single writer anyway.
        """
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._connect()
            self._configure(conn)
            self._local.conn = conn
            with self._all_conns_lock:
                self._all_conns.append(conn)
        return conn

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0,
                               isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _configure(self, conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA wal_autocheckpoint=100")

    def _init_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                account TEXT PRIMARY KEY,
                balance INTEGER NOT NULL CHECK (balance >= 0)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS replica_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS txn_prepared (
                txn_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                epoch INTEGER NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prepare_decisions (
                txn_id TEXT PRIMARY KEY,
                vote TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prepared_decisions (
                txn_id TEXT PRIMARY KEY,
                decision TEXT NOT NULL,
                decided_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS applied_requests (
                request_id TEXT PRIMARY KEY,
                seq INTEGER,
                kind TEXT NOT NULL,
                response TEXT NOT NULL,
                epoch INTEGER NOT NULL DEFAULT 0,
                amount_cents INTEGER NOT NULL DEFAULT 0,
                applied_at REAL NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_applied_seq ON applied_requests(seq)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fencing (
                shard TEXT PRIMARY KEY,
                epoch INTEGER NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                updated_at REAL NOT NULL
            )
            """
        )
        conn.commit()

    def close(self) -> None:
        with self._all_conns_lock:
            conns, self._all_conns = self._all_conns, []
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass
        self._local = threading.local()

    def release_thread(self) -> None:
        """Close the connection owned by the *current* thread, if any.

        The thread-per-connection server gives every request a new thread, so
        the per-thread connection must be released when that request finishes.
        Otherwise each request leaks one file descriptor until the process hits
        its descriptor limit and SQLite fails with ``unable to open database
        file`` - which is a descriptor ceiling, not a missing file.
        """
        conn = getattr(self._local, "conn", None)
        if conn is None:
            return
        self._local.conn = None
        with self._all_conns_lock:
            try:
                self._all_conns.remove(conn)
            except ValueError:
                pass
        try:
            conn.close()
        except Exception:
            pass



    @staticmethod
    def _now() -> float:
        return time.time()

    def _set_state(self, key: str, value: str, conn: Optional[sqlite3.Connection] = None) -> None:
        conn = conn or self._conn
        conn.execute(
            "INSERT INTO replica_state(key, value, updated_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, value, self._now()),
        )

    def get_state(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self._conn.execute(
            "SELECT value FROM replica_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else default

    def bump_served(self, count: int = 1) -> None:
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT value FROM replica_state WHERE key='served'"
            ).fetchone()
            current = int(row["value"]) if row else 0
            self._set_state("served", str(current + count))
            self._conn.commit()


    def _owner_shard(self, account: str, epoch: int) -> Optional[str]:
        """Ask our own copy of the shard map who owns ``account`` at ``epoch``."""
        from ..sharding.shard_map import ShardMap

        raw = self.get_state("shard_map")
        if not raw:
            return None
        data = json.loads(raw)
        if int(data.get("epoch", -1)) != int(epoch):
            return None
        mapping = ShardMap.from_ranges(data["epoch"], data["ranges"], data.get("accounts", []))
        try:
            return mapping.shard_of(account)
        except Exception:
            return None

    def assert_writable(self, account: str, epoch: Optional[int]) -> str:
        """Reject writes whose routing epoch is stale, or routed to the wrong shard.

        This is the *fence*: the check happens before any row is read or written,
        and it is enforced here, at the storage path, not only in the gateway.
        A request that raced a cutover therefore cannot modify the old owner even
        though it was addressed to it.
        """
        if epoch is None:
            return self.shard
        raw = self.get_state("shard_map")
        if not raw:
            return self.shard
        data = json.loads(raw)
        local_epoch = int(data.get("epoch", 0))
        if int(epoch) < local_epoch:
            raise StaleEpoch(
                f"epoch {epoch} is older than the shard's epoch {local_epoch}",
                epoch=local_epoch, requested_epoch=int(epoch), shard=self.shard,
            )
        owner = self._owner_shard(account, int(epoch))
        if owner is not None and owner != self.shard:
            raise NotOwner(
                f"account {account} is owned by shard {owner} at epoch {epoch}",
                owner=owner, shard=self.shard, requested_epoch=int(epoch),
            )
        return self.shard


    def insert_accounts(self, balances: Dict[str, int], *, replace: bool = False) -> int:
        """Insert seed accounts.  Existing balances are never disturbed."""
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            created = 0
            for account, balance in balances.items():
                account = str(account).upper()
                parse_account(account)
                if replace:
                    self._conn.execute(
                        "INSERT INTO accounts(account, balance) VALUES(?,?) "
                        "ON CONFLICT(account) DO UPDATE SET balance=excluded.balance",
                        (account, int(balance)),
                    )
                    created += 1
                else:
                    cur = self._conn.execute(
                        "INSERT OR IGNORE INTO accounts(account, balance) VALUES(?,?)",
                        (account, int(balance)),
                    )
                    created += cur.rowcount
            self._conn.commit()
        return created

    def count_accounts(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS n FROM accounts").fetchone()
        return int(row["n"])

    def snapshot(self) -> Dict[str, int]:
        rows = self._conn.execute(
            "SELECT account, balance FROM accounts ORDER BY account"
        ).fetchall()
        return {r["account"]: int(r["balance"]) for r in rows}

    def total_cents(self) -> int:
        row = self._conn.execute("SELECT COALESCE(SUM(balance),0) AS s FROM accounts").fetchone()
        return int(row["s"])

    def balance(self, account: str) -> int:
        row = self._conn.execute(
            "SELECT balance FROM accounts WHERE account=?", (account,)
        ).fetchone()
        if row is None:
            raise ShardError("unknown_account", f"unknown account: {account}",
                             account=account)
        return int(row["balance"])

    def _balance_locked(self, account: str, conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT balance FROM accounts WHERE account=?", (account,)).fetchone()
        if row is None:
            raise ShardError("unknown_account", f"unknown account: {account}",
                             account=account)
        return int(row["balance"])


    def _lookup_applied(self, request_id: str) -> Optional[Dict[str, Any]]:
        if not request_id:
            return None
        row = self._conn.execute(
            "SELECT response FROM applied_requests WHERE request_id=?", (request_id,)
        ).fetchone()
        return json.loads(row["response"]) if row else None

    def _remember_applied(self, conn: sqlite3.Connection, request_id: str, kind: str,
                          response: Dict[str, Any], epoch: Optional[int],
                          amount_cents: int = 0) -> None:
        if not request_id:
            return
        row = conn.execute("SELECT COALESCE(MAX(seq), 0) AS s FROM applied_requests").fetchone()
        next_seq = int(row["s"]) + 1
        conn.execute(
            "INSERT OR REPLACE INTO applied_requests"
            "(request_id, seq, kind, response, epoch, amount_cents, applied_at)"
            " VALUES(?,?,?,?,?,?,?)",
            (request_id, next_seq, kind, json.dumps(response), int(epoch or 0),
             int(amount_cents), self._now()),
        )

    def count_applied(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS n FROM applied_requests").fetchone()
        return int(row["n"])


    def applied_since(self, position: int, limit: int = 500) -> Dict[str, Any]:
        """Ordered slice of the idempotency log, for change capture.

        The log is the shard's own record of every write it applied, keyed by the
        client's ``request_id`` and stamped with a monotonically increasing row
        id.  A migration replays from a durable ``position`` and stops at
        ``limit``, reporting ``more`` so a huge delta is applied in batches
        rather than one unbounded burst.
        """
        rows = self._conn.execute(
            "SELECT seq, request_id, kind, response, epoch, amount_cents, applied_at"
            " FROM applied_requests WHERE seq > ? ORDER BY seq LIMIT ?",
            (int(position), int(limit) + 1)
        ).fetchall()
        more = len(rows) > limit
        rows = rows[:limit]
        return {
            "position": int(rows[-1]["seq"]) if rows else int(position),
            "more": more,
            "entries": [dict(r) for r in rows],
        }

    def import_applied(self, entry: Dict[str, Any], *, epoch: int) -> bool:
        """Apply a captured write on the new owner, exactly once.

        The original ``request_id`` travels with the entry, so if the migration
        replayed the same delta twice the second attempt finds the id already in
        ``applied_requests`` and does nothing.  The balance that results is the
        balance the original write would have produced, because the operations
        are additive and the snapshot they were applied on top of is the same.
        """
        request_id = entry["request_id"]
        kind = entry.get("kind", "delta")
        cents = int(entry.get("amount_cents") or 0)
        if cents <= 0:
            raise ShardError("migration_conflict",
                             "delta entry has no amount; cannot replay safely")
        payload = entry["response"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        src, dst = payload.get("account"), payload.get("to")

        if self._lookup_applied(request_id) is not None:
            return False
        if kind == "transfer":
            if not src or not dst:
                raise ShardError("migration_conflict", "delta transfer lacks endpoints")
            self.local_transfer(src, dst, cents, epoch=epoch, request_id=request_id)
        elif kind == "deposit":
            self.deposit(src, cents, epoch=epoch, request_id=request_id)
        elif kind == "withdraw":
            self.withdraw(src, cents, epoch=epoch, request_id=request_id)
        else:
            raise ShardError("migration_conflict", f"cannot replay kind {kind!r}")
        return True

    def delete_range(self, low: str, high: str) -> int:
        """Remove the old owner's rows for a migrated range (final cleanup)."""
        _, low_n, prefix, high_n = parse_range(f"{low}-{high}")
        removed = 0
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            for number in range(low_n, high_n + 1):
                cur = self._conn.execute("DELETE FROM accounts WHERE account=?",
                                         (format_account(prefix, number),))
                removed += cur.rowcount
            self._conn.commit()
        return removed


    def deposit(self, account: str, cents: int, *, epoch: Optional[int] = None,
                request_id: Optional[str] = None) -> Dict[str, Any]:
        return self._simple_op("deposit", account, cents, +1, epoch, request_id)

    def withdraw(self, account: str, cents: int, *, epoch: Optional[int] = None,
                 request_id: Optional[str] = None) -> Dict[str, Any]:
        return self._simple_op("withdraw", account, cents, -1, epoch, request_id)

    def _simple_op(self, kind: str, account: str, cents: int, sign: int,
                   epoch: Optional[int], request_id: Optional[str]) -> Dict[str, Any]:
        if cents <= 0:
            raise ShardError("invalid_amount", "amount must be greater than zero")

        cached = self._lookup_applied(request_id)
        if cached is not None:
            cached = dict(cached)
            cached["replayed"] = True
            return cached

        with self._write_lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self.assert_writable(account, epoch)
                current = self._balance_locked(account, self._conn)
                if sign < 0 and current < cents:
                    raise ShardError("insufficient_funds", "insufficient funds",
                                     account=account, balance_cents=current,
                                     balance=round(current / 100.0, 2))
                self._conn.execute(
                    "UPDATE accounts SET balance = balance + ? WHERE account=?",
                    (sign * cents, account),
                )
                new_balance = self._balance_locked(account, self._conn)
                response = {
                    "status": "success", "action": kind, "account": account,
                    "balance_cents": new_balance, "balance": round(new_balance / 100.0, 2),
                    "shard": self.shard, "epoch": int(epoch or 0),
                }
                self._remember_applied(self._conn, request_id, kind, response, epoch, cents)
                self._conn.commit()
            except ShardError:
                self._conn.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                self._conn.rollback()
                if "CHECK" in str(exc):
                    raise ShardError("insufficient_funds", "insufficient funds",
                                     account=account) from exc
                raise
            except Exception:
                self._conn.rollback()
                raise
        return response

    def local_transfer(self, src: str, dst: str, cents: int, *, epoch: Optional[int] = None,
                       request_id: Optional[str] = None) -> Dict[str, Any]:
        """Move money between two accounts of *this* shard atomically."""
        if cents <= 0:
            raise ShardError("invalid_amount", "amount must be greater than zero")
        if src == dst:
            raise ShardError("invalid_transfer", "source and destination must differ")

        cached = self._lookup_applied(request_id)
        if cached is not None:
            cached = dict(cached)
            cached["replayed"] = True
            return cached

        with self._write_lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                self.assert_writable(src, epoch)
                self.assert_writable(dst, epoch)
                src_balance = self._balance_locked(src, self._conn)
                self._balance_locked(dst, self._conn)
                if src_balance < cents:
                    raise ShardError("insufficient_funds", "insufficient funds",
                                     account=src, balance_cents=src_balance,
                                     balance=round(src_balance / 100.0, 2))
                self._conn.execute("UPDATE accounts SET balance = balance - ? WHERE account=?",
                                   (cents, src))
                self._conn.execute("UPDATE accounts SET balance = balance + ? WHERE account=?",
                                   (cents, dst))
                src_after = self._balance_locked(src, self._conn)
                dst_after = self._balance_locked(dst, self._conn)
                response = {
                    "status": "success", "action": "transfer", "account": src,
                    "balance_cents": src_after, "balance": round(src_after / 100.0, 2),
                    "to": dst, "to_balance_cents": dst_after,
                    "to_balance": round(dst_after / 100.0, 2),
                    "shard": self.shard, "epoch": int(epoch or 0),
                }
                self._remember_applied(self._conn, request_id, "transfer", response, epoch, cents)
                self._conn.commit()
            except ShardError:
                self._conn.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                self._conn.rollback()
                if "CHECK" in str(exc):
                    raise ShardError("insufficient_funds", "insufficient funds",
                                     account=src) from exc
                raise
            except Exception:
                self._conn.rollback()
                raise
        return response


    def prepare(self, txn_id: str, branch: Dict[str, Any], epoch: int) -> Dict[str, Any]:
        """Validate a branch and durably promise to be able to commit it.

        Two idempotency layers, both needed:

        * ``prepare_decisions`` remembers the vote, so a repeated PREPARE (the
          coordinator restarted and re-sent the message) returns the stored vote
          instead of validating against a balance that may already have moved;
        * ``txn_prepared`` remembers a rejection, so the coordinator can ask
          "what did you say about txn X?" after its own crash.
        """
        txn_id = str(txn_id)
        account = branch["account"]
        kind = branch["kind"]
        cents = int(branch["amount_cents"])

        row = self._conn.execute(
            "SELECT decision FROM prepared_decisions WHERE txn_id=?", (txn_id,)
        ).fetchone()
        if row is not None:
            decision = row["decision"]
            return self._vote_body(txn_id, "YES" if decision == COMMITTED else "NO",
                                   reason="decision already recorded", repeated=True,
                                   decision=decision)

        row = self._conn.execute(
            "SELECT vote, reason FROM prepare_decisions WHERE txn_id=?", (txn_id,)
        ).fetchone()
        if row is not None:
            return self._vote_body(txn_id, row["vote"], reason=row["reason"], repeated=True)

        with self._write_lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    self.assert_writable(account, epoch)
                except StaleEpoch as exc:
                    self._conn.rollback()
                    self._store_rejection(txn_id, "stale_epoch", exc.message, "NO", epoch)
                    return self._vote_body(txn_id, "NO", reason=exc.message, code=exc.code)
                except NotOwner as exc:
                    self._conn.rollback()
                    self._store_rejection(txn_id, "not_owner", exc.message, "NO", epoch)
                    return self._vote_body(txn_id, "NO", reason=exc.message, code=exc.code)
                balance = self._balance_locked(account, self._conn)
                if kind == "DEBIT" and balance < cents:
                    raise ShardError("insufficient_funds", "insufficient funds",
                                     account=account, balance_cents=balance,
                                     balance=round(balance / 100.0, 2))
                if kind == "CREDIT" and branch.get("account_must_exist", True):
                    pass
                self._conn.execute(
                    """
                    INSERT INTO txn_prepared(txn_id, payload, status, epoch, created_at, updated_at)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(txn_id) DO UPDATE SET
                        payload=excluded.payload, status=excluded.status,
                        epoch=excluded.epoch, updated_at=excluded.updated_at
                    """,
                    (txn_id, json.dumps(branch), PREPARED, int(epoch),
                     self._now(), self._now()),
                )
                self._conn.execute(
                    "INSERT OR REPLACE INTO prepare_decisions(txn_id, vote, reason, created_at)"
                    " VALUES(?,'YES','',?)",
                    (txn_id, self._now()),
                )
                self._conn.commit()
            except ShardError as exc:
                self._conn.rollback()
                self._store_rejection(txn_id, exc.code, exc.message, "NO", epoch)
                return self._vote_body(txn_id, "NO", reason=exc.message, code=exc.code,
                                       **{k: v for k, v in exc.extra.items()})
            except Exception:
                self._conn.rollback()
                raise

        return self._vote_body(txn_id, "YES", reason="prepared", decision="PREPARED")

    def _store_rejection(self, txn_id: str, code: str, message: str, vote: str,
                         epoch: int) -> None:
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                "INSERT OR REPLACE INTO prepare_decisions(txn_id, vote, reason, created_at)"
                " VALUES(?,?,?,?)",
                (txn_id, vote, message, self._now()),
            )
            self._conn.execute(
                """
                INSERT INTO txn_prepared(txn_id, payload, status, epoch, created_at, updated_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(txn_id) DO UPDATE SET status=excluded.status,
                    updated_at=excluded.updated_at
                """,
                (txn_id, json.dumps({"rejected": code}), ABORTED, int(epoch),
                 self._now(), self._now()),
            )
            self._conn.commit()

    def _vote_body(self, txn_id: str, vote: str, *, reason: str = "", code: str = "",
                   repeated: bool = False, **extra: Any) -> Dict[str, Any]:
        body = {
            "status": "success", "txn_id": txn_id, "participant": self.shard,
            "vote": vote, "reason": reason, "repeated": repeated,
        }
        if code:
            body["code"] = code
        body.update(extra)
        return body

    def commit_prepared(self, txn_id: str, decision: str = COMMITTED) -> Dict[str, Any]:
        """Apply (or discard) a prepared branch.  Safe to call many times."""
        txn_id = str(txn_id)
        decision = decision.upper()
        row = self._conn.execute(
            "SELECT decision FROM prepared_decisions WHERE txn_id=?", (txn_id,)
        ).fetchone()
        if row is not None and row["decision"] == decision:
            return {"status": "success", "txn_id": txn_id, "participant": self.shard,
                    "decision": decision, "repeated": True, "applied": 0}

        with self._write_lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                pending = self._conn.execute(
                    "SELECT payload, status FROM txn_prepared WHERE txn_id=?", (txn_id,)
                ).fetchone()
                already = self._conn.execute(
                    "SELECT decision FROM prepared_decisions WHERE txn_id=?", (txn_id,)
                ).fetchone()

                if decision == COMMITTED and already is not None and already["decision"] == ABORTED:
                    raise ShardError("decision_conflict",
                                     f"txn {txn_id} was already aborted on {self.shard}",
                                     status=409)
                if decision == ABORTED and already is not None and already["decision"] == COMMITTED:
                    raise ShardError("decision_conflict",
                                     f"txn {txn_id} was already committed on {self.shard}",
                                     status=409)

                applied = 0
                if decision == COMMITTED:
                    if pending is None:
                        raise ShardError("unknown_transaction",
                                         f"txn {txn_id} is not prepared on {self.shard}")
                    branch = json.loads(pending["payload"])
                    account = branch["account"]
                    cents = int(branch["amount_cents"])
                    kind = branch["kind"]
                    delta = -cents if kind == "DEBIT" else cents
                    cur = self._conn.execute(
                        "UPDATE accounts SET balance = balance + ? WHERE account=?",
                        (delta, account),
                    )
                    if cur.rowcount != 1:
                        raise ShardError("unknown_account", f"unknown account: {account}",
                                         account=account)
                    applied = 1

                self._conn.execute(
                    "INSERT OR REPLACE INTO prepared_decisions(txn_id, decision, decided_at)"
                    " VALUES(?,?,?)",
                    (txn_id, decision, self._now()),
                )
                if pending is not None:
                    self._conn.execute(
                        "UPDATE txn_prepared SET status=?, updated_at=? WHERE txn_id=?",
                        (decision, self._now(), txn_id),
                    )
                self._conn.commit()
            except ShardError:
                self._conn.rollback()
                raise
            except sqlite3.IntegrityError as exc:
                self._conn.rollback()
                if "CHECK" in str(exc):
                    raise ShardError("insufficient_funds",
                                     "insufficient funds at commit time",
                                     account=branch.get("account")) from exc
                raise
            except Exception:
                self._conn.rollback()
                raise

        return {"status": "success", "txn_id": txn_id, "participant": self.shard,
                "decision": decision, "applied": applied, "repeated": False,
                "branch": (json.loads(pending["payload"]) if pending is not None else None)}

    def import_prepared(self, txn_id: str, branch: Dict[str, Any], epoch: int) -> bool:
        """Adopt a prepared branch that was made on another shard.

        Only the live re-sharding bonus needs this: a branch may be prepared on
        the old owner while the account is mid-migration, and the new owner has
        to be able to honour the coordinator's COMMIT.  The branch keeps its
        original ``txn_id`` so that exactly one COMMIT message resolves it
        wherever it now lives, and importing is idempotent.
        """
        txn_id = str(txn_id)
        with self._write_lock:
            cur = self._conn.execute(
                """
                INSERT INTO txn_prepared(txn_id, payload, status, epoch, created_at, updated_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(txn_id) DO NOTHING
                """,
                (txn_id, json.dumps(branch), PREPARED, int(epoch), self._now(), self._now()),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def transaction_status(self, txn_id: str) -> Dict[str, Any]:
        """What does this participant know about ``txn_id``?"""
        txn_id = str(txn_id)
        decision = self._conn.execute(
            "SELECT decision, decided_at FROM prepared_decisions WHERE txn_id=?", (txn_id,)
        ).fetchone()
        pending = self._conn.execute(
            "SELECT status, created_at FROM txn_prepared WHERE txn_id=?", (txn_id,)
        ).fetchone()
        vote_row = self._conn.execute(
            "SELECT vote, reason FROM prepare_decisions WHERE txn_id=?", (txn_id,)
        ).fetchone()

        state = "UNKNOWN"
        age = 0.0
        if decision is not None:
            state = decision["decision"]
        elif pending is not None:
            age = max(0.0, self._now() - float(pending["created_at"]))
            if pending["status"] == PREPARED and age > UNKNOWN_AFTER_SECONDS:
                state = "UNKNOWN"
            else:
                state = pending["status"]

        return {
            "status": "success", "shard": self.shard, "txn_id": txn_id, "state": state,
            "vote": vote_row["vote"] if vote_row else None,
            "reason": vote_row["reason"] if vote_row else None,
            "age_seconds": round(age, 3),
            "prepared_count": int(self.prepared_count()),
        }


    def prepared_count(self) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM txn_prepared WHERE status=?", (PREPARED,)
        ).fetchone()
        return int(row["n"])

    def prepared_list(self) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT txn_id, payload, status, epoch, created_at, updated_at FROM txn_prepared"
            " WHERE status=? ORDER BY created_at", (PREPARED,)
        ).fetchall()
        return [dict(r) for r in rows]

    def decisions_list(self) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT txn_id, decision, decided_at FROM prepared_decisions ORDER BY decided_at"
        ).fetchall()
        return [dict(r) for r in rows]

    def resolve_in_flight(self, *, force: str = ABORTED, only_stale_seconds: float = 0.0,
                          shard_map_note: str = "") -> List[str]:
        """Force a decision on every prepared branch this participant is holding.

        Used by two callers with different intent:

        * the **bonus migration** calls it before ownership changes, so no branch
          is stranded while the account moves;
        * an operator repairing a deadlocked participant.

        ``force=ABORTED`` is the only safe default: aborting never loses money,
        whereas committing a branch whose coordinator already decided ABORT would.
        """
        force = force.upper()
        if force not in (COMMITTED, ABORTED):
            raise ShardError("invalid_decision", f"cannot force decision {force!r}")
        resolved: List[str] = []
        now = self._now()
        for row in self.prepared_list():
            if only_stale_seconds and (now - float(row["created_at"])) < only_stale_seconds:
                continue
            txn_id = row["txn_id"]
            try:
                self.commit_prepared(txn_id, force)
                resolved.append(txn_id)
            except ShardError:
                continue
        return resolved

    def legacy_dump(self) -> List[Dict[str, Any]]:
        """Rows for ``POST /internal/bulk`` (a raw seed for tests)."""
        return [{"account": a, "balance_cents": b} for a, b in self.snapshot().items()]
