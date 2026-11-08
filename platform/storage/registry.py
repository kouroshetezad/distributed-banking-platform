"""Durable state of the *platform* rather than of one shard.

Three small databases live here:

* ``ShardMapRegistry`` - the authoritative shard map, versioned by a
  monotonically increasing epoch, plus the migration state machine that the
  self-healing and re-sharding bonuses drive;
* ``CoordinatorJournal`` - the two-phase-commit coordinator's durable state, one
  row per transaction, rewritten transactionally on every state change;
* ``HealthBoard`` - the gateway's rolling view of which replicas answered, the
  basis for load distribution *and* for failure detection.

Keeping the registry's own database (rather than a config file) is what makes
the expert bonus possible: the epoch and the migration phase have to survive a
crash of the process that owns them, and they have to be readable by whoever
takes over.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

from ..http_api import ValidationError, require_account_range
from ..sharding.shard_map import ShardMap, ShardMapError

MIGRATION_PHASES = (
    "PLANNED",
    "SNAPSHOTTING",
    "CATCHUP",
    "FENCING",
    "ACTIVATED",
    "COMPLETED",
    "FAILED",
)
TERMINAL_PHASES = ("COMPLETED", "FAILED")


class Registry:
    """Common SQLite plumbing for the coordinator/registry databases."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or ".", exist_ok=True)
        self._lock = threading.RLock()
        self._local = threading.local()
        self._conns: List[sqlite3.Connection] = []
        self._conns_lock = threading.Lock()
        self._configure(self._conn)

    @property
    def _conn(self) -> sqlite3.Connection:
        """One connection per thread, for the same reason as ShardStore."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0,
                                   isolation_level=None)
            conn.row_factory = sqlite3.Row
            self._configure(conn)
            self._local.conn = conn
            with self._conns_lock:
                self._conns.append(conn)
        return conn

    @staticmethod
    def _configure(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=30000")

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def close(self) -> None:
        with self._conns_lock:
            conns, self._conns = self._conns, []
        for conn in conns:
            try:
                conn.close()
            except Exception:
                pass
        self._local = threading.local()

    def release_thread(self) -> None:
        """Close the connection owned by the *current* thread, if any.

        Each request is served by a fresh thread on the thread-per-connection
        server, so the per-thread connection created for that request must be
        released when the request ends.  Without this a long-lived service leaks
        one file descriptor per request and eventually fails with the famously
        misleading ``sqlite3.OperationalError: unable to open database file``.
        """
        conn = getattr(self._local, "conn", None)
        if conn is None:
            return
        self._local.conn = None
        with self._conns_lock:
            try:
                self._conns.remove(conn)
            except ValueError:
                pass
        try:
            conn.close()
        except Exception:
            pass



class ShardMapRegistry(Registry):
    """The authoritative shard map, its version history and migration state."""

    def __init__(self, db_path: str, initial: Optional[ShardMap] = None,
                 actors: Optional[Dict[str, List[str]]] = None):
        """``actors`` maps an owner name to the URLs that can serve it.

        It is stored alongside the map so that adding shard C does not require
        editing a compose file: whoever publishes the epoch declares who serves
        it.  A malformed entry is rejected on write rather than causing a
        confusing failure at request time.
        """
        super().__init__(db_path)
        self._init_schema()
        if initial is not None and self.current() is None:
            if not initial.note:
                initial.note = "initial map"
            self.publish(initial, actor=actors or {})

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS shard_maps (
                epoch INTEGER PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                payload TEXT NOT NULL,
                actors TEXT NOT NULL DEFAULT '{}',
                note TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                created_by TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS current_map (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                epoch INTEGER NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migrations (
                id TEXT PRIMARY KEY,
                source_shard TEXT NOT NULL,
                target_shard TEXT NOT NULL,
                low TEXT NOT NULL,
                high TEXT NOT NULL,
                base_epoch INTEGER NOT NULL,
                target_epoch INTEGER NOT NULL,
                phase TEXT NOT NULL,
                plan TEXT NOT NULL,
                progress TEXT NOT NULL,
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at REAL NOT NULL,
                kind TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS replica_claims (
                shard TEXT NOT NULL,
                generation INTEGER NOT NULL,
                state TEXT NOT NULL DEFAULT 'claimed',
                url TEXT NOT NULL DEFAULT '',
                claimed_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (shard, generation)
            )
            """
        )
        self._conn.commit()


    def log_event(self, kind: str, detail: str = "") -> None:
        with self.tx() as conn:
            conn.execute("INSERT INTO events(at, kind, detail) VALUES(?,?,?)",
                         (time.time(), kind, detail))

    def events(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT at, kind, detail FROM events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


    def current_epoch(self) -> int:
        row = self._conn.execute("SELECT epoch FROM current_map WHERE id=1").fetchone()
        if row is not None:
            epoch = int(row["epoch"])
            exists = self._conn.execute(
                "SELECT 1 FROM shard_maps WHERE epoch=?", (epoch,)).fetchone()
            if exists is not None:
                return epoch
        row = self._conn.execute("SELECT MAX(epoch) AS e FROM shard_maps").fetchone()
        return int(row["e"]) if row and row["e"] is not None else 0

    def current(self) -> Optional[ShardMap]:
        epoch = self.current_epoch()
        if epoch == 0:
            return None
        try:
            return self.get(epoch).get("map")
        except ShardMapError:
            return None

    def current_body(self) -> Optional[Dict[str, Any]]:
        epoch = self.current_epoch()
        if epoch == 0:
            return None
        try:
            return self.get(epoch)
        except ShardMapError:
            return None

    def get(self, epoch: int) -> Dict[str, Any]:
        row = self._conn.execute("SELECT * FROM shard_maps WHERE epoch=?", (int(epoch),)).fetchone()
        if row is None:
            raise ShardMapError(f"no shard map at epoch {epoch}")
        payload = json.loads(row["payload"])
        mapping = ShardMap.from_ranges(payload["epoch"], payload["ranges"],
                                       payload.get("accounts", []), payload.get("note", ""))
        return {
            "epoch": int(row["epoch"]),
            "fingerprint": row["fingerprint"],
            "map": mapping,
            "ranges": payload["ranges"],
            "accounts": payload.get("accounts", []),
            "note": row["note"],
            "actors": json.loads(row["actors"] or "{}"),
            "created_at": row["created_at"],
            "created_by": row["created_by"],
        }

    def history(self) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT epoch, fingerprint, note, created_at, created_by FROM shard_maps"
            " ORDER BY epoch"
        ).fetchall()
        return [dict(r) for r in rows]

    def publish(self, mapping: ShardMap, *, actor: Optional[Dict[str, List[str]]] = None,
                created_by: str = "") -> Dict[str, Any]:
        """Install ``mapping`` as the new current map.

        The epoch must be strictly greater than the current one.  That single
        rule is what makes the fence sound: epochs can never go backwards, so a
        shard that has seen epoch *n* can reject any request that speaks an older
        one for the rest of eternity, without keeping a history.
        """
        if mapping.epoch <= self.current_epoch():
            raise ShardMapError(
                f"epoch {mapping.epoch} is not newer than the current epoch "
                f"{self.current_epoch()}"
            )
        actors = actor if actor is not None else (self.current_body() or {}).get("actors", {})
        actors = {str(k).upper(): [str(u).rstrip("/") for u in v] for k, v in actors.items()}
        payload = {
            "epoch": mapping.epoch,
            "ranges": [r.as_dict() for r in mapping.ranges],
            "accounts": mapping.accounts,
            "note": mapping.note,
        }
        with self.tx() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO shard_maps"
                "(epoch, fingerprint, payload, actors, note, created_at, created_by)"
                " VALUES(?,?,?,?,?,?,?)",
                (mapping.epoch, mapping.fingerprint(), json.dumps(payload),
                 json.dumps(actors), mapping.note, time.time(), created_by),
            )
            conn.execute(
                "INSERT INTO current_map(id, epoch, updated_at) VALUES(1,?,?)"
                " ON CONFLICT(id) DO UPDATE SET epoch=excluded.epoch,"
                " updated_at=excluded.updated_at",
                (mapping.epoch, time.time()),
            )
            conn.execute("INSERT INTO events(at, kind, detail) VALUES(?,?,?)",
                         (time.time(), "map_published",
                          f"epoch={mapping.epoch} fp={mapping.fingerprint()} note={mapping.note}"))
        return self.get(mapping.epoch)

    def set_actors(self, actors: Dict[str, List[str]], *, epoch: Optional[int] = None) -> Dict[str, Any]:
        """Change who serves an epoch without changing ownership."""
        epoch = epoch or self.current_epoch()
        body = self.current_body() if epoch == 0 else self.get(epoch)
        if body is None:
            raise ShardMapError(
                "no shard map exists yet; publish a map (with its ranges) before "
                "registering replicas")
        merged = dict(body["actors"])
        for shard, urls in actors.items():
            merged[str(shard).upper()] = [str(u).rstrip("/") for u in urls]
        with self.tx() as conn:
            conn.execute("UPDATE shard_maps SET actors=? WHERE epoch=?",
                         (json.dumps(merged), epoch))
            conn.execute("INSERT INTO events(at, kind, detail) VALUES(?,?,?)",
                         (time.time(), "actors_updated", json.dumps(merged)))
        return self.get(epoch)

    def actors(self) -> Dict[str, List[str]]:
        body = self.current_body()
        return body["actors"] if body else {}


    def claim_replica_slot(self, shard: str) -> Dict[str, Any]:
        """Reserve one replacement replica for ``shard``, durably.

        Returns ``{"granted": True, "generation": n}`` if the caller now owns the
        right to start that replica, or ``{"granted": False, "reason": ...}`` if
        an unfinished claim already exists.  The claim is taken inside a single
        ``BEGIN IMMEDIATE`` transaction so two controllers racing on the same gap
        cannot both be granted it.
        """
        shard = str(shard).upper()
        with self.tx() as conn:
            row = conn.execute(
                "SELECT generation, state, url FROM replica_claims WHERE shard=?"
                " ORDER BY generation DESC LIMIT 1", (shard,)
            ).fetchone()
            if row is not None and row["state"] == "claimed":
                return {"granted": False, "generation": int(row["generation"]),
                        "reason": "an unfinished replacement is already claimed"}
            generation = (int(row["generation"]) + 1) if row is not None else 1
            now = time.time()
            conn.execute(
                "INSERT INTO replica_claims(shard, generation, state, claimed_at, updated_at)"
                " VALUES(?,?,?,?,?)", (shard, generation, "claimed", now, now))
            conn.execute("INSERT INTO events(at, kind, detail) VALUES(?,?,?)",
                         (now, "replica_claimed", f"shard={shard} gen={generation}"))
            return {"granted": True, "generation": generation, "reason": "claimed"}

    def complete_replica_claim(self, shard: str, generation: int, url: str) -> None:
        shard = str(shard).upper()
        with self.tx() as conn:
            conn.execute(
                "UPDATE replica_claims SET state='live', url=?, updated_at=?"
                " WHERE shard=? AND generation=?",
                (str(url).rstrip("/"), time.time(), shard, int(generation)))
            conn.execute("INSERT INTO events(at, kind, detail) VALUES(?,?,?)",
                         (time.time(), "replica_live", f"shard={shard} gen={generation} url={url}"))

    def release_replica_claim(self, shard: str, generation: int) -> None:
        """Give up an unfinished claim so the next sweep may retry it."""
        shard = str(shard).upper()
        with self.tx() as conn:
            conn.execute(
                "UPDATE replica_claims SET state='failed', updated_at=?"
                " WHERE shard=? AND generation=?",
                (time.time(), shard, int(generation)))
            conn.execute("INSERT INTO events(at, kind, detail) VALUES(?,?,?)",
                         (time.time(), "replica_claim_released",
                          f"shard={shard} gen={generation}"))

    def replica_claims(self, shard: Optional[str] = None) -> List[Dict[str, Any]]:
        if shard is None:
            rows = self._conn.execute(
                "SELECT * FROM replica_claims ORDER BY shard, generation").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM replica_claims WHERE shard=? ORDER BY generation",
                (str(shard).upper(),)).fetchall()
        return [dict(r) for r in rows]


    def create_migration(self, migration_id: str, *, source_shard: str, target_shard: str,
                         low: str, high: str, base_epoch: int, target_epoch: int,
                         plan: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        require_account_range(f"{low}-{high}")
        with self.tx() as conn:
            conn.execute(
                """
                INSERT INTO migrations(id, source_shard, target_shard, low, high,
                    base_epoch, target_epoch, phase, plan, progress, error,
                    created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (migration_id, source_shard.upper(), target_shard.upper(), low.upper(),
                 high.upper(), int(base_epoch), int(target_epoch), "PLANNED",
                 json.dumps(plan or {}), json.dumps({}), "", time.time(), time.time()),
            )
            conn.execute("INSERT INTO events(at, kind, detail) VALUES(?,?,?)",
                         (time.time(), "migration_created",
                          f"{migration_id} {low}-{high} -> {target_shard} @epoch {target_epoch}"))
        return self.get_migration(migration_id)

    def get_migration(self, migration_id: str) -> Dict[str, Any]:
        row = self._conn.execute("SELECT * FROM migrations WHERE id=?", (migration_id,)).fetchone()
        if row is None:
            raise ValidationError("unknown_migration", f"no migration {migration_id!r}")
        body = dict(row)
        body["plan"] = json.loads(body["plan"] or "{}")
        body["progress"] = json.loads(body["progress"] or "{}")
        return body

    def migrations(self) -> List[Dict[str, Any]]:
        rows = self._conn.execute("SELECT id FROM migrations ORDER BY created_at").fetchall()
        return [self.get_migration(r["id"]) for r in rows]

    def open_migrations(self) -> List[Dict[str, Any]]:
        return [m for m in self.migrations() if m["phase"] not in TERMINAL_PHASES]

    def advance_migration(self, migration_id: str, phase: str, *,
                          progress: Optional[Dict[str, Any]] = None,
                          progress_patch: Optional[Dict[str, Any]] = None,
                          error: str = "") -> Dict[str, Any]:
        if phase not in MIGRATION_PHASES:
            raise ValidationError("invalid_phase", f"unknown migration phase {phase!r}")
        current = self.get_migration(migration_id)
        if current["phase"] in TERMINAL_PHASES and phase != current["phase"]:
            raise ValidationError(
                "migration_closed",
                f"migration {migration_id} is {current['phase']} and cannot advance",
            )
        merged = dict(current["progress"])
        merged.update(progress_patch or {})
        merged.update(progress or {})
        with self.tx() as conn:
            conn.execute(
                "UPDATE migrations SET phase=?, progress=?, error=?, updated_at=? WHERE id=?",
                (phase, json.dumps(merged), error, time.time(), migration_id),
            )
            conn.execute("INSERT INTO events(at, kind, detail) VALUES(?,?,?)",
                         (time.time(), "migration_phase",
                          f"{migration_id} -> {phase}"))
        return self.get_migration(migration_id)


    def recover(self) -> Dict[str, Any]:
        """Bring the registry back to a consistent state after a restart.

        Called at start-up.  Two things can have gone wrong:

        * an epoch was published but some actors never learned about it - the
          caller (the gateway/supervisor) re-pushes the current map, and we
          simply report which migrations are still open;
        * a migration stopped mid-flight.  We do **not** guess: the migration
          engine resumes from the recorded phase, and until it does the range is
          still owned by the old shard, so the platform keeps working.  That is
          the "converge without data loss or manual edits" requirement.
        """
        epoch = self.current_epoch()
        open_migrations = self.open_migrations()
        report = {
            "epoch": epoch,
            "open_migrations": [{"id": m["id"], "phase": m["phase"]} for m in open_migrations],
            "recovering": bool(open_migrations),
        }
        self.log_event("registry_recovered", json.dumps(report))
        return report


class CoordinatorJournal(Registry):
    """Durable two-phase-commit state, one row per transaction.

    Every transition is a single UPDATE inside a transaction, so a crash leaves
    the journal on one of the states, never between two.  ``status`` values are
    the assignment's vocabulary: INITIATED, PREPARING, PREPARED,
    COMMIT_DECIDED, ABORT_DECIDED, COMPLETED, plus PENDING/UNKNOWN computed for
    the caller when a transaction has been sitting unresolved.
    """

    STATES = ("INITIATED", "PREPARING", "PREPARED", "COMMIT_DECIDED",
              "ABORT_DECIDED", "COMPLETED", "ABORTED")

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                txn_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL DEFAULT '',
                kind TEXT NOT NULL,
                src_account TEXT NOT NULL DEFAULT '',
                dst_account TEXT NOT NULL DEFAULT '',
                amount_cents INTEGER NOT NULL DEFAULT 0,
                participants TEXT NOT NULL,
                votes TEXT NOT NULL,
                state TEXT NOT NULL,
                decision TEXT NOT NULL DEFAULT '',
                result TEXT NOT NULL DEFAULT '',
                epoch INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_txn_state ON transactions(state)"
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_txn_request ON transactions(request_id, kind)"
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS coordinator_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at REAL NOT NULL,
                txn_id TEXT NOT NULL DEFAULT '',
                kind TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self._conn.commit()

    def event(self, txn_id: str, kind: str, detail: str = "") -> None:
        with self.tx() as conn:
            conn.execute("INSERT INTO coordinator_events(at, txn_id, kind, detail)"
                         " VALUES(?,?,?,?)", (time.time(), txn_id, kind, detail))

    def events(self, limit: int = 200) -> List[Dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT at, txn_id, kind, detail FROM coordinator_events"
            " ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


    def create(self, txn_id: str, *, request_id: str, kind: str,
               participants: List[str], epoch: int, src_account: str = "",
               dst_account: str = "", amount_cents: int = 0,
               state: str = "INITIATED") -> Dict[str, Any]:
        with self.tx() as conn:
            conn.execute(
                """
                INSERT INTO transactions(txn_id, request_id, kind, src_account, dst_account,
                    amount_cents, participants, votes, state, decision, result, epoch,
                    attempts, error, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (txn_id, request_id, kind, src_account, dst_account, int(amount_cents),
                 json.dumps(list(participants)), json.dumps({}), state, "", "", int(epoch),
                 0, "", time.time(), time.time()),
            )
            conn.execute("INSERT INTO coordinator_events(at, txn_id, kind, detail)"
                         " VALUES(?,?,?,?)",
                         (time.time(), txn_id, "created",
                          f"kind={kind} participants={participants}"))
        return self.get(txn_id)

    def get(self, txn_id: str) -> Dict[str, Any]:
        row = self._conn.execute("SELECT * FROM transactions WHERE txn_id=?", (txn_id,)).fetchone()
        if row is None:
            raise ValidationError("unknown_transaction", f"no transaction {txn_id!r}")
        return self._decode(row)

    @staticmethod
    def _decode(row: sqlite3.Row) -> Dict[str, Any]:
        body = dict(row)
        body["participants"] = json.loads(body["participants"] or "[]")
        body["votes"] = json.loads(body["votes"] or "{}")
        body["result"] = json.loads(body["result"] or "null")
        return body

    def find_by_request(self, request_id: str, kind: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """The transaction a client's ``request_id`` already created, if any.

        An indexed lookup rather than a scan of recent rows: idempotency is a
        correctness property, so it must not depend on how many transactions have
        happened since.
        """
        if not request_id:
            return None
        if kind:
            row = self._conn.execute(
                "SELECT * FROM transactions WHERE request_id=? AND kind=?"
                " ORDER BY created_at LIMIT 1", (request_id, kind)).fetchone()
        else:
            row = self._conn.execute(
                "SELECT * FROM transactions WHERE request_id=?"
                " ORDER BY created_at LIMIT 1", (request_id,)).fetchone()
        return self._decode(row) if row else None

    def list(self, *, state: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
        if state:
            rows = self._conn.execute(
                "SELECT * FROM transactions WHERE state=? ORDER BY created_at DESC LIMIT ?",
                (state, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM transactions ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._decode(r) for r in rows]

    def update(self, txn_id: str, **fields: Any) -> Dict[str, Any]:
        if "state" in fields and fields["state"] not in self.STATES:
            raise ValidationError("invalid_state", f"unknown state {fields['state']!r}")
        allowed = {"state", "decision", "votes", "result", "epoch", "attempts", "error"}
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return self.get(txn_id)
        for key in ("votes", "result"):
            if key in updates and not isinstance(updates[key], str):
                updates[key] = json.dumps(updates[key])
        assignments = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [time.time(), txn_id]
        with self.tx() as conn:
            conn.execute(
                f"UPDATE transactions SET {assignments}, updated_at=? WHERE txn_id=?",
                values,
            )
        return self.get(txn_id)

    def bump_attempts(self, txn_id: str) -> None:
        with self.tx() as conn:
            conn.execute(
                "UPDATE transactions SET attempts=attempts+1, updated_at=? WHERE txn_id=?",
                (time.time(), txn_id),
            )


    def unresolved(self) -> List[Dict[str, Any]]:
        """Transactions that a restart has to finish or resolve.

        Anything that reached a decision is recoverable automatically; anything
        that did not is reported to the client as PENDING/UNKNOWN and is either
        re-driven or safely aborted by the recovery loop.
        """
        rows = self._conn.execute(
            "SELECT * FROM transactions WHERE state NOT IN ('COMPLETED','ABORTED')"
            " ORDER BY created_at"
        ).fetchall()
        return [self._decode(r) for r in rows]

    def counts(self) -> Dict[str, int]:
        rows = self._conn.execute(
            "SELECT state, COUNT(*) AS n FROM transactions GROUP BY state"
        ).fetchall()
        counts = {state: 0 for state in self.STATES}
        for row in rows:
            counts[row["state"]] = int(row["n"])
        return counts
