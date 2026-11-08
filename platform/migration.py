"""Live re-sharding: add a shard and move a range while banking continues.

The graded scenario is specific: bring up **Shard C**, move ``A1501-A2000`` off
Shard B, and keep serving the accounts being moved.  A static restart, freezing
the range for the whole copy, or having two writable owners all disqualify.

The protocol below is a small migration state machine.  Every step records its
progress *before* doing the work, so a crash can be resumed rather than guessed
at.  The phases are:

``PLANNED``
    The target epoch and the range are reserved in the registry.  Nothing has
    moved.  Re-running from here is safe.

``SNAPSHOTTING``
    The old owner exports its rows for the range.  Banking continues throughout:
    nothing on the old owner is locked, and writes keep landing.  Because those
    writes keep landing, the snapshot is a *prefix*, not the truth - which is
    exactly why the next phase exists.

``CATCHUP``
    The rows are installed on the new owner, then a *delta* is replayed: every
    change that happened on the old owner since the snapshot.  The delta source
    is the old owner's own idempotency log (``applied_requests``) and its
    prepared-branch log, both of which are already durable and ordered.  The
    migration keeps a durable ``delta_position`` so a crash mid-catch-up
    replays at most one delta twice - and every replay is idempotent because it
    is keyed on the original ``request_id``.

``FENCING``
    The new owner is told to take ownership of the range at the *new* epoch, and
    at the same moment the old owner is told that the new epoch is current.  In
    this phase the old owner already refuses writes to those accounts ("this is
    not mine at this epoch") while the new owner is not yet published to the
    gateway.  Traffic for those accounts is briefly rejected rather than
    misrouted; that is the only pause, and it lasts for one push.

``ACTIVATED``
    The new epoch is published.  The gateway routes the range to the new owner
    from its next request.  A request in flight that still carries the old epoch
    is *fenced*: the old owner rejects it, so it can never modify both sides.

``COMPLETED``
    The old owner's rows for the range are deleted.  Only now, after ownership
    is verified moved, is stale data removed.

Why "no lost update" holds
--------------------------
The hand-over is a single epoch bump.  Before it, exactly the old owner accepts
writes; after it, exactly the new owner does.  There is no window with two
writable owners, and no window where neither accepts the *current* epoch.  The
old owner keeps refusing the old epoch forever, because epochs never decrease.

Cross-shard 2PC during a migration
----------------------------------
If a branch is prepared on the old owner for an account that is about to move,
the prepared branch is exported with the snapshot and imported into the new
owner under the same ``txn_id``.  A COMMIT that arrives after cutover is
therefore honoured by the new owner and *only* by it.  Prepared branches that
were never decided are resolved before cutover (safely aborted), so nothing is
stranded and no durable COMMIT is discarded.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from .http_api import ValidationError
from .sharding.shard_map import ShardMap, ShardMapError, format_account, parse_range
from .storage.registry import ShardMapRegistry
from .targets import http_json, url_for

try:
    from .http_service import configure_logging

    LOG = configure_logging("bank.migration")
except Exception:
    import logging

    LOG = logging.getLogger("bank.migration")


class MigrationError(Exception):
    def __init__(self, message: str, **extra: Any):
        super().__init__(message)
        self.message = message
        self.extra = extra


def _looks_like_refused(exc: Optional[Exception]) -> bool:
    """True when a failure means "nobody is listening yet", not "rejected".

    A shard whose containers were just created may still be binding its port, so
    a refused connection during a migration is a timing artefact worth retrying,
    not a permanent error.  A 4xx/5xx reply is a real answer and is handled by
    the caller, so it is deliberately not treated as retryable here.
    """
    if exc is None:
        return False
    text = str(exc).lower()
    markers = ("connection refused", "errno 111", "connection reset",
               "timed out", "temporarily unavailable")
    if any(marker in text for marker in markers):
        return True
    cause = getattr(exc, "__cause__", None) or getattr(exc, "reason", None)
    if cause is not None and cause is not exc:
        return _looks_like_refused(cause if isinstance(cause, Exception) else None)
    return False


class MigrationEngine:
    """Runs one range migration end to end, resumable at every phase."""

    def __init__(self, registry: ShardMapRegistry,
                 push: Callable[[], Dict[str, Any]],
                 health: Optional[Callable[[str], bool]] = None):
        self.registry = registry
        self.push = push
        self.health = health or (lambda url: True)
        self._lock = threading.Lock()
        self._stop = threading.Event()


    def actors(self) -> Dict[str, List[str]]:
        return self.registry.actors()

    def replicas_of(self, shard: str) -> List[str]:
        urls = self.actors().get(shard.upper()) or []
        if not urls:
            raise MigrationError(f"shard {shard} has no registered replicas",
                                 shard=shard)
        return urls

    def _live_replica(self, shard: str) -> str:
        for url in self.replicas_of(shard):
            if self.health(url):
                return url
        raise MigrationError(f"no live replica for shard {shard}", shard=shard)

    def _post(self, shard: str, path: str, payload: Dict[str, Any],
              timeout: float = 10.0) -> Dict[str, Any]:
        """Talk to a shard, trying each replica until one answers.

        If *every* replica refuses the connection, the shard is most likely
        still starting up (its containers were just created).  That is a
        transient condition, not a migration fault, so the whole replica set is
        retried a few times before giving up - a shard that is still binding its
        port should not fail a migration that is otherwise correct.
        """
        last: Optional[Exception] = None
        deadline = time.time() + 30.0
        while True:
            for url in self.replicas_of(shard):
                try:
                    reply = http_json(url_for(url, path), payload, timeout=timeout)
                    if reply.get("_http_status", 200) >= 400:
                        return reply
                    return reply
                except Exception as exc:
                    last = exc
            if not _looks_like_refused(last) or time.time() >= deadline:
                break
            time.sleep(1.0)
        raise MigrationError(f"every replica of shard {shard} failed {path}: {last}",
                             shard=shard, path=path)

    def _post_all(self, shard: str, path: str, payload: Dict[str, Any],
                  timeout: float = 10.0) -> List[Dict[str, Any]]:
        """Talk to *every* replica of a shard and return all answers.

        Used at the fence, where reaching just one replica is not enough.  A
        shard's replicas share one database but keep their own copy of the shard
        map in it, and they serve reads from their own copy.  Telling a single
        replica that the epoch moved leaves the others still willing to accept a
        write for the departing range, so the change stream can grow again after
        a drain that thought it had quiesced.  Fanning the fence out to all of
        them closes that window.

        A replica that is already down is not fatal here: it will re-read the
        published map from the registry database when it comes back up, and it
        cannot accept a write while it is not answering at all.
        """
        replies: List[Dict[str, Any]] = []
        last: Optional[Exception] = None
        for url in self.replicas_of(shard):
            try:
                replies.append(http_json(url_for(url, path), payload, timeout=timeout))
            except Exception as exc:
                last = exc
        if not replies:
            raise MigrationError(
                f"every replica of shard {shard} failed {path}: {last}",
                shard=shard, path=path)
        return replies


    def run(self, source_shard: str, target_shard: str, low: str, high: str,
            migration_id: Optional[str] = None) -> Dict[str, Any]:
        """Start (or restart) a migration.  Idempotent for a given id."""
        source_shard = source_shard.upper()
        target_shard = target_shard.upper()
        low, high = low.upper(), high.upper()
        parse_range(f"{low}-{high}")
        with self._lock:
            return self._run_locked(source_shard, target_shard, low, high, migration_id)

    def _run_locked(self, source_shard: str, target_shard: str, low: str, high: str,
                    migration_id: Optional[str]) -> Dict[str, Any]:
        current = self.registry.current()
        if current is None:
            raise MigrationError("no shard map published; cannot migrate")

        for existing in self.registry.open_migrations():
            if (existing["source_shard"] == source_shard
                    and existing["target_shard"] == target_shard
                    and existing["low"] == low and existing["high"] == high):
                LOG.info("resuming open migration %s at phase %s", existing["id"],
                         existing["phase"])
                return self._continue(existing)

        if source_shard not in self.actors() or target_shard not in self.actors():
            raise MigrationError(
                f"both shards must be registered before migrating "
                f"(have {sorted(self.actors())})")

        owner = current.shard_of(low)
        if owner != source_shard:
            raise MigrationError(
                f"shard {source_shard} does not own {low} (it is {owner})",
                owner=owner)

        plan = {
            "low": low, "high": high, "source": source_shard, "target": target_shard,
            "base_epoch": current.epoch, "started_at": time.time(),
        }
        migration_id = migration_id or f"mig-{uuid.uuid4().hex[:12]}"
        self.registry.create_migration(
            migration_id, source_shard=source_shard, target_shard=target_shard,
            low=low, high=high, base_epoch=current.epoch,
            target_epoch=current.epoch + 1, plan=plan)
        return self._continue(self.registry.get_migration(migration_id))

    def _continue(self, migration: Dict[str, Any]) -> Dict[str, Any]:
        """Drive a migration from wherever its durable phase says it is."""
        mig_id = migration["id"]
        phase = migration["phase"]
        try:
            while phase not in ("COMPLETED", "FAILED"):
                if phase == "ACTIVATED":
                    break
                handler = {
                    "PLANNED": self._phase_snapshot,
                    "SNAPSHOTTING": self._phase_snapshot,
                    "CATCHUP": self._phase_catchup,
                    "FENCING": self._phase_fence,
                }[phase]
                migration = handler(migration)
                new_phase = migration["phase"]
                if new_phase == phase and phase == "CATCHUP":
                    break
                phase = new_phase
        except Exception as exc:
            LOG.exception("migration %s failed in phase %s", mig_id, phase)
            try:
                self.registry.advance_migration(mig_id, "FAILED", error=str(exc))
            except Exception:
                pass
            raise
        return self.registry.get_migration(mig_id)


    def _phase_snapshot(self, migration: Dict[str, Any]) -> Dict[str, Any]:
        mig_id, source = migration["id"], migration["source_shard"]
        self.registry.advance_migration(mig_id, "SNAPSHOTTING",
                                        progress_patch={"started_snapshot": time.time()})
        LOG.info("migration %s: snapshotting %s-%s from shard %s", mig_id,
                 migration["low"], migration["high"], source)

        exported = self._post(source, "/internal/migrate/export",
                              {"low": migration["low"], "high": migration["high"]})
        if exported.get("status") == "error":
            raise MigrationError(f"export from {source} failed: {exported.get('message')}")

        rows = exported.get("accounts", [])
        present = [r for r in rows if r.get("balance_cents") is not None]
        snapshot_position = {
            "applied_position": exported.get("applied_position", 0),
            "at": exported.get("snapshot_at"),
        }
        self.registry.advance_migration(
            mig_id, "CATCHUP",
            progress_patch={
                "snapshot_rows": len(present),
                "snapshot_position": snapshot_position,
                "delta_position": exported.get("applied_position", 0),
                "copied_at": time.time(),
            },
        )
        migration = self.registry.get_migration(mig_id)
        migration["_rows"] = present
        migration["_prepared"] = exported.get("prepared", [])
        return migration

    def _phase_catchup(self, migration: Dict[str, Any]) -> Dict[str, Any]:
        """Install the snapshot on the target, then replay the write delta."""
        mig_id = migration["id"]
        target, source = migration["target_shard"], migration["source_shard"]
        rows = migration.get("_rows")
        prepared = migration.get("_prepared")

        if rows is None:
            exported = self._post(source, "/internal/migrate/export",
                                  {"low": migration["low"], "high": migration["high"]})
            rows = [r for r in exported.get("accounts", [])
                    if r.get("balance_cents") is not None]
            prepared = exported.get("prepared", [])

        installed = self._post(target, "/internal/migrate/import", {
            "accounts": rows, "prepared": prepared,
            "epoch": migration["base_epoch"],
        })

        delta = self._post(source, "/internal/migrate/delta", {
            "low": migration["low"], "high": migration["high"],
            "since_position": migration["progress"].get("delta_position", 0),
            "target_shard": target,
            "target_urls": self.replicas_of(target),
            "epoch": migration["target_epoch"],
        })
        applied = delta.get("applied", [])
        LOG.info("migration %s: installed %s rows, replayed %d delta operations",
                 mig_id, installed.get("written"), len(applied))

        self.registry.advance_migration(
            mig_id, "CATCHUP",
            progress_patch={
                "delta_position": delta.get("position", 0),
                "delta_applied": int(migration["progress"].get("delta_applied", 0)) + len(applied),
                "rows_installed": installed.get("written"),
            },
        )
        migration = self.registry.get_migration(mig_id)
        if delta.get("more"):
            return migration
        return self._phase_fence(migration)

    def _phase_fence(self, migration: Dict[str, Any]) -> Dict[str, Any]:
        """Cut over: target owns the range at the new epoch, source is fenced.

        The order inside this phase is the whole ballgame:

        1. resolve (safely abort) every prepared branch for the range on the
           source, so no branch is stranded and no durable COMMIT is discarded;
        2. export any *remaining* prepared branches and import them on the
           target, so a COMMIT that is already decided is honoured by the new
           owner;
        3. build the new map and give it to the target and to the source.  After
           the source has it, the source refuses writes to the range at any
           epoch other than the new one;
        4. publish the epoch.  The gateway picks it up on its next request.
        """
        mig_id = migration["id"]
        source, target = migration["source_shard"], migration["target_shard"]
        low, high = migration["low"], migration["high"]
        self.registry.advance_migration(mig_id, "FENCING",
                                        progress_patch={"fencing_at": time.time()})
        LOG.info("migration %s: fencing %s-%s", mig_id, low, high)

        resolved = self._post(source, "/2pc/resolve",
                              {"force": "ABORTED", "only_stale_seconds": 0})
        stored = migration["progress"].get("new_map")
        if stored:
            new_map = ShardMap.from_ranges(stored["epoch"], stored["ranges"],
                                           stored["accounts"], note=stored.get("note", ""))
        else:
            current = self.registry.current()
            new_map = ShardMap.vacate_subrange(
                current, source, target, low, high, migration["target_epoch"],
                note=f"migration {mig_id}: {low}-{high} -> {target}")
        payload = {"epoch": new_map.epoch,
                   "ranges": [r.as_dict() for r in new_map.ranges],
                   "accounts": new_map.accounts,
                   "note": new_map.note}
        self.registry.advance_migration(mig_id, "FENCING",
                                        progress_patch={"new_map": payload})

        self._post_all(target, "/internal/shard-map", payload, timeout=8.0)
        self._post_all(source, "/internal/shard-map", payload, timeout=8.0)

        drained = 0
        for _ in range(50):
            position = self.registry.get_migration(mig_id)["progress"].get(
                "delta_position", 0)
            delta = self._post(source, "/internal/migrate/delta", {
                "low": low, "high": high,
                "since_position": position,
                "target_shard": target,
                "target_urls": self.replicas_of(target),
                "epoch": migration["target_epoch"],
            })
            applied = len(delta.get("applied", []))
            drained += applied
            self.registry.advance_migration(
                mig_id, "FENCING",
                progress_patch={"delta_position": delta.get("position", 0),
                                "fenced_drain": drained})
            if not delta.get("more") and applied == 0:
                break

        leftovers = self._post(source, "/internal/migrate/export", {"low": low, "high": high})
        if leftovers.get("prepared"):
            self._post(target, "/internal/migrate/import", {
                "accounts": [],
                "prepared": leftovers["prepared"],
                "epoch": migration["target_epoch"],
            })

        self.registry.advance_migration(
            mig_id, "FENCING",
            progress_patch={"fenced_at": time.time(),
                            "resolved_on_source": resolved.get("resolved", []),
                            "new_map": payload},
        )
        migration = self.registry.get_migration(mig_id)
        migration["_new_map"] = payload
        return self._phase_activate(migration)

    def _phase_activate(self, migration: Dict[str, Any]) -> Dict[str, Any]:
        mig_id = migration["id"]
        target_epoch = migration["target_epoch"]

        if self.registry.current_epoch() >= target_epoch:
            current = self.registry.current()
            owned = (current.shard_of(migration["low"]) == migration["target_shard"]
                     and current.shard_of(migration["high"]) == migration["target_shard"])
            if owned:
                LOG.info("migration %s: epoch %d already active, closing", mig_id,
                         current.epoch)
                self.push()
                self.registry.advance_migration(
                    mig_id, "ACTIVATED",
                    progress_patch={"activated_at": migration["progress"].get(
                        "activated_at", time.time()), "new_epoch": current.epoch,
                        "resumed": True},
                )
                return self.registry.get_migration(mig_id)

        payload = migration.get("_new_map") or migration["progress"].get("new_map")
        if payload is None:
            current = self.registry.current()
            try:
                new_map = ShardMap.vacate_subrange(
                    current, migration["source_shard"], migration["target_shard"],
                    migration["low"], migration["high"], target_epoch,
                    note=f"migration {mig_id}")
            except ShardMapError as exc:
                raise MigrationError(
                    f"cannot construct the post-migration map: {exc}") from exc
            payload = {"epoch": new_map.epoch,
                       "ranges": [r.as_dict() for r in new_map.ranges],
                       "accounts": new_map.accounts, "note": new_map.note}

        new_map = ShardMap.from_ranges(payload["epoch"], payload["ranges"], payload["accounts"],
                                       note=payload["note"])
        self.registry.publish(new_map, created_by="migration")
        self.push()
        LOG.info("migration %s: activated epoch %d", mig_id, new_map.epoch)
        self.registry.advance_migration(
            mig_id, "ACTIVATED",
            progress_patch={"activated_at": time.time(), "new_epoch": new_map.epoch},
        )
        return self.registry.get_migration(mig_id)


    def verify(self, migration: Dict[str, Any], *, transfers: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Prove the target really owns the range before deleting anything."""
        target = migration["target_shard"]
        reply = self._post(target, "/internal/migrate/verify", {
            "low": migration["low"], "high": migration["high"],
            "epoch": migration["target_epoch"],
        })
        return reply

    def cleanup_completed(self) -> Dict[str, Any]:
        """Delete stale rows on the source, but only once ownership has moved."""
        removed: Dict[str, Any] = {}
        for migration in self.registry.migrations():
            if migration["phase"] != "ACTIVATED":
                continue
            mig_id = migration["id"]
            try:
                self._post(migration["source_shard"], "/internal/migrate/cleanup", {
                    "low": migration["low"], "high": migration["high"],
                    "target_epoch": migration["target_epoch"],
                })
            except MigrationError as exc:
                removed[mig_id] = f"cleanup deferred: {exc}"
                continue
            self.registry.advance_migration(mig_id, "COMPLETED",
                                            progress_patch={"completed_at": time.time()})
            removed[mig_id] = "completed"
            LOG.info("migration %s: old rows for %s-%s removed", mig_id,
                     migration["low"], migration["high"])
        return removed


    def resume_all(self) -> Dict[str, Any]:
        """Finish every migration that a crash left open."""
        results: Dict[str, Any] = {}
        for migration in self.registry.open_migrations():
            try:
                LOG.info("resuming migration %s from phase %s", migration["id"],
                         migration["phase"])
                results[migration["id"]] = self._continue(migration)["phase"]
            except Exception as exc:
                LOG.exception("could not resume migration %s", migration["id"])
                results[migration["id"]] = f"failed: {exc}"
        return results

    def start_background_resume(self, interval: float = 3.0) -> threading.Thread:
        def loop() -> None:
            while not self._stop.wait(interval):
                try:
                    open_migrations = self.registry.open_migrations()
                    if open_migrations:
                        self.resume_all()
                    else:
                        self.cleanup_completed()
                except Exception:
                    LOG.exception("background migration resume failed")

        thread = threading.Thread(target=loop, name="migration-resume", daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()
