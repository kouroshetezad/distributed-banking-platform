"""Run the whole suite: one PASS/FAIL line per experiment.

Usage::

    python3 -m tests.run_tests              # everything
    python3 -m tests.run_tests experiments  # one group
    python3 -m tests.run_tests --list       # what is available

The exit code is 0 only if every selected test passed, so the same command works
as a smoke check in CI or by hand during the demo recording.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Callable, Dict, List, Tuple

from .helpers import run_case

GROUPS: Dict[str, List[Tuple[str, Callable]]] = {}


def _collect() -> None:
    from . import (
        test_experiments,
        test_recovery,
        test_bonus_migration,
        test_bonus_selfhealing,
    )

    GROUPS["experiments"] = [
        ("1. shard distribution", test_experiments.test_shard_distribution),
        ("2. replica load distribution", test_experiments.test_replica_load_distribution),
        ("3. concurrent replica operations",
         test_experiments.test_concurrent_replica_operations),
        ("4. replica interruption", test_experiments.test_replica_interruption),
        ("5. local and cross-shard transfers",
         test_experiments.test_local_and_cross_shard_transfers),
        ("6. interrupted distributed transaction",
         test_experiments.test_interrupted_distributed_transaction),
        ("7. concurrent cross-shard transfers",
         test_experiments.test_concurrent_cross_shard_transfers),
        ("8. per-request descriptor release",
         test_experiments.test_service_releases_descriptors_per_request),
    ]
    GROUPS["recovery"] = [
        ("A. one shard votes NO -> safe abort", test_recovery.test_scenario_a_abort),
        ("B. coordinator dies while prepared", test_recovery.test_scenario_b_prepared_crash),
        ("C. commit recorded, ack missing", test_recovery.test_scenario_c_commit_completion),
        ("   repeated COMMIT is safe", test_recovery.test_repeated_commit_is_safe),
        ("   repeated PREPARE keeps its vote", test_recovery.test_repeated_prepare_keeps_vote),
        ("   unresolved txn is queryable", test_recovery.test_unknown_transaction_is_queryable),
        ("   durability across a full restart", test_recovery.test_durability_across_restart),
        ("   shard container start-up", test_recovery.test_shard_container_startup),
    ]
    GROUPS["bonus1"] = [
        ("B1.1 online copy + change capture", test_bonus_migration.test_online_copy_and_delta),
        ("B1.2 atomic ownership + stale-route fencing",
         test_bonus_migration.test_fencing_rejects_stale_routes),
        ("B1.3 2PC and exactly-once across cutover",
         test_bonus_migration.test_2pc_during_migration),
        ("B1.4 crash-consistent state machine", test_bonus_migration.test_crash_recovery),
        ("B1.5 live traffic during migration",
         test_bonus_migration.test_live_traffic_during_migration),
    ]
    GROUPS["bonus2"] = [
        ("B2.1 maintains the configured replica count",
         test_bonus_selfhealing.test_maintains_replica_count),
        ("B2.2 replaces a dead replica", test_bonus_selfhealing.test_replaces_dead_replica),
        ("B2.3 controller crash makes no duplicates",
         test_bonus_selfhealing.test_controller_crash_no_duplicates),
        ("B2.4 correctness under concurrent load",
         test_bonus_selfhealing.test_correctness_under_load),
        ("B2.5 docker rejections surface, not swallowed",
         test_bonus_selfhealing.test_docker_api_reports_rejections),
        ("B2.6 replacement mounts the Compose volume",
         test_bonus_selfhealing.test_spawner_mounts_the_compose_volume),
    ]


def _raise_fd_limit() -> None:
    """Raise the soft file-descriptor limit to the hard limit on POSIX.

    Each ``LocalStack`` is several services: a listening socket plus one SQLite
    connection per worker thread, times every replica, gateway and coordinator.
    A machine whose soft limit is low (some Ubuntu VMs default well under the
    1024 a desktop uses) exhausts descriptors partway through the suite, and the
    failure looks like ``[Errno 24] Too many open files`` followed by
    ``unable to open database file`` - which reads like a database bug but is
    purely a resource limit.  The hard limit is the ceiling the kernel will allow
    without privileges, so lift the soft limit to it.  Failure is ignored: on
    Windows there is no such limit, and the caller simply runs as before.
    """
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if hard == resource.RLIM_INFINITY:
            return
        if soft == hard:
            if hard < 2048:
                print(f"warning: file-descriptor limit is {hard} (soft == hard). "
                      f"This suite needs more; raise it before running, e.g.\n"
                      f"    ulimit -n 8192\n"
                      f"or add '{os.environ.get('USER', 'youruser')} soft nofile 8192' "
                      f"to /etc/security/limits.conf and log back in.")
            return
        target = min(hard, 65536)
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        new_soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        print(f"file-descriptor soft limit raised {soft} -> {new_soft}")
    except Exception:
        pass


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Distributed banking platform test suite")
    parser.add_argument("groups", nargs="*", help="experiments | recovery | bonus1 | bonus2")
    parser.add_argument("--list", action="store_true", help="list tests and exit")
    args = parser.parse_args(argv)

    _raise_fd_limit()
    _collect()
    selected = args.groups or list(GROUPS)

    if args.list:
        for group in selected:
            print(group)
            for name, _ in GROUPS.get(group, []):
                print(f"    {name}")
        return 0

    unknown = [g for g in selected if g not in GROUPS]
    if unknown:
        print(f"unknown group(s): {unknown}; known: {list(GROUPS)}", file=sys.stderr)
        return 2

    print("=" * 78)
    print("Distributed Banking Platform - test suite")
    print("=" * 78)
    started = time.time()
    total = passed = 0
    failures: List[Tuple[str, str, str]] = []

    for group in selected:
        print(f"\n# {group}")
        print("-" * 78)
        for name, fn in GROUPS[group]:
            result = run_case(name, fn)
            total += 1
            if result["passed"]:
                passed += 1
                print(f"  PASS  {name}  ({result['seconds']}s)")
                if result["note"]:
                    print(f"        {result['note']}")
            else:
                print(f"  FAIL  {name}  ({result['seconds']}s)")
                for line in str(result["note"]).splitlines():
                    print(f"        {line}")
                failures.append((group, name, str(result["note"])))

    print("\n" + "=" * 78)
    print(f"{passed}/{total} passed in {time.time() - started:.1f}s")
    if failures:
        print("\nfailures:")
        for group, name, _ in failures:
            print(f"  [{group}] {name}")
    print("=" * 78)
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
