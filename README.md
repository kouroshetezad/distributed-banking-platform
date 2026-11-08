# Distributed banking platform

A sharded bank built for Assignment 2 of a distributed systems course. Accounts
`A0001` through `A2000` are split across shards, each shard runs two application
replicas over one SQLite database, and transfers that cross a shard boundary
commit through two-phase commit. Two bonus sections add live re-sharding and a
self-healing replica controller.

Everything runs on the Python standard library. `requirements.txt` installs
nothing; the optional `docker` SDK listed there is commented out because the
self-healing controller talks to the Docker API over plain HTTP.

Both bonus parts, every experiment in the assignment and the recovery scenarios
are covered by the test suite in `tests/` (27 checks).

## Architecture

```
client
  |
  v
gateway :8000 ----> shard-a-1 :8080
  |      |           shard-a-2 :8080    (shard A: A0001-A1000)
  |      |
  |      +---------> shard-b-1 :8080
  |      |           shard-b-2 :8080    (shard B: A1001-A2000)
  |      |
  |      +---------> shardmap :8500     (ranges, epochs, replicas)
  |
  +----------------> coordinator :8100  (2PC, durable journal)
```

The client knows one address, the gateway. The gateway answers "which shard owns
this account" from the shard map, picks a healthy replica for that shard, and
sends the request there. For a cross-shard transfer it hands the work to the
coordinator, because two-phase commit does not belong in a router.

| Service | Port | What it does |
| --- | --- | --- |
| gateway | 8000 | routing, replica selection, health loop, client-facing API |
| coordinator | 8100 | two-phase commit and the transaction journal |
| shardmap | 8500 | shard map registry, epoch, replica sets, migration driver |
| shard replicas | 8080 (internal) | banking rows for one shard; both replicas open the same SQLite file |
| selfhealer | none | watches replicas on the Docker socket and starts replacements |

Replica selection defaults to least-connections with round-robin as the
tie-break, since a transfer holds the write lock longer than a balance read. Set
`REPLICA_STRATEGY=round_robin` to change it.

## Running with Docker

```bash
bash scripts/up.sh
```

The script builds the image, starts the compose stack, registers the two shards
with the supervisor and pushes the shard map to every replica. When it returns,
the gateway is on `http://localhost:8000`, the coordinator on
`http://localhost:8100` and the shard map on `http://localhost:8500`.

Use the bundled client to poke at it:

```bash
python3 -m platform.client balance A1001
python3 -m platform.client deposit A0001 50
python3 -m platform.client transfer A0500 A1500 12.50
python3 -m platform.client shardmap
python3 -m platform.client --interactive
```

Or with curl:

```bash
curl -s "http://localhost:8000/account?account=A1500"
curl -s -X POST http://localhost:8000/transfer \
  -H 'content-type: application/json' \
  -d '{"account":"A0500","to":"A1500","amount":12.5}'
```

A cross-shard transfer returns a `txn_id`. If the reply is `PENDING`, ask the
coordinator for the outcome later with
`python3 -m platform.client --query-txn <txn_id>`.

To reset everything, including the database volumes:

```bash
docker compose down -v
```

## Running without Docker

```bash
python3 -m platform.local_stack
```

This starts the same service classes in one process on loopback ports, using a
temporary directory for the SQLite files. The test suite uses the same harness,
so protocol bugs can be reproduced without containers.

## Tests

```bash
python3 -m tests.run_tests              # all 27 checks
python3 -m tests.run_tests experiments  # one group
python3 -m tests.run_tests --list       # list every check
```

| Group | Checks | What it covers |
| --- | --- | --- |
| `experiments` | 8 | shard distribution, replica load, concurrent operations, replica interruption, local and cross-shard transfers, interrupted transaction, concurrent cross-shard transfers, descriptor release |
| `recovery` | 8 | vote-no abort, coordinator crash while prepared, commit recorded but ack missing, repeated COMMIT, repeated PREPARE, unresolved transaction queries, restart durability, shard container start |
| `bonus1` | 5 | online copy with change capture, stale-route fencing, 2PC across a cutover, crash-consistent resume, live traffic during a move |
| `bonus2` | 6 | replica count, dead replica replacement, no duplicate healing after a controller crash, correctness under load, Docker errors surfaced, volume mount on a replacement |

The runner exits with 0 only when every selected check passes. On a Linux VM
with a low file-descriptor limit it raises the soft limit itself, and prints the
`ulimit -n` advice if the hard limit is too low.

## Demos

Both scripts need the compose stack from `scripts/up.sh` and a Docker socket the
self-healer can use.

`bash scripts/demo_migration.sh` adds shard C, registers it, moves
`A1501-A2000` from shard B to C while a background loop keeps depositing into
`A1800`, then prints the new route for that account and the shard map history.

`bash scripts/demo_selfhealing.sh` kills one replica of shard B without a
restart policy, watches the supervisor's count go from one down replica back to
zero, shows the replacement container the controller started, and runs a deposit
on the healed shard.

## How the guarantees hold

### Writes are applied once

Every write carries a `request_id`. Each shard stores the ids it has applied in
`applied_requests`, so a retry with the same id returns the stored answer
instead of moving money a second time. The client generates one id per operation
and reuses it across retries, which is what makes a timeout safe to retry.

Amounts are integer cents inside the platform. A decimal amount is converted
once at the edge, at most two decimal places, and peers exchange `*_cents`
values so no float round-trip sits in the middle.

### Two-phase commit

The coordinator assigns a transaction id, prepares both participants, and
records the decision in its journal before any COMMIT leaves. After a restart it
replays the recorded decision and never flips it. A participant that cannot be
reached is retried, and the transaction stays `PENDING` rather than being
reported as failed. Follow-up messages go through the gateway's `/route`, so a
replica that dies mid-transaction does not strand the decision.

### Epoch fencing

The shard map carries an epoch that increments on every change. A write reaches
a shard with the caller's epoch, and the shard refuses it with `409 stale_epoch`
before touching a row if a newer map exists. A shard that no longer owns the
account refuses with `409 not_owner`. A request that raced a cutover therefore
cannot modify both sides of the move.

### Live re-sharding (bonus 1)

`platform/migration.py` moves a range while banking continues. Each phase writes
its position to the registry before doing work, so a crash resumes instead of
guessing: `PLANNED`, `SNAPSHOTTING`, `CATCHUP` (replay the delta from the old
owner's idempotency log), `FENCING` (bump the epoch; the old owner starts
refusing writes), `ACTIVATED` (publish the new map), `COMPLETED` (delete the old
rows). Ownership changes in one epoch bump, so there is never a moment with two
writable owners.

### Self-healing replicas (bonus 2)

`platform/selfhealing.py` keeps a desired replica count per shard. It probes
every registered replica, and when a shard falls short it starts another front
end on the same shard database and registers it. Each replacement is claimed in
the registry by shard and generation before the container starts, so a
controller that crashes mid-heal resumes the same claim instead of starting a
second replacement. There is no restart policy, Swarm or Kubernetes involved.

## Repository layout

```
platform/
  gateway.py          routing, replica pool, health loop
  coordinator.py      2PC, journal, recovery
  shard_server.py     shard replica service and participant endpoints
  shardmap_service.py shard map supervisor, migration endpoints
  migration.py        live re-sharding state machine
  selfhealing.py      replica controller
  client.py           CLI client
  local_stack.py      in-process stack for tests and local use
  storage/            SQLite access for shards, registry and journal
  sharding/           ranges, epochs, fencing helpers
tests/                27 checks in four groups
scripts/              up.sh and the two demo scripts
docker/run.sh         container entrypoint, picks a role from $ROLE
docker-compose.yml    gateway, coordinator, shard map, four replicas, self-healer
docker-compose.migrate.yml  adds shard C for the migration demo
```

## Client-facing endpoints

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/account?account=A1001` | GET | balance for one account |
| `/deposit`, `/withdraw` | POST | single-account write |
| `/transfer` | POST | local or cross-shard transfer |
| `/txn?txn_id=` | GET | transaction status from the coordinator |
| `/shard-map` | GET | current ranges, epoch and replicas |
| `/clusters` | GET | per-shard replica state |
| `/gateway/stats` | GET | replica pools, in-flight requests, served counts |
| `/health` | GET | liveness |

Participant and supervisor endpoints are listed at the top of
`platform/shard_server.py` and `platform/shardmap_service.py`.

## Notes

- The compose stack runs on one machine. Replicas of a shard open the same
  SQLite file from a shared volume, which is how the assignment's two-replica
  requirement is met without adding a replication layer.
- There is no authentication or TLS. Treat this as a teaching system running on
  localhost.
- The platform source is copied into the image at build time, so a code change
  needs `docker compose up --build -d`, not a restart.
- `report.pdf` is the seven-page written report (Persian) and
  `hw2-video-lowsize.mp4` is the demo recording.
