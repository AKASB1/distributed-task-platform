# Design trade-offs

Decisions made for this implementation, what they cost, and when they should be revisited.

## At-least-once delivery

A job id can reach a worker more than once (re-dispatch after a restart, reconciler re-dispatch, broker redelivery),
and an attempt can be re-run after its worker stopped heartbeating even if that worker was only paused. The platform
therefore guarantees **at-least-once execution and exactly-once completion**: every claim and every report is a
compare-and-set on the job version, so at most one attempt can move a job to a terminal state, and a cancelled job can
never complete (`RaceIT`, `FailureInjectionIT`). Side effects inside a handler are not deduplicated; handlers must be
idempotent or use their own idempotency key (the job id and attempt number are available in `TaskContext`).
Exactly-once execution would need a transactional coupling between the handler's side effects and the job row, which a
general-purpose platform cannot provide.

## Lease length versus failover time

A crashed worker's job is reclaimed after `lease-duration + lease-grace + reconciler interval` (defaults
30 s + 2 s + 1 s; the compose crash demo measured 36 s from restart to completion, including the 1 s backoff and the
5 s job). Shorter leases fail over faster but cost more heartbeat writes and risk false expiry during GC pauses or
database hiccups; a false expiry is safe (the zombie's late report is rejected) but wastes an attempt. Heartbeats run
every `heartbeat-interval` (10 s, a third of the lease), so two heartbeats can be lost before the lease expires. The
attempt counter increases on every lease, so a job whose workers keep dying is eventually dead-lettered instead of
looping forever.

## Outbox versus direct dispatch

Submission persists the job (`PENDING`), moves it to `QUEUED`, and then dispatches directly. There is no separate
outbox table: the job row itself plays that role. Its state plus `dispatched_at` tell the reconciler what still has to
be handed to the queue (`PENDING` older than 5 s, `QUEUED` not dispatched for 60 s, and every `QUEUED` job at startup
for the non-durable in-memory queue). This keeps one write path and one source of truth; the price is a recovery delay
of up to `pending-grace` / `redispatch-after` after a crash, and duplicate messages that consumers must tolerate (they
do: a stale id fails the claim CAS and is dropped). A transactional outbox with a relay would bound the delay more
tightly at the cost of another table and process.

## Polling versus broker push

Workers pull ids from a `JobDispatcher` (blocking poll with a timeout), and the reconciler polls the database on a
fixed interval (1 s, 100 ms in the load test). Pull keeps back-pressure natural: a slot only asks for work when it is
free, so no lease is taken for work that sits in a local buffer. The cost is up to one reconciler interval of extra
latency for retries and recovered jobs, and steady low-rate queries against indexed partial scans. The RabbitMQ
adapter receives pushed messages, but its consumer buffers at most `prefetch` of them and the job state still lives in
PostgreSQL; messages are hints, not authority.

RabbitMQ specifics: a message is acknowledged when a worker slot takes the id (hand-off ack), not when the job
finishes; from then on PostgreSQL is responsible (a lost claim is re-dispatched by the reconciler, a lost attempt is
reclaimed through its lease). There are no publisher confirms: a confirm per submission would add broker latency, and
the database plus reconciler already bound any loss to `redispatch-after`. Unlike the in-memory queue, the broker does
not de-duplicate, so a job that waits longer than `redispatch-after` gets an extra message per reconciler pass; these
are dropped by one primary-key read. Publishing and consuming use separate connections, so a broker memory alarm that
blocks publishers does not stop consumers from acknowledging.

## Idempotency storage

Idempotency keys are a unique partial index on `jobs.idempotency_key` in PostgreSQL. `INSERT ... ON CONFLICT DO
NOTHING` decides the winner atomically, even for concurrent requests (8 concurrent submissions with one key create one
job in `JdbcJobRepositoryIT` and `JobApiIT`). The same key with the same request returns the original job (`200`,
`Idempotent-Replayed: true`); with a different request it returns `422`. Requests are compared by value (JSON key order
and number formatting are ignored, because jsonb normalises them). Keys never expire, which keeps the guarantee
unconditional but grows the index with the job table; a retention policy (for example 24 h as in public payment APIs)
would be added together with job archiving. With `taskplatform.redis.enabled=true` a Redis cache sits in front
of the index: a hit is used only if PostgreSQL confirms that the cached job id carries the same key, so a stale or
poisoned entry can neither create, hide nor change a job; a miss or a Redis failure takes the normal database path.

## Rate limiting

An optional fixed-window limit per client on `POST /v1/jobs` (client = `X-Client-Id` header, else remote address).
Counters live in Redis (an atomic Lua `INCR` + `PEXPIRE`) so instances share them, or in memory per instance when Redis
is disabled. Fixed windows are cheap and predictable but allow up to twice the limit around a window boundary; a
sliding window or token bucket would smooth that at the cost of more state. The limiter fails open: while Redis is
down there is no limit, because rejecting work because a cache is down would turn a non-authoritative dependency into
an availability risk. `X-Client-Id` separates cooperating clients but is not authentication.

## Clock source

All timestamps come from an injected `java.time.Clock` in the service, not from the database's `now()`. This makes
every time-dependent rule (lease expiry, timeouts, backoff, recovery delays) testable with a controllable clock and no
sleeping. With several service instances, clock skew between hosts shifts lease expiry; the `lease-grace` margin and
NTP-synchronised hosts cover small skew. Using the database clock for lease deadlines would remove the skew problem at
the cost of testability.

## Cancellation semantics

Cancelling a running job moves it to `CANCELLED` immediately with a version bump. The worker learns about it on its
next heartbeat (or when its completion is rejected), interrupts the handler and closes the delivery as `CANCELLED`.
The API answer is therefore final at once and a cancelled job can never complete, but the handler may keep running
for up to one heartbeat interval, and a handler that ignores interrupts keeps running in the background until it
returns (its result is discarded). A two-phase `CANCEL_REQUESTED` state would let clients see "still stopping", at the
cost of another state and an extra write.

## Retry policy

Capped exponential backoff with bounded ("equal-style") jitter: `base = min(cap, initial · multiplier^(n−1))`,
`delay ∈ [(1 − jitter) · base, base]`. Compared with full jitter (`[0, base]`) it spreads retries less, but every
delay is guaranteed to be positive and, while `(1 − jitter) · multiplier > 1`, strictly longer than the previous one,
which is what the tests check. Unknown exceptions, timeouts and lost leases are retryable; only
`NonRetryableTaskException` fails fast. Defaults: 1 s initial, ×2, 60 s cap, jitter 0.5, 3 attempts.

## Failed versus dead-lettered

Two terminal failure states: `FAILED` (the handler said the error is permanent) and `DEAD_LETTER` (retryable failures
used up every attempt). Both keep the last error and the full attempt history; they are separate so that operators can
tell "bad input" from "the dependency kept failing" and so that dead-letter counts are meaningful.

## In-memory queue as the default dispatcher

The default dispatcher is a per-queue in-process FIFO that drops ids already waiting. It needs no infrastructure and
is enough for one instance, because the database is the truth and the reconciler re-dispatches what a restart lost.
It cannot share work across instances; a broker adapter behind the same `JobDispatcher` interface does that.

## Real PostgreSQL in tests without Docker

SQL-dependent tests (CAS races, idempotent inserts, lease expiry, the HTTP tests) run against real PostgreSQL 17
started from embedded binaries (`io.zonky.test:embedded-postgres`), one server per test JVM and a fresh database per
test class. This keeps `./mvnw -B verify` independent of Docker and avoids H2 dialect differences. The embedded
binaries live only on the test classpath; the production image talks to a real PostgreSQL server.

## Default port 18080

The service listens on `127.0.0.1:18080` by default instead of 8080: on the development machine 8080 fell inside a
Windows (Hyper-V/WSL) excluded port range, and 18080 is less likely to collide with other local tools. `PORT` and
`BIND_ADDRESS` override it; the container binds `0.0.0.0` and compose publishes it on loopback only.
