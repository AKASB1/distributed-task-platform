# Architecture

The platform accepts jobs over HTTP, stores them in PostgreSQL, hands their ids to workers through a dispatcher, and
tracks every attempt with a lease. PostgreSQL is the only source of truth: queues, caches and worker memory may lose
information at any time, and a reconciler repairs the gaps from the database.

## Modules

```text
io.akasb.taskplatform
├── domain         job model and rules, no framework code
│     JobState (transition table), Job (immutable, versioned), Delivery + AckState (one row per attempt),
│     Lease, RetryPolicy (backoff + jitter), FailureKind, TaskFailure
├── persistence    JobRepository (compare-and-set contract), JdbcJobRepository (PostgreSQL), InMemoryJobRepository
├── dispatch       no Spring types
│     JobDispatcher (transport SPI), InMemoryDispatcher, JobLifecycle (all state changes), Reconciler,
│     Checkpoints (crash injection), results and exceptions
│     └── rabbitmq   RabbitMqDispatcher (plain amqp-client; durable queue per job queue, manual hand-off acks)
├── worker         WorkerProtocol (worker ↔ control plane), LocalWorkerProtocol, WorkerPool, TaskHandler API,
│                  handlers/ (sleep, flaky)
├── api            JobService (framework-free application service), JobController, DTOs, RFC 7807 errors,
│                  IdempotencyCache / RateLimiter interfaces, RateLimitFilter, RequestBodyLimitFilter
├── cache          RedisIdempotencyCache, RedisRateLimiter (Lettuce, lazy connection, fail open)
├── observability  LifecycleListener (events), MicrometerJobMetrics, JobMdc (log context), Latency
└── config         Spring wiring: properties, beans, SmartLifecycle runners for reconciler and worker pools
```

Dependency direction: `api → dispatch → persistence → domain`, `worker → dispatch`, everything may use
`observability` (which only depends on `domain`). `domain` and `dispatch` import no Spring classes; `persistence`
uses Spring JDBC only inside `JdbcJobRepository`; `config` is the only package that knows about all others.

## Data model

```text
jobs                                         deliveries (one row per attempt)
─────────────────────────────                ─────────────────────────────────────
id (uuid, pk)                                job_id, attempt (pk)
queue, type, payload (jsonb)                 lease_owner, lease_deadline
state, version                ◄── CAS ──►    ack_state  LEASED | ACKED | NACKED | EXPIRED | CANCELLED
attempts, max_attempts, timeout_ms           queued_at, started_at, heartbeat_at, finished_at
idempotency_key (unique, partial index)      progress (0–100), error
created_at, updated_at, ready_at,
dispatched_at, next_attempt_at, finished_at,
last_error, result (jsonb)
```

Schema migrations: Flyway, `src/main/resources/db/migration/V1__jobs_and_deliveries.sql`. Partial indexes back the
reconciler scans (`state = 'RETRY_WAIT'`, `'PENDING'`, `'QUEUED'`, open leases).

Rules every writer follows:

- every change to a job row bumps `version` and is written with `UPDATE jobs ... WHERE id = ? AND version = ?`;
  0 rows updated means another writer won, and the caller re-reads instead of overwriting. The one exception is the
  dispatch timestamp `dispatched_at` of a `QUEUED` job, which the reconciler refreshes on re-dispatch without a
  version bump (it is bookkeeping, not state, and a claim racing with it is still decided by the version);
- a change that touches a job and its delivery (claim, finishing an attempt, reclaiming a lease) runs in one
  transaction, job row first;
- lease renewals write only the delivery row, so the job version stays fixed for the whole attempt and works as a
  fencing token: a worker whose lease was reclaimed, or whose job was cancelled, cannot complete it.

## State machine

```text
            submit                   claim (lease)
 (new) ──► PENDING ──► QUEUED ───────────────────────► RUNNING ──► SUCCEEDED
              │          │  ▲                            │
              │          │  │ backoff elapsed             ├──► RETRY_WAIT ──┐   retryable failure, timeout,
              │          │  └─────────────────────────────┼─────────────────┘   lost lease, attempts left
              │          │                                ├──► FAILED          non-retryable failure
              │          │                                ├──► DEAD_LETTER     attempts exhausted
              ▼          ▼                                ▼
           CANCELLED ◄───────────────────────────────── CANCELLED ◄── (also from RETRY_WAIT)
```

The table lives in `JobState` and is tested pair by pair (`JobStateTest`, 64 cases): `PENDING → QUEUED | CANCELLED`,
`QUEUED → RUNNING | CANCELLED`, `RUNNING → SUCCEEDED | RETRY_WAIT | FAILED | DEAD_LETTER | CANCELLED`,
`RETRY_WAIT → QUEUED | CANCELLED`; `SUCCEEDED`, `FAILED`, `DEAD_LETTER` and `CANCELLED` are terminal.

## Data flow

1. **Submit** (`POST /v1/jobs`). `JobService` validates the request and applies defaults. `JobLifecycle.submit`
   inserts the job as `PENDING` (an `ON CONFLICT DO NOTHING` on the idempotency key returns the existing job for a
   replay), moves it to `QUEUED` with a CAS, then calls `JobDispatcher.dispatch(queue, id)`.
2. **Claim.** A worker slot polls the dispatcher for an id and calls `claim`: if the job is still `QUEUED`, one
   transaction moves it to `RUNNING` (attempts + 1, version + 1) and inserts the delivery with
   `lease_deadline = now + leaseDuration`. Losers of the race, and stale ids, are dropped.
3. **Execute.** The slot runs the `TaskHandler` on a virtual thread and supervises it: every `heartbeatInterval` it
   renews the lease and sends the handler's progress; it enforces the job's timeout; it interrupts the handler when
   the heartbeat answers `CANCELLED` (job cancelled) or `LOST` (lease reclaimed).
4. **Finish.** Success → `SUCCEEDED` + delivery `ACKED`. Failure → `RetryPolicy.decide`: `RETRY_WAIT` with
   `next_attempt_at = now + backoff`, `FAILED` for non-retryable errors, or `DEAD_LETTER` when the attempt was the
   last one; the delivery becomes `NACKED`. Each outcome is one CAS transaction.
5. **Retry.** The reconciler moves due `RETRY_WAIT` jobs back to `QUEUED` and dispatches them.

## Failure handling

| Failure | Detection | Recovery |
|---|---|---|
| Process dies between insert and enqueue | `PENDING` older than `pending-grace` (5 s) | reconciler moves it to `QUEUED` and dispatches (`FailureInjectionIT`, `ReconcilerTest`) |
| Process dies after `QUEUED`, before dispatch, or the in-memory queue is lost on restart | `QUEUED` with `dispatched_at` older than `redispatch-after` (60 s); at startup every `QUEUED` job (non-durable dispatcher) | reconciler re-dispatches; duplicates are harmless because claims are CAS |
| Worker crashes or stalls mid-job | open delivery with `lease_deadline < now − lease-grace` | reconciler marks the delivery `EXPIRED` and retries or dead-letters the job; failover time ≈ lease + grace + scan interval (`FailureInjectionIT`, `scripts/crash-demo.sh`) |
| Zombie worker reports after its lease was reclaimed | job version changed | completion/failure/heartbeat rejected (`LEASE_LOST`); the new attempt's result stands (`RaceIT`) |
| Two workers race for one job | CAS on version in one transaction with the delivery insert | exactly one lease (`RaceIT`, 50 jobs × 8 claimers) |
| Handler exceeds its timeout | slot compares the injected clock with `startedAt + timeout` | handler interrupted, attempt `NACKED` with `TIMEOUT`, retried or dead-lettered |
| Job cancelled while running | next heartbeat (or the rejected completion) sees the new version | handler interrupted, delivery `CANCELLED`; the job is never completed |
| Handler throws a non-retryable error | `NonRetryableTaskException` | `FAILED` without retries |
| Retryable failures keep happening | attempt counter (incremented on every lease, including after expiry) | `DEAD_LETTER` after `maxAttempts` |
| Dispatcher unavailable during submit | exception from `dispatch` | logged; the job stays `QUEUED` and is re-dispatched by the reconciler |
| Concurrent reconcilers (several instances) | every step is a CAS | each lease is expired and each retry released exactly once (`RaceIT`) |
| RabbitMQ unreachable or connection lost | connect failure / automatic recovery | the service still starts; `dispatch` fails fast and the job stays `QUEUED` until the reconciler re-dispatches it; buffered unacked deliveries return to the broker (`RabbitMqDispatcherIT`) |
| Redis unreachable | command error or timeout | idempotency falls back to PostgreSQL, rate limiting is skipped (fail open), `taskplatform_redis_errors_total` counts it, health stays UP (`RedisEndToEndIT`) |
| Oversized or unstorable submission | body size / payload content check | `413` before parsing; `400` for U+0000 or unpaired surrogates that jsonb cannot store (`RequestLimitsIT`) |

Timestamps come from an injected `java.time.Clock` (truncated to microseconds, the PostgreSQL precision), which makes
lease expiry and timeouts testable without sleeping.

## Observability

- **Metrics** (Micrometer → `/actuator/prometheus`): queue latency and execution latency histograms, retries by
  reason, finished jobs by outcome (success/failure rate), attempts by outcome, stale leases reclaimed, reconciler
  recoveries, worker utilization/busy/slots/busy-time per pool, dispatcher queue depth.
- **Logs**: JSON lines (Spring Boot structured logging, Logstash layout) with MDC fields `jobId`, `attempt`, `queue`,
  `workerId` on every lifecycle and worker log line.

## Runtime topology (default)

One process runs the REST API, the reconciler (single scheduled thread) and one in-process worker pool per configured
queue; PostgreSQL runs next to it (Docker Compose, or embedded binaries for local runs and tests). Several instances
can share one database: claims, completions and reconciler steps are all compare-and-set. With the default in-memory
dispatcher each instance's queue only feeds its own workers; with `taskplatform.dispatcher.type=rabbitmq` all
instances publish to and consume from one durable RabbitMQ queue per job queue, and the broker spreads the work.
Redis, when enabled, only caches idempotency answers and holds rate-limit counters.
