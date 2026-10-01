# Implementation plan

## 1. Job model

A strict, explicit state machine (`domain/JobState`, tested pair by pair):

```text
PENDING -> QUEUED -> RUNNING -> SUCCEEDED
   |         |  ^       |-> RETRY_WAIT -> QUEUED      (retryable failure, timeout, lost lease; attempts left)
   |         |  |       |-> FAILED                    (non-retryable failure)
   |         |  |       |-> DEAD_LETTER               (attempts exhausted)
   +---------+--+-------+-> CANCELLED                 (also from RETRY_WAIT)
```

Every state transition is persisted with a version number (`UPDATE jobs ... WHERE id = ? AND version = ?`).
Optimistic locking prevents two workers from completing the same job; the job version is the fencing token of an
attempt.

## 2. API layer

- `POST /v1/jobs` (optional `Idempotency-Key` header)
- `GET /v1/jobs/{id}`
- `POST /v1/jobs/{id}/cancel`
- `GET /v1/queues/{name}/stats`

The API writes job metadata first, then hands dispatch to the queue layer. Submission is idempotent when an
idempotency key is supplied (unique index in PostgreSQL; same key + same request → same job, different request →
422).

## 3. Dispatch and retry

Delivery is represented separately from job state: the `deliveries` table holds one row per attempt with attempt
number, lease owner, lease deadline and acknowledgement state (`LEASED`, `ACKED`, `NACKED`, `EXPIRED`, `CANCELLED`).

Retry policy:

- capped exponential backoff with bounded jitter (seedable)
- maximum attempt count (first attempt included)
- retryable vs non-retryable failure classes (`NonRetryableTaskException` fails fast; timeouts and lost leases are
  retryable)
- dead-letter state after exhaustion

A reconciler recovers jobs left in limbo: persisted but never enqueued, queued but never dispatched (or lost with the
in-memory queue), running with an expired lease, and retries whose backoff elapsed.

## 4. Worker protocol

Workers lease jobs, renew leases with heartbeats, report progress and outcomes through `worker/WorkerProtocol`. The
in-process implementation (`LocalWorkerProtocol`) calls the job lifecycle directly; a remote transport (gRPC) would
implement the same interface.

## 5. Storage

PostgreSQL is the source of truth. Redis is optional and not authoritative: it caches idempotency answers (a hit is
confirmed against PostgreSQL) and holds rate-limit counters, and every Redis failure fails open.

## 6. Observability

Tracked (Micrometer, Prometheus format at `/actuator/prometheus`):

- queue latency
- execution latency
- retry count
- worker utilization
- task success/failure rate
- stale leases

Logs are JSON lines carrying `jobId`, `attempt`, `queue` and `workerId`.

## 7. Delivery order

Minimum path, built first: steps 1–3, then 6 and 7, so the platform runs with retries, metrics, and Docker Compose.
Steps 4, 5, and 8 follow.

- [x] 1. single queue + one worker
- [x] 2. persistence and state machine
- [x] 3. retries and cancellation
- [x] 4. multiple worker pools (queue names, per-pool concurrency)
- [ ] 5. gRPC worker protocol (optional next step)
- [ ] 6. metrics/tracing — metrics done; tracing not done (optional next step)
- [x] 7. Docker Compose
- [x] 8. Kubernetes deployment and load test (manifests validated offline, not applied to a cluster)

## Evaluation and acceptance

- [x] failure-injection tests: kill a worker mid-job and observe lease expiry and retry; crash the dispatcher between
  persist and enqueue (`FailureInjectionIT`, `ReconcilerTest`, `scripts/crash-demo.sh`)
- [x] submission with the same idempotency key creates one job (`JobServiceTest`, `JdbcJobRepositoryIT`, `JobApiIT`)
- [x] no job completes twice when two workers race (optimistic-lock test on PostgreSQL, `RaceIT`)
- [x] a load test reports throughput and P50 / P95 / P99 queue latency, with hardware and configuration stated
  (`benchmarks/README.md`)
- [x] `docker compose up` brings up the local stack (app + PostgreSQL)

## Current state

The minimum path is implemented and tested on Java 21 / Spring Boot 3.5: versioned job state in PostgreSQL with
compare-and-set updates and Flyway migrations, the REST API with idempotency, leases with heartbeats and progress,
retries with backoff and jitter, timeouts, cancellation of queued and running jobs, dead-lettering, the reconciler, an
in-process worker pool per queue, Micrometer metrics, structured logs, a Maven Wrapper build, smoke scripts, a
Dockerfile and a compose file. `./mvnw -B verify` runs 570+ unit tests and 130+ integration tests against real
PostgreSQL (embedded binaries, no Docker needed). Tier 2 adds an optional RabbitMQ dispatcher, an
optional Redis idempotency cache and rate limit, multiple worker pools and a CI workflow; Kubernetes manifests are
validated statically with kubeconform (not applied to a cluster). Not done: gRPC workers and tracing.
