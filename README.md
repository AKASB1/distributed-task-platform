# Distributed Task Platform

A distributed backend for long-running and asynchronous compute jobs: reliable submission, dispatch, retries, worker
execution and job state tracking, built to stay correct under failure.

**Status:** local MVP, benchmarked. The minimum path runs end to end on Java 21 / Spring Boot 3.5 with PostgreSQL,
is covered by unit, integration, concurrency and failure-injection tests, runs in Docker Compose (optionally with
RabbitMQ and Redis), and has a small documented load test. Kubernetes manifests are validated statically (not applied
to a cluster). OpenTelemetry tracing and a gRPC worker protocol are not done (see [TODO](#todo)).

## Available now

- **REST API**: `POST /v1/jobs` (with `Idempotency-Key`), `GET /v1/jobs/{id}` (with attempt history),
  `POST /v1/jobs/{id}/cancel`, `GET /v1/queues/{name}/stats`; RFC 7807 problem responses for invalid input.
- **Versioned job state in PostgreSQL**: explicit state machine (`PENDING`, `QUEUED`, `RUNNING`, `RETRY_WAIT`,
  `SUCCEEDED`, `FAILED`, `DEAD_LETTER`, `CANCELLED`), every state change a compare-and-set
  (`UPDATE ... WHERE id = ? AND version = ?`), Flyway migrations.
- **Deliveries separate from job state**: one row per attempt with lease owner, lease deadline and acknowledgement
  state; leases renewed by heartbeats that also carry progress.
- **Retries**: capped exponential backoff with bounded, seedable jitter; maximum attempts; retryable vs non-retryable
  failures; dead-letter after exhaustion; per-attempt timeouts; cancellation of queued and running jobs.
- **Reconciler**: recovers jobs persisted but never enqueued, queued but never dispatched (or lost with the in-memory
  queue on restart), running with an expired lease (crashed worker), and due retries.
- **Workers**: in-process worker pools (one per queue, configurable concurrency) behind a `WorkerProtocol`
  interface; pluggable `TaskHandler` API with two sample handlers (`sleep`, `flaky`).
- **Dispatcher**: in-memory by default, behind the `JobDispatcher` interface; optional **RabbitMQ** adapter
  (durable queues, manual acks, lazy connect with automatic recovery) for sharing work across instances.
- **Redis (optional, never authoritative)**: idempotency-answer cache in front of PostgreSQL and per-client rate
  limiting of submissions (`429` with `Retry-After`); both fail open when Redis is unavailable.
- **Observability**: Micrometer metrics in Prometheus format (queue latency, execution latency, retries, worker
  utilization, success/failure counts, stale leases); JSON logs with `jobId`, `attempt`, `queue`, `workerId`.
- **Run path**: Maven Wrapper, one command to build and test, one command to start the service without Docker
  (embedded PostgreSQL), smoke scripts for PowerShell and shell, multi-stage Dockerfile, Docker Compose stack (with
  optional RabbitMQ and Redis profiles), a crash-recovery demo, a small load test, and a GitHub Actions workflow
  (`.github/workflows/ci.yml`, JDK 21, `./mvnw -B verify`).
- **Kubernetes manifests** (`deploy/k8s`, kustomize): PostgreSQL StatefulSet, the service with probes, resources and a
  non-root read-only security context, optional RabbitMQ/Redis components; validated offline with kubeconform
  (`scripts/validate-k8s.sh`), not applied to a cluster.

## Requirements

- JDK 21 (`java` on `PATH` or `JAVA_HOME` set). Maven is downloaded by the wrapper.
- Optional: Docker with Compose v2 for the container stack.
- Tests and the local run need neither Docker nor an installed PostgreSQL: they start PostgreSQL 17 from embedded
  binaries (downloaded from Maven Central on first use).

## Build and test

Windows PowerShell:

```powershell
.\mvnw.cmd -B verify
```

Linux / macOS:

```bash
./mvnw -B verify
```

If a checkout has lost the executable bit (ZIP download, web-uploaded repository), run `chmod +x mvnw scripts/*.sh`
once, or prefix the commands with `sh` / `bash` (for example `sh ./mvnw -B verify`).

This runs the unit tests (surefire, `*Test`) and the integration tests (failsafe, `*IT`: real PostgreSQL, the HTTP
API on a random port, races and failure injection). The RabbitMQ and Redis integration tests use Testcontainers and
skip themselves when Docker is not available; everything else needs no Docker. The concurrency test classes can be
repeated with `scripts/repeat-concurrency-tests.ps1` / `scripts/repeat-concurrency-tests.sh` (20 consecutive runs by
default).

## Run

### Locally, without Docker

Starts an embedded PostgreSQL (data kept in `tmp/dev-postgres`) and the service on `http://127.0.0.1:18080`:

```powershell
.\mvnw.cmd spring-boot:test-run
```

```bash
./mvnw spring-boot:test-run
```

In a second terminal, run the smoke test of the main flow (health, submit, idempotent replay, validation errors,
retry, permanent failure, cancelling a running job, stats, metrics):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\smoke.ps1
```

```bash
./scripts/smoke.sh
```

Stop the service with Ctrl+C in its terminal, or from PowerShell:

```powershell
Invoke-RestMethod -Method Post -ContentType application/json http://127.0.0.1:18080/actuator/shutdown
```

(The shutdown endpoint is enabled only by this local launcher, which binds to 127.0.0.1.)

### With an existing PostgreSQL

Any PostgreSQL 14+ works; the schema is created by Flyway at startup. For example with the PostgreSQL container of the
compose file (published on `127.0.0.1:15432`):

```bash
docker compose up -d postgres
./mvnw -B package -DskipTests
java -jar target/distributed-task-platform-0.1.0-SNAPSHOT.jar --spring.datasource.url=jdbc:postgresql://127.0.0.1:15432/taskplatform --spring.datasource.username=taskplatform --spring.datasource.password=taskplatform-dev
```

### Docker Compose

```bash
docker compose up -d --build
./scripts/smoke.sh
./scripts/crash-demo.sh
docker compose down
```

With RabbitMQ as dispatcher and Redis for the idempotency cache and rate limits:

```bash
TASKPLATFORM_DISPATCHER_TYPE=rabbitmq TASKPLATFORM_REDIS_ENABLED=true TASKPLATFORM_RATE_LIMIT_ENABLED=true docker compose --profile broker --profile cache up -d --build
./scripts/smoke.sh
docker compose --profile broker --profile cache down
```

See [deploy/README.md](deploy/README.md) for ports, profiles and the crash-recovery demo.

## API examples

```bash
# submit (201 Created; the same Idempotency-Key and body later returns 200 with the same job)
curl -s -X POST http://127.0.0.1:18080/v1/jobs -H "Content-Type: application/json" -H "Idempotency-Key: demo-1" -d '{"type":"sleep","payload":{"durationMs":500}}'

# job with its attempt history (replace the id)
curl -s http://127.0.0.1:18080/v1/jobs/4f1c2e1a-0000-0000-0000-000000000000

# a job that fails once and is retried, and one that fails permanently
curl -s -X POST http://127.0.0.1:18080/v1/jobs -H "Content-Type: application/json" -d '{"type":"flaky","maxAttempts":3,"payload":{"durationMs":50,"failAttempts":1}}'
curl -s -X POST http://127.0.0.1:18080/v1/jobs -H "Content-Type: application/json" -d '{"type":"flaky","payload":{"failure":"permanent"}}'

# cancel (queued or running; 409 for finished jobs)
curl -s -X POST http://127.0.0.1:18080/v1/jobs/4f1c2e1a-0000-0000-0000-000000000000/cancel

# queue statistics: counts per state and the age of the oldest queued job
curl -s http://127.0.0.1:18080/v1/queues/default/stats
```

Request body of `POST /v1/jobs`:

| Field | Required | Default | Notes |
|---|---|---|---|
| `type` | yes | – | a registered handler: `sleep` or `flaky` |
| `queue` | no | `default` | `^[a-z0-9][a-z0-9_-]{0,63}$`, must be served by a worker pool |
| `payload` | no | `{}` | JSON object, at most 64 KiB |
| `maxAttempts` | no | 3 | 1–20, first attempt included |
| `timeoutMs` | no | 30000 | execution timeout per attempt, at most 1 h |

Sample handlers: `sleep` (`durationMs`) always succeeds; `flaky` (`durationMs`, `failAttempts`, `failure` =
`retryable` | `permanent` | `hang`, optional `failProbability` + `failSeed`) fails in a configurable, deterministic
way. New handlers implement `io.akasb.taskplatform.worker.TaskHandler` and are registered as Spring beans.

Responses: `201` created, `200` replayed (`Idempotent-Replayed: true`), read, or cancelled (also when the job was
already cancelled), `400` invalid input, `404` unknown job, `409` cancelling a job that already ended `SUCCEEDED`,
`FAILED` or `DEAD_LETTER`, `413` request body larger than the payload limit + 16 KiB, `415` wrong content type,
`422` idempotency key reused with a different request, `429` rate limit exceeded (only when enabled; with
`Retry-After`). Error bodies are RFC 7807 problem documents.

## Configuration

`src/main/resources/application.yml` holds every setting with its default; override with environment variables
(Spring relaxed binding, e.g. `TASKPLATFORM_LEASE_DURATION=10s`) or `--name=value` arguments.
[configs/example.properties](configs/example.properties) lists the most useful ones.

| Setting | Default | Meaning |
|---|---|---|
| `PORT`, `BIND_ADDRESS` | 18080, 127.0.0.1 | HTTP listener |
| `spring.datasource.url` / `username` / `password` | local PostgreSQL | database (source of truth) |
| `taskplatform.lease-duration` | 30s | lease validity without a heartbeat |
| `taskplatform.heartbeat-interval` | 10s | lease renewal and progress reporting interval |
| `taskplatform.retry.initial-delay` / `multiplier` / `max-delay` / `jitter` | 1s / 2.0 / 60s / 0.5 | backoff |
| `taskplatform.retry.default-max-attempts` | 3 | when a request does not say |
| `taskplatform.reconciler.interval` / `lease-grace` / `pending-grace` / `redispatch-after` | 1s / 2s / 5s / 60s | recovery timing |
| `taskplatform.workers.pools[i].name` / `queue` / `concurrency` | `default` / `default` / 4 | worker pools |
| `taskplatform.dispatcher.type` | `in-memory` | job id transport: `in-memory` or `rabbitmq` |
| `taskplatform.rabbitmq.host` / `port` / `username` / `password` / `virtual-host` / `queue-prefix` / `prefetch` | 127.0.0.1 / 5672 / guest / guest / `/` / `taskplatform.` / 16 | RabbitMQ dispatcher (pass the password through the environment) |
| `taskplatform.redis.enabled` / `uri` / `timeout` / `idempotency-ttl` | false / `redis://127.0.0.1:6379` / 500ms / 24h | optional Redis (cache + rate-limit counters) |
| `taskplatform.rate-limit.enabled` / `requests-per-window` / `window` | false / 100 / 1s | per-client limit on `POST /v1/jobs` (Redis counters if Redis is enabled, else per instance) |
| `LOG_FORMAT` | `logstash` | JSON log layout (`logstash`, `ecs`, `gelf`) |

## Metrics

`GET /actuator/prometheus` (also `/actuator/health`, `/actuator/metrics`):

| Metric | Type | Labels |
|---|---|---|
| `taskplatform_job_queue_latency_seconds` | histogram | `queue` |
| `taskplatform_job_execution_latency_seconds` | histogram | `queue`, `outcome` (acked, nacked, expired) |
| `taskplatform_job_retries_total` | counter | `queue`, `reason` |
| `taskplatform_jobs_finished_total` | counter | `queue`, `outcome` (succeeded, failed, dead_letter, cancelled) — success/failure rate |
| `taskplatform_job_attempts_total` | counter | `queue`, `outcome` |
| `taskplatform_jobs_submitted_total` | counter | `queue` |
| `taskplatform_leases_expired_total` | counter | `queue` — stale leases reclaimed |
| `taskplatform_reconciler_recovered_total` | counter | `kind` |
| `taskplatform_worker_utilization` | gauge | `pool`, `queue` — busy slots / slots |
| `taskplatform_worker_busy`, `taskplatform_worker_slots` | gauge | `pool` |
| `taskplatform_worker_busy_time_seconds_total` | counter | `pool` |
| `taskplatform_queue_depth` | gauge | `queue` (-1 if the dispatcher cannot tell) |
| `taskplatform_redis_idempotency_lookups_total` | counter | `result` (hit, miss) — only with Redis |
| `taskplatform_redis_errors_total` | counter | `operation` — Redis failures answered by failing open |
| `taskplatform_ratelimit_rejected_total` | counter | — submissions rejected with 429 |

## Benchmarks

A small load test (2,000 mixed jobs, 8 workers, one machine) is described with its full configuration, hardware and
raw output in [benchmarks/README.md](benchmarks/README.md). Run it with `./mvnw -B -Pload test`.

## Repository layout

```text
src/main/java/io/akasb/taskplatform/
  api/            REST endpoints, JobService (validation, idempotency), error mapping
  domain/         job model, state machine, deliveries, retry policy (no framework code)
  dispatch/       JobDispatcher, in-memory queue, JobLifecycle, Reconciler (no Spring types)
  dispatch/rabbitmq/  RabbitMQ JobDispatcher adapter (plain amqp-client)
  cache/          Redis idempotency cache and rate limiter (Lettuce), fail open
  persistence/    JobRepository contract, PostgreSQL (JDBC) and in-memory implementations
  worker/         worker protocol, worker pool, task handler API, sample handlers
  observability/  lifecycle events, Micrometer metrics, log context
  config/         Spring wiring and properties
src/main/resources/db/migration/   Flyway schema
src/test/java/...                  unit tests, *IT integration tests, bench/ load test, LocalDevApplication
scripts/          smoke tests, crash demo, repeated concurrency runs
benchmarks/       load-test notes and raw results
deploy/           deployment notes, Kubernetes manifests (k8s/, k8s-overlays/)
docs/             architecture and design trade-offs
```

More: [docs/architecture.md](docs/architecture.md) (modules, data flow, state machine, failure handling),
[docs/design-tradeoffs.md](docs/design-tradeoffs.md), [IMPLEMENTATION.md](IMPLEMENTATION.md) (delivery checklist).

## TODO

- gRPC worker protocol and separate worker processes (workers run in-process behind `WorkerProtocol`) — optional next
  step.
- OpenTelemetry tracing — optional next step.
- The Kubernetes manifests were validated with kubeconform only, never applied to a cluster.
- Work sharing across instances needs the RabbitMQ dispatcher: with the default in-memory dispatcher several
  instances may share one database safely, but each instance's queue only feeds its own workers.
- The RabbitMQ dispatcher has functional and failure tests but was not load-tested.
- Idempotency-key expiry and job archiving.
- Authentication and authorization on the API.

## Reference projects

- [temporalio/temporal](https://github.com/temporalio/temporal) — durable execution, activity timeouts, heartbeats
- [celery/celery](https://github.com/celery/celery) — task queues, retries, routing, worker model
- [rq/rq](https://github.com/rq/rq) — a deliberately small queue/worker design
- [kagkarlsson/db-scheduler](https://github.com/kagkarlsson/db-scheduler) — database-backed leases and heartbeats in Java

They were read for design only; this repository implements a smaller system from first principles and contains no
code from them.

## License

MIT
