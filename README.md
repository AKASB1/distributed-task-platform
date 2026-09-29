# Distributed Task Platform

A distributed backend for long-running and asynchronous compute jobs. The first version focuses on reliable task submission, dispatch, retries, worker execution, and job state tracking.

**Status:** implementation scaffold.

## Scope

- REST API for job submission and status queries
- gRPC channel between control plane and workers
- durable job state in PostgreSQL
- Redis for short-lived coordination, idempotency, and rate limiting
- RabbitMQ-backed task delivery
- retry policy, timeout handling, dead-letter flow, and cancellation
- worker heartbeats and lease-based ownership
- OpenTelemetry traces, Prometheus metrics, and structured logs
- Docker Compose for local development; Kubernetes manifests later

## Proposed stack

Java 21 · Spring Boot 3 · gRPC · PostgreSQL · Redis · RabbitMQ · OpenTelemetry · Docker · Kubernetes

## High-level design

```text
Client
  │
  ▼
REST API ──► Job Store (PostgreSQL)
  │
  ▼
Dispatcher ──► Queue (RabbitMQ) ──► Worker Pool
  │                                  │
  └────────► Retry / DLQ ◄───────────┘
                  │
                  ▼
        Metrics / Traces / Logs
```

## Repository layout

```text
src/main/java/io/akasb/taskplatform/
  api/          REST and gRPC endpoints
  domain/       job model and state machine
  dispatch/     queue dispatch and retry policy
  persistence/  PostgreSQL repositories
  worker/       worker protocol and lease handling
  observability/
src/test/
deploy/
docs/
```

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for the build order and module boundaries.

## Reference projects

- [temporalio/temporal](https://github.com/temporalio/temporal) — durable workflow execution and failure semantics
- [celery/celery](https://github.com/celery/celery) — task queues, retries, routing, and worker model
- [rq/rq](https://github.com/rq/rq) — a deliberately small queue/worker design that is useful for API comparison

These are references for architecture and operational behavior; this repository is intended to implement a smaller system from first principles.

## License

MIT

## Available now

A local Java 21 path has a versioned job model, state checks, in-memory repository and dispatcher, and a submission service. Run `mvn test`. External transports and stores are planned.
