# Implementation plan

## 1. Job model

Start with a strict state machine:

```text
PENDING -> QUEUED -> RUNNING -> SUCCEEDED
                     |   |
                     |   -> FAILED -> RETRY_WAIT -> QUEUED
                     -> CANCELLED
```

Persist every state transition with a version number. Use optimistic locking to prevent two workers from completing the same job.

## 2. API layer

Implement:

- `POST /v1/jobs`
- `GET /v1/jobs/{id}`
- `POST /v1/jobs/{id}/cancel`
- `GET /v1/queues/{name}/stats`

The API writes job metadata first, then hands dispatch to the queue layer. Submission must be idempotent when an idempotency key is supplied.

## 3. Dispatch and retry

Represent delivery separately from job state. A delivery has an attempt number, lease owner, lease deadline, and acknowledgement state.

Initial retry policy:

- exponential backoff with jitter
- maximum attempt count
- retryable vs non-retryable failure classes
- dead-letter queue after exhaustion

## 4. Worker protocol

Workers register capabilities, poll or consume jobs, renew leases, and report progress. gRPC is used for worker-control-plane communication.

## 5. Storage

PostgreSQL is the source of truth. Redis is not authoritative; use it only for short-lived coordination, rate limits, and cacheable counters.

## 6. Observability

Track at least:

- queue latency
- execution latency
- retry count
- worker utilization
- task success/failure rate
- stale leases

## 7. Delivery order

1. single queue + one worker
2. persistence and state machine
3. retries and cancellation
4. multiple worker pools
5. gRPC worker protocol
6. metrics/tracing
7. Docker Compose
8. Kubernetes deployment and load test

## Scaffold checkpoint

The state model and local submission path are implemented. Optimistic locking is represented by a version check in memory; durable transactions, HTTP/gRPC, retry delivery, and workers remain planned.
