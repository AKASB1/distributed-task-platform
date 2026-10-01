# Load test result

## Environment

- CPU: Intel(R) Core(TM) i9-14900KF
- logical processors: 32
- RAM: 96 GB
- OS: Windows 11 10.0 (amd64)
- Java: Eclipse Adoptium 21.0.12.1+1-LTS
- JVM max heap: 24544 MB

## Configuration

- jobs: 2000
- queue: bench
- submitter threads: 8
- maxAttempts per job: 3
- workload seed: 42
- submission: burst (as fast as possible)
- transient jobs: first attempt fails, later attempts fail with p=0.3
- service: in-process (same JVM as the load generator), embedded PostgreSQL 17 on the same machine
- worker pool: 1 pool, 8 slots
- lease / heartbeat: 30 s / 10 s (defaults)
- retry policy: initial 1 s, x2, cap 60 s, jitter 0.5, seed 42 (defaults + seed)
- reconciler interval: 100 ms
- dispatcher: in-memory

## Workload (generated)

- sizes: {long=105, medium=515, short=1380}
- failure modes: {none=1787, permanent=20, transient=193}

## Results

| Metric | Value |
|---|---|
| jobs submitted | 2000 |
| drained before timeout | true |
| final states | {DEAD_LETTER=14, FAILED=20, SUCCEEDED=1966} |
| makespan (first submit to last finish) | 46.1 s |
| submission phase | 1.6 s |
| throughput (terminal jobs / makespan) | 43.4 jobs/s |
| throughput (succeeded jobs / makespan) | 42.7 jobs/s |
| attempts | 2249 |
| retries (attempts - jobs) | 249 |
| dead-lettered jobs | 14 |
| failed (non-retryable) jobs | 20 |
| jobs acknowledged twice | 0 |
| queue latency P50 / P95 / P99 (all attempts) | 19539.2 / 37014.6 / 38628.0 ms (n=2249) |
| queue latency P50 / P95 / P99 (first attempts) | 19732.0 / 37105.3 / 38628.0 ms (n=2000) |
| execution latency P50 / P95 / P99 (all attempts) | 46.1 / 312.0 / 1793.5 ms (n=2249) |
| execution latency P50 / P95 / P99 (short) | 44.8 / 61.8 / 63.1 ms (n=1550) |
| execution latency P50 / P95 / P99 (medium) | 205.3 / 298.1 / 311.6 ms (n=590) |
| execution latency P50 / P95 / P99 (long) | 1435.0 / 1937.0 / 1966.7 ms (n=109) |
| submit request latency P50 / P95 / P99 | 3.9 / 5.2 / 9.4 ms (n=2000) |

Queue latency = delivery.startedAt - delivery.queuedAt (time from becoming eligible to being leased; for retries it starts when the backoff elapsed). Execution latency = delivery.finishedAt - delivery.startedAt (lease to acknowledgement). Percentiles use the nearest-rank method.
