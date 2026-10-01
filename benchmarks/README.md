# Benchmarks

A small, local load test. It checks that the platform behaves correctly under a realistic mixed workload and shows
where time goes. It is not a capacity claim: the service, the load generator and PostgreSQL all run on one
workstation, and the numbers below hold only for the configuration stated next to them.

## How to run

One command (about 1–2 minutes with the defaults; capped by `load.timeoutSeconds`, default 270 s):

```bash
./mvnw -B -Pload test
```

```powershell
.\mvnw.cmd -B -Pload test
```

The `load` Maven profile runs only `src/test/java/io/akasb/taskplatform/bench/LoadTest.java`. It starts the service
in-process on an embedded PostgreSQL 17 (no Docker), submits the workload over HTTP on a random local port, waits
until every job is terminal, and writes the raw output to `benchmarks/results/<timestamp>-<jobs>jobs-<workers>workers-<mode>/`.

Parameters (Maven `-D` properties; in PowerShell quote them, e.g. `'-Dload.jobs=500'`):

| Property | Default | Meaning |
|---|---|---|
| `load.jobs` | 2000 | number of jobs |
| `load.workers` | 8 | worker slots in the pool |
| `load.ratePerSecond` | 0 | 0 = submit as fast as possible (burst); > 0 = paced, open-loop submission |
| `load.seed` | 42 | seed for the workload and the retry jitter |
| `load.maxAttempts` | 3 | attempts per job (first attempt included) |
| `load.submitters` | 8 | client threads submitting jobs |
| `load.timeoutSeconds` | 270 | give up waiting after this long |
| `load.baseUrl` | (empty) | target an already running service instead of starting one (use with `load.queue`) |
| `load.queue` | bench | queue name |

## Workload

Generated from one seeded `java.util.Random`, so every run with the same seed submits the same jobs:

- 70 % short jobs (10–50 ms), 25 % medium (100–300 ms), 5 % long (1–2 s), all `sleep`-style work;
- independently, about 10 % fail transiently: the first attempt always fails with a retryable error and every later
  attempt fails with probability 0.3 (decided from a per-job seed), so some of them exhaust their 3 attempts and are
  dead-lettered;
- about 1 % fail permanently (non-retryable error → `FAILED` after one attempt).

With seed 42 and 2,000 jobs the generator produced 1,380 short, 515 medium and 105 long jobs; 1,787 without
failures, 193 transient and 20 permanent.

## Metrics and how they are computed

All latencies come from the timestamps the service stores for every delivery (attempt), read back through
`GET /v1/jobs/{id}` after the run:

- **queue latency** = `startedAt − queuedAt` of a delivery: from the moment the job became eligible (submitted, or its
  retry backoff elapsed) until a worker leased it;
- **execution latency** = `finishedAt − startedAt`: both are stamped by the service, `startedAt` when the claim is
  made and `finishedAt` when the worker's report arrives, so it covers the claim transaction, the handler's work and
  the supervision loop, but not the completion write itself;
- **throughput** = terminal jobs / makespan, where makespan runs from the first submission to the last finish;
- **retries** = attempts − jobs; **dead-letter** and **failed** counts come from the final job states.

Percentiles use the nearest-rank method over all attempts (`n` is shown). The Prometheus counters scraped at the end
(`prometheus.txt`) agree with these totals (for example `taskplatform_job_retries_total` = 249 in both runs below).

## Results (2026-10-01)

These numbers were measured at commit 0246d52, before the review fixes and the Tier 2 additions (RabbitMQ, Redis,
request limits), and were not re-run afterwards.

Hardware and runtime: Intel Core i9-14900KF (24 cores / 32 threads), 96 GB RAM, Windows 11 (10.0.26100),
Eclipse Temurin 21.0.12.1, JVM default heap (max 24 GB). Service, generator and embedded PostgreSQL 17.11 on the same
machine. Service configuration: one worker pool with 8 slots, in-memory dispatcher, lease 30 s, heartbeat 10 s,
retry initial 1 s ×2 capped at 60 s with jitter 0.5 (seed 42), reconciler every 100 ms, 3 attempts per job.

| Metric | Burst (2,000 jobs, 8 workers) | Paced 30 jobs/s (2,000 jobs, 8 workers) |
|---|---|---|
| final states | 1,966 SUCCEEDED, 20 FAILED, 14 DEAD_LETTER | 1,966 SUCCEEDED, 20 FAILED, 14 DEAD_LETTER |
| jobs acknowledged twice | 0 | 0 |
| makespan | 46.1 s (submission took 1.6 s) | 68.8 s (submission took 66.6 s) |
| throughput (terminal jobs / makespan) | **43.4 jobs/s** | 29.1 jobs/s (= offered rate) |
| attempts / retries | 2,249 / 249 | 2,249 / 249 |
| queue latency P50 / P95 / P99 | 19,539 / 37,015 / 38,628 ms | **1.0 / 104 / 247 ms** |
| execution latency P50 / P95 / P99 (all) | 46.1 / 312.0 / 1,793.5 ms | 45.6 / 312.2 / 1,793.1 ms |
| execution latency P50 (short / medium / long) | 44.8 / 205.3 / 1,435.0 ms | 44.1 / 203.9 / 1,435.4 ms |
| submit request latency P50 / P95 / P99 | 3.9 / 5.2 / 9.4 ms | 2.6 / 4.7 / 7.1 ms |

Raw output: [`results/20261001-080652-2000jobs-8workers-burst/`](results/20261001-080652-2000jobs-8workers-burst/)
and [`results/20261001-080836-2000jobs-8workers-paced30/`](results/20261001-080836-2000jobs-8workers-paced30/)
(`summary.md`, `attempts.csv` with one row per attempt, `prometheus.txt`).

Commands that produced them:

```bash
./mvnw -B -Pload test
./mvnw -B -Pload test -Dload.ratePerSecond=30
```

## Reading the numbers

- **Burst.** All 2,000 jobs arrive within 1.6 s, while 8 slots can process roughly 43 jobs/s of this mix
  (mean work ≈ 0.15 s per attempt plus retries), so the queue latency mostly measures how long the backlog takes to
  drain. Throughput is bounded by the worker slots, not by the API or the database.
- **Paced below capacity.** At 30 jobs/s the queue stays almost empty: the median job waits about 1 ms between
  becoming eligible and being leased; the tail (P99 ≈ 0.25 s) appears when several long jobs occupy most slots at
  once.
- **Execution overhead.** Short jobs (nominal mean 30 ms) show a median of about 45 ms. Most of the difference is the
  Windows timer granularity (≈ 15.6 ms) on the handler's final sleep step plus the claim transaction;
  medium and long jobs land close to their nominal durations.
- **Correctness under load.** No job was acknowledged twice, every transient failure was retried with backoff, the
  14 jobs whose three attempts all failed were dead-lettered, and the 20 permanent failures went to `FAILED` without
  retries.

## Not measured

- Multiple service instances, a separate database host, or network latency between components.
- The RabbitMQ dispatcher under load (functional tests only).
- Long-running soak tests, memory growth, or database growth over time.
