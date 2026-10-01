# Deployment

## Docker Compose (local stack)

`docker-compose.yml` in the repository root runs the service and PostgreSQL 17. The image is built by the multi-stage
`Dockerfile` (Maven Wrapper on `eclipse-temurin:21-jdk`, runtime on `eclipse-temurin:21-jre`, non-root user). All
containers, the volume and the network carry the label `cvproject=distributed-task-platform`, and every published port
is bound to `127.0.0.1` only.

```bash
docker compose config --quiet
docker compose up -d --build
./scripts/smoke.sh
docker compose down
```

```powershell
docker compose config --quiet
docker compose up -d --build
powershell -ExecutionPolicy Bypass -File scripts\smoke.ps1
docker compose down
```

| Service | Image | Host port (override) | Notes |
|---|---|---|---|
| `app` | built from `Dockerfile` | `127.0.0.1:18080` (`APP_PORT`) | REST API, workers, reconciler, `/actuator/health`, `/actuator/prometheus` |
| `postgres` | `postgres:17-alpine` | `127.0.0.1:15432` (`POSTGRES_PORT`) | source of truth; data in the `pgdata` volume |

Configuration is passed as environment variables (Spring relaxed binding), for example `SPRING_DATASOURCE_URL`,
`TASKPLATFORM_LEASE_DURATION=10s` or `LOG_FORMAT=ecs`. A worker-pool list set through the environment replaces the
default list, so every field of every pool must be given, e.g. `TASKPLATFORM_WORKERS_POOLS_0_NAME=default`,
`TASKPLATFORM_WORKERS_POOLS_0_QUEUE=default`, `TASKPLATFORM_WORKERS_POOLS_0_CONCURRENCY=8`. The PostgreSQL
password defaults to a local development value; set `POSTGRES_PASSWORD` in a `.env` file (ignored by git) to change it.

`docker compose down -v` also removes the database volume.

### Optional broker and cache

Two compose profiles add RabbitMQ (`broker`) and Redis (`cache`). The app uses them only when told to:

```bash
TASKPLATFORM_DISPATCHER_TYPE=rabbitmq TASKPLATFORM_REDIS_ENABLED=true TASKPLATFORM_RATE_LIMIT_ENABLED=true docker compose --profile broker --profile cache up -d --build
./scripts/smoke.sh
./scripts/crash-demo.sh
docker compose --profile broker --profile cache down
```

| Service | Image | Host ports (override) | Used when |
|---|---|---|---|
| `rabbitmq` | `rabbitmq:4-management-alpine` | `127.0.0.1:15670` AMQP (`RABBITMQ_PORT`), `127.0.0.1:15671` management UI (`RABBITMQ_UI_PORT`) | `TASKPLATFORM_DISPATCHER_TYPE=rabbitmq` |
| `redis` | `redis:7-alpine` | `127.0.0.1:16379` (`REDIS_PORT`) | `TASKPLATFORM_REDIS_ENABLED=true` (cache) and `TASKPLATFORM_RATE_LIMIT_ENABLED=true` (shared counters) |

The app waits for these services when their profile is active and also connects lazily, so a broker or cache that
starts late (or restarts) does not stop it. With RabbitMQ the crash demo behaves the same, except that the queued jobs
are redelivered by the broker instead of being re-dispatched at startup (measured: all six jobs `SUCCEEDED` 27 s after
the restart).

### Failure demo

With the stack running, `scripts/crash-demo.sh` submits six 5-second jobs, kills the app container with `SIGKILL`
while four are running and two are queued, starts it again and checks that all six finish exactly once:

```bash
./scripts/crash-demo.sh
```

The four running jobs are recovered through lease expiry (their first delivery ends `EXPIRED`, the retry is `ACKED`);
the two queued ones are re-dispatched at startup because the in-memory queue did not survive the restart. On the
development machine all six were `SUCCEEDED` 36 s after the restart (lease 30 s + grace 2 s + backoff ≈ 1 s + the 5 s
job).

## Kubernetes

Plain manifests with a kustomization in [`k8s/`](k8s/) (namespace `taskplatform`):

| File | Content |
|---|---|
| `namespace.yaml` | namespace |
| `app-configmap.yaml` | non-secret settings as environment variables (database URL, leases, retries, reconciler, worker pool, dispatcher) |
| `kustomization.yaml` | resources, common labels, and a `secretGenerator` for the database password (a **local/dev default** literal, to be replaced in an overlay or by a secret manager) |
| `postgres-statefulset.yaml`, `postgres-service.yaml` | PostgreSQL 17 (`postgres:17-alpine`) with a 1 Gi volume claim, `pg_isready` probes, headless + ClusterIP services |
| `app-deployment.yaml` | the service: 1 replica, init container waiting for PostgreSQL, startup/readiness/liveness probes on `/actuator/health/{liveness,readiness}`, resources, non-root (uid 999), read-only root filesystem with an `emptyDir` at `/tmp`, all capabilities dropped, `RuntimeDefault` seccomp, 60 s termination grace, Prometheus scrape annotations |
| `app-service.yaml`, `app-pdb.yaml` | ClusterIP service on 18080, PodDisruptionBudget |
| `optional/rabbitmq`, `optional/redis` | kustomize components: RabbitMQ (and its password secret) with the app switched to the RabbitMQ dispatcher; Redis with the idempotency cache enabled |

One replica is the default because the in-memory dispatcher does not share work between pods (several pods would still
be safe, but each runs only the jobs submitted to it). [`k8s-overlays/rabbitmq-redis/`](k8s-overlays/rabbitmq-redis/)
is an example overlay that adds both components and runs two replicas. The image `distributed-task-platform:local`
must be available to the cluster's nodes (build it with `docker compose build app` or `docker build -t
distributed-task-platform:local .` and load or push it as your cluster requires).

```bash
kubectl kustomize deploy/k8s
kubectl apply -k deploy/k8s
```

Validation without a cluster (render with kustomize, then check every object against the Kubernetes 1.32 JSON
schemas with [kubeconform](https://github.com/yannh/kubeconform)):

```bash
KUBECONFORM=/path/to/kubeconform ./scripts/validate-k8s.sh
```

On the development machine both renders validated (`deploy/k8s`: 9 objects, overlay: 15 objects, 0 invalid). No
cluster was available, so the manifests were not applied; `kubectl apply --dry-run=client` also needs a reachable API
server for discovery and could not be used. The container-level settings were checked with Docker instead: the
PostgreSQL image with a read-only root filesystem as uid 70 and the service image read-only as uid 999 with all
capabilities dropped, connected to each other, passed `scripts/smoke.sh` (29/29).
