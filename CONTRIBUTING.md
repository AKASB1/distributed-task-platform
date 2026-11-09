# Contributing

This is a portfolio repository with one maintainer. Issues and pull requests are welcome; this page lists what a change needs before it is merged.

## Environment

- JDK 21. The Maven Wrapper (`mvnw`, `mvnw.cmd`) downloads Maven.
- No installed PostgreSQL is needed: the tests and the local run start PostgreSQL 17 from embedded binaries.
- Docker is optional: the RabbitMQ and Redis integration tests use Testcontainers and skip themselves without Docker.

## Checks

Run this before opening a pull request; CI runs the same build (`.github/workflows/ci.yml`) and then validates the compose file and builds the container image:

```bash
./mvnw -B verify
```

It runs the unit tests (surefire, `*Test`) and the integration tests (failsafe, `*IT`). A change to the concurrency or recovery code should also pass `scripts/repeat-concurrency-tests.sh` (or `.ps1`), which repeats the concurrency test classes 20 times by default. A change to the Kubernetes manifests should pass `scripts/validate-k8s.sh` (kubeconform).

## Results

The load-test results in the README and in [benchmarks/README.md](benchmarks/README.md) come from the committed raw output in `benchmarks/results/` (`summary.md`, `attempts.csv`, `prometheus.txt`). A change that should move them reruns `./mvnw -B -Pload test` (and the paced run) and commits the new output next to the old one, with the commit it was measured on.

## Diagrams and icon

The architecture diagram is drawn from `docs/diagrams/architecture.d2` with [D2](https://d2lang.com) (`bash scripts/render_diagrams.sh` or `powershell -ExecutionPolicy Bypass -File scripts\render_diagrams.ps1`); the committed SVG was drawn with D2 0.9.0. `python scripts/make_assets.py` writes the icon. CI needs neither.

## Style

- PostgreSQL is the only source of truth; every state change is a compare-and-set on the version column, and the dispatcher carries only job ids ([docs/architecture.md](docs/architecture.md)).
- `domain/` holds no framework code, and `dispatch/` no Spring types.
- Every failure mode gets a test that injects it (`FailureInjectionIT`, `RaceIT`), and every metric is documented in the README's metrics table.
