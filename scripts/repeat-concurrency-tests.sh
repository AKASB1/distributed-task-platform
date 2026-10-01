#!/usr/bin/env bash
# Runs the thread-based test classes N times in a row (default 20) and stops at the first failing run.
# Usage: scripts/repeat-concurrency-tests.sh [runs]
set -uo pipefail
RUNS="${1:-20}"
UNIT="WorkerPoolTest,JobLifecycleTest,ReconcilerTest,InMemoryDispatcherTest,InMemoryRateLimiterTest,RedisConnectionHolderTest"
# the broker/cache ITs need Docker and skip themselves without it
IT="RaceIT,FailureInjectionIT,JdbcJobRepositoryIT,JobApiIT,RabbitMqDispatcherIT,RedisRateLimiterIT"
cd "$(dirname "$0")/.."
mkdir -p target
for i in $(seq 1 "$RUNS"); do
  start=$SECONDS
  if sh ./mvnw -B -q verify -Dtest="$UNIT" -Dit.test="$IT" -Dsurefire.failIfNoSpecifiedTests=false \
      -Dfailsafe.failIfNoSpecifiedTests=false > "target/repeat-run-$i.log" 2>&1; then
    echo "run $i/$RUNS: PASS ($((SECONDS - start)) s)"
  else
    echo "run $i/$RUNS: FAIL ($((SECONDS - start)) s) - see target/repeat-run-$i.log"
    exit 1
  fi
done
echo "all $RUNS runs passed"
