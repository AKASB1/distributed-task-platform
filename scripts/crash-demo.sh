#!/usr/bin/env bash
# Failure demo on the Docker Compose stack: SIGKILL the app while jobs are RUNNING and QUEUED, start it again, and
# check that every job still finishes exactly once (running ones via lease expiry + retry, queued ones via the
# startup re-dispatch of the reconciler).
# Usage: scripts/crash-demo.sh [base-url]   (stack must be up: docker compose up -d --build)
set -uo pipefail

BASE="${1:-http://127.0.0.1:18080}"
JOBS="${CRASH_DEMO_JOBS:-6}"
BODY="$(mktemp)"
trap 'rm -f "$BODY"' EXIT
curl_() { curl -s --noproxy 127.0.0.1,localhost -o "$BODY" -w '%{http_code}' "$@"; }
json_str() { grep -o "\"$1\":\"[^\"]*\"" "$BODY" | head -1 | cut -d'"' -f4; }

wait_health() {
  for _ in $(seq 1 120); do
    curl -s --noproxy 127.0.0.1,localhost "$BASE/actuator/health" | grep -q '"UP"' && return 0
    sleep 1
  done
  return 1
}

wait_health || { echo "service not healthy at $BASE"; exit 1; }
IDS=()
for i in $(seq 1 "$JOBS"); do
  curl_ -X POST -H 'Content-Type: application/json' --data '{"type":"sleep","payload":{"durationMs":5000}}' \
    "$BASE/v1/jobs" >/dev/null
  IDS+=("$(json_str id)")
done
echo "submitted ${#IDS[@]} jobs of 5 s each"
for _ in $(seq 1 30); do
  curl_ "$BASE/v1/queues/default/stats" >/dev/null
  grep -q '"RUNNING":[1-9]' "$BODY" && break
  sleep 0.5
done
echo "before kill: $(grep -o '"counts":{[^}]*}' "$BODY")"

grep -q '"RUNNING":[1-9]' "$BODY" || { echo "no job reached RUNNING before the kill"; exit 1; }
docker compose kill -s SIGKILL app >/dev/null 2>&1 || { echo "could not kill the app container"; exit 1; }
echo "app container killed (SIGKILL)"
docker compose start app >/dev/null 2>&1 || { echo "could not start the app container"; exit 1; }
echo "app container started"
wait_health || { echo "service did not come back"; exit 1; }
EXPIRED=0
START=$SECONDS

deadline=$((SECONDS + 180))
while [ $SECONDS -lt $deadline ]; do
  done_count=0
  for id in "${IDS[@]}"; do
    curl_ "$BASE/v1/jobs/$id" >/dev/null
    [ "$(json_str state)" = "SUCCEEDED" ] && done_count=$((done_count + 1))
  done
  [ "$done_count" -eq "${#IDS[@]}" ] && break
  sleep 1
done
echo "all succeeded ${done_count}/${#IDS[@]} after $((SECONDS - START)) s from restart"

FAIL=0
for id in "${IDS[@]}"; do
  curl_ "$BASE/v1/jobs/$id" >/dev/null
  acks=$(grep -o '"ackState":"ACKED"' "$BODY" | wc -l | tr -d ' ')
  states=$(grep -o '"ackState":"[A-Z]*"' "$BODY" | cut -d'"' -f4 | tr '\n' ' ')
  echo "job $id state=$(json_str state) deliveries=[ $states] acked=$acks"
  [ "$(json_str state)" = "SUCCEEDED" ] && [ "$acks" = "1" ] || FAIL=1
  grep -q '"ackState":"EXPIRED"' "$BODY" && EXPIRED=$((EXPIRED + 1))
done
# the kill must actually have interrupted running attempts: their first delivery ends EXPIRED
[ "$EXPIRED" -ge 1 ] || { echo "no attempt was reclaimed after the kill"; FAIL=1; }
if [ "$FAIL" -eq 0 ]; then echo "crash demo: PASS (every job finished exactly once)"; else echo "crash demo: FAIL"; fi
exit "$FAIL"
