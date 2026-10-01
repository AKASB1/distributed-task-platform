#!/usr/bin/env bash
# Smoke test of the main flow against a running service.
# Usage: scripts/smoke.sh [base-url]        (default http://127.0.0.1:18080)
# Env:   SMOKE_TIMEOUT seconds to wait for each job (default 60)
set -uo pipefail

BASE="${1:-${BASE_URL:-http://127.0.0.1:18080}}"
TIMEOUT="${SMOKE_TIMEOUT:-60}"
BODY="$(mktemp)"
HEADERS="$(mktemp)"
trap 'rm -f "$BODY" "$HEADERS"' EXIT
PASS=0
FAIL=0

ok() { echo "PASS  $1"; PASS=$((PASS + 1)); }
ko() { echo "FAIL  $1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else ko "$1 (expected $3, got $2)"; fi; }

# request METHOD PATH [JSON] [extra curl args...] -> prints status code; body in $BODY, headers in $HEADERS
request() {
  local method="$1" path="$2" data="${3:-}"
  shift 3 2>/dev/null || shift $#
  local args=(-s --noproxy 127.0.0.1,localhost -o "$BODY" -D "$HEADERS" -w '%{http_code}' -X "$method")
  if [ -n "$data" ]; then args+=(-H 'Content-Type: application/json' --data "$data"); fi
  curl "${args[@]}" "$@" "$BASE$path"
}

json_str() { grep -o "\"$1\":\"[^\"]*\"" "$BODY" | head -1 | cut -d'"' -f4; }

wait_state() { # id wanted-state -> 0 when reached
  local id="$1" want="$2" deadline=$((SECONDS + TIMEOUT)) state=""
  while [ $SECONDS -lt $deadline ]; do
    request GET "/v1/jobs/$id" "" >/dev/null
    state="$(json_str state)"
    [ "$state" = "$want" ] && return 0
    sleep 0.5
  done
  echo "      job $id is $state, wanted $want"
  return 1
}

echo "smoke test against $BASE"

code=$(request GET /actuator/health "")
check "health endpoint" "$code" 200
grep -q '"UP"' "$BODY" && ok "health is UP" || ko "health is UP"

KEY="smoke-$(date +%s)-$$"
code=$(request POST /v1/jobs '{"type":"sleep","payload":{"durationMs":200}}' -H "Idempotency-Key: $KEY")
check "submit sleep job -> 201" "$code" 201
ID="$(json_str id)"

code=$(request POST /v1/jobs '{"type":"sleep","payload":{"durationMs":200}}' -H "Idempotency-Key: $KEY")
check "same Idempotency-Key -> 200" "$code" 200
check "same Idempotency-Key -> same job" "$(json_str id)" "$ID"
grep -qi '^Idempotent-Replayed: true' "$HEADERS" && ok "Idempotent-Replayed header" || ko "Idempotent-Replayed header"

code=$(request POST /v1/jobs '{"type":"sleep","payload":{"durationMs":1}}' -H "Idempotency-Key: $KEY")
check "same key, different body -> 422" "$code" 422

code=$(request POST /v1/jobs '{"type":"does-not-exist"}')
check "unknown type -> 400" "$code" 400
code=$(request POST /v1/jobs '{"type":"sleep","maxAttempts":0}')
check "maxAttempts 0 -> 400" "$code" 400
code=$(request POST /v1/jobs '{not json')
check "malformed JSON -> 400" "$code" 400

wait_state "$ID" SUCCEEDED && ok "sleep job SUCCEEDED" || ko "sleep job SUCCEEDED"

code=$(request POST /v1/jobs '{"type":"flaky","payload":{"durationMs":20,"failAttempts":1}}')
FLAKY="$(json_str id)"
check "submit flaky job -> 201" "$code" 201
wait_state "$FLAKY" SUCCEEDED && ok "flaky job retried and SUCCEEDED" || ko "flaky job retried and SUCCEEDED"
check "flaky job has 2 attempts" "$(grep -o '"attempt":[0-9]*' "$BODY" | wc -l | tr -d ' ')" 2

code=$(request POST /v1/jobs '{"type":"flaky","payload":{"failure":"permanent","failAttempts":9}}')
PERM="$(json_str id)"
wait_state "$PERM" FAILED && ok "permanent failure -> FAILED" || ko "permanent failure -> FAILED"

code=$(request POST /v1/jobs '{"type":"flaky","timeoutMs":120000,"payload":{"failure":"hang","failAttempts":9}}')
HANG="$(json_str id)"
wait_state "$HANG" RUNNING && ok "hanging job RUNNING" || ko "hanging job RUNNING"
code=$(request POST "/v1/jobs/$HANG/cancel" "")
check "cancel running job -> 200" "$code" 200
check "cancelled job state" "$(json_str state)" CANCELLED
deadline=$((SECONDS + TIMEOUT)); acked=""
while [ $SECONDS -lt $deadline ]; do
  request GET "/v1/jobs/$HANG" "" >/dev/null
  grep -q '"ackState":"CANCELLED"' "$BODY" && { acked=yes; break; }
  sleep 0.5
done
[ -n "$acked" ] && ok "worker acknowledged cancellation" || ko "worker acknowledged cancellation"
code=$(request POST "/v1/jobs/$ID/cancel" "")
check "cancel finished job -> 409" "$code" 409

code=$(request GET /v1/queues/default/stats "")
check "queue stats -> 200" "$code" 200
grep -q '"SUCCEEDED":' "$BODY" && grep -q '"oldestQueuedAgeMs":' "$BODY" && ok "queue stats fields" || ko "queue stats fields"

code=$(request GET /v1/jobs/00000000-0000-0000-0000-000000000000 "")
check "unknown job -> 404" "$code" 404

code=$(request GET /actuator/prometheus "")
check "prometheus endpoint" "$code" 200
for metric in taskplatform_job_queue_latency_seconds_count taskplatform_job_execution_latency_seconds_count \
  taskplatform_job_retries_total taskplatform_jobs_finished_total taskplatform_worker_utilization; do
  grep -q "^$metric" "$BODY" && ok "metric $metric" || ko "metric $metric"
done

echo "smoke: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
