# Smoke test of the main flow against a running service (Windows PowerShell 5.1+ or PowerShell 7).
# Usage: powershell -ExecutionPolicy Bypass -File scripts\smoke.ps1 [-BaseUrl http://127.0.0.1:18080] [-TimeoutSeconds 60]
param(
    [string]$BaseUrl = "http://127.0.0.1:18080",
    [int]$TimeoutSeconds = 60
)
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Net.Http

$handler = New-Object System.Net.Http.HttpClientHandler
$handler.UseProxy = $false
$client = New-Object System.Net.Http.HttpClient($handler)
$client.Timeout = [TimeSpan]::FromSeconds(30)
$script:pass = 0
$script:fail = 0

function Ok($name) { Write-Output "PASS  $name"; $script:pass++ }
function Ko($name) { Write-Output "FAIL  $name"; $script:fail++ }
function Check($name, $actual, $expected) {
    if ("$actual" -eq "$expected") { Ok $name } else { Ko "$name (expected $expected, got $actual)" }
}

function Send($method, $path, $json, $headers) {
    $request = New-Object System.Net.Http.HttpRequestMessage([System.Net.Http.HttpMethod]::new($method), "$BaseUrl$path")
    if ($json) { $request.Content = New-Object System.Net.Http.StringContent($json, [System.Text.Encoding]::UTF8, "application/json") }
    if ($headers) { foreach ($k in $headers.Keys) { [void]$request.Headers.TryAddWithoutValidation($k, $headers[$k]) } }
    $response = $client.SendAsync($request).GetAwaiter().GetResult()
    $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
    $parsed = $null
    if ($body -and $body.TrimStart().StartsWith("{")) { $parsed = $body | ConvertFrom-Json }
    return [pscustomobject]@{ Status = [int]$response.StatusCode; Body = $body; Json = $parsed; Headers = $response.Headers }
}

function WaitState($id, $want) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $state = ""
    while ((Get-Date) -lt $deadline) {
        $r = Send "GET" "/v1/jobs/$id" $null $null
        $state = $r.Json.state
        if ($state -eq $want) { return $r }
        Start-Sleep -Milliseconds 500
    }
    # Write-Host, not Write-Output: anything on the output stream would become part of the return value
    Write-Host "      job $id is $state, wanted $want"
    return $null
}

Write-Output "smoke test against $BaseUrl"

$r = Send "GET" "/actuator/health" $null $null
Check "health endpoint" $r.Status 200
Check "health is UP" $r.Json.status "UP"

$key = "smoke-" + [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() + "-" + $PID
$r = Send "POST" "/v1/jobs" '{"type":"sleep","payload":{"durationMs":200}}' @{ "Idempotency-Key" = $key }
Check "submit sleep job -> 201" $r.Status 201
$id = $r.Json.id

$r = Send "POST" "/v1/jobs" '{"type":"sleep","payload":{"durationMs":200}}' @{ "Idempotency-Key" = $key }
Check "same Idempotency-Key -> 200" $r.Status 200
Check "same Idempotency-Key -> same job" $r.Json.id $id
$replayed = $null
if ($r.Headers.Contains("Idempotent-Replayed")) { $replayed = ($r.Headers.GetValues("Idempotent-Replayed") | Select-Object -First 1) }
Check "Idempotent-Replayed header" $replayed "true"

$r = Send "POST" "/v1/jobs" '{"type":"sleep","payload":{"durationMs":1}}' @{ "Idempotency-Key" = $key }
Check "same key, different body -> 422" $r.Status 422

Check "unknown type -> 400" (Send "POST" "/v1/jobs" '{"type":"does-not-exist"}' $null).Status 400
Check "maxAttempts 0 -> 400" (Send "POST" "/v1/jobs" '{"type":"sleep","maxAttempts":0}' $null).Status 400
Check "malformed JSON -> 400" (Send "POST" "/v1/jobs" '{not json' $null).Status 400

if (WaitState $id "SUCCEEDED") { Ok "sleep job SUCCEEDED" } else { Ko "sleep job SUCCEEDED" }

$r = Send "POST" "/v1/jobs" '{"type":"flaky","payload":{"durationMs":20,"failAttempts":1}}' $null
Check "submit flaky job -> 201" $r.Status 201
$done = WaitState $r.Json.id "SUCCEEDED"
if ($done) { Ok "flaky job retried and SUCCEEDED"; Check "flaky job has 2 attempts" @($done.Json.deliveries).Count 2 }
else { Ko "flaky job retried and SUCCEEDED" }

$r = Send "POST" "/v1/jobs" '{"type":"flaky","payload":{"failure":"permanent","failAttempts":9}}' $null
if (WaitState $r.Json.id "FAILED") { Ok "permanent failure -> FAILED" } else { Ko "permanent failure -> FAILED" }

$r = Send "POST" "/v1/jobs" '{"type":"flaky","timeoutMs":120000,"payload":{"failure":"hang","failAttempts":9}}' $null
$hang = $r.Json.id
if (WaitState $hang "RUNNING") { Ok "hanging job RUNNING" } else { Ko "hanging job RUNNING" }
$r = Send "POST" "/v1/jobs/$hang/cancel" $null $null
Check "cancel running job -> 200" $r.Status 200
Check "cancelled job state" $r.Json.state "CANCELLED"
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$acked = $false
while ((Get-Date) -lt $deadline -and -not $acked) {
    $r = Send "GET" "/v1/jobs/$hang" $null $null
    if (@($r.Json.deliveries | Where-Object { $_.ackState -eq "CANCELLED" }).Count -gt 0) { $acked = $true } else { Start-Sleep -Milliseconds 500 }
}
if ($acked) { Ok "worker acknowledged cancellation" } else { Ko "worker acknowledged cancellation" }
Check "cancel finished job -> 409" (Send "POST" "/v1/jobs/$id/cancel" $null $null).Status 409

$r = Send "GET" "/v1/queues/default/stats" $null $null
Check "queue stats -> 200" $r.Status 200
if ($null -ne $r.Json.counts.SUCCEEDED -and $null -ne $r.Json.oldestQueuedAgeMs) { Ok "queue stats fields" } else { Ko "queue stats fields" }

Check "unknown job -> 404" (Send "GET" "/v1/jobs/00000000-0000-0000-0000-000000000000" $null $null).Status 404

$r = Send "GET" "/actuator/prometheus" $null $null
Check "prometheus endpoint" $r.Status 200
foreach ($metric in @("taskplatform_job_queue_latency_seconds_count", "taskplatform_job_execution_latency_seconds_count",
        "taskplatform_job_retries_total", "taskplatform_jobs_finished_total", "taskplatform_worker_utilization")) {
    if ($r.Body -match "(?m)^$metric") { Ok "metric $metric" } else { Ko "metric $metric" }
}

Write-Output "smoke: $($script:pass) passed, $($script:fail) failed"
if ($script:fail -gt 0) { exit 1 }
exit 0
