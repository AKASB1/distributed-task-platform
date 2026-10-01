# Runs the thread-based test classes N times in a row (default 20) and stops at the first failing run.
# Usage: powershell -ExecutionPolicy Bypass -File scripts\repeat-concurrency-tests.ps1 [-Runs 20]
param([int]$Runs = 20)
$unit = "WorkerPoolTest,JobLifecycleTest,ReconcilerTest,InMemoryDispatcherTest,InMemoryRateLimiterTest,RedisConnectionHolderTest"
# the broker/cache ITs need Docker and skip themselves without it
$it = "RaceIT,FailureInjectionIT,JdbcJobRepositoryIT,JobApiIT,RabbitMqDispatcherIT,RedisRateLimiterIT"
Set-Location (Join-Path $PSScriptRoot "..")
New-Item -ItemType Directory -Force target | Out-Null
for ($i = 1; $i -le $Runs; $i++) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $log = "target\repeat-run-$i.log"
    & .\mvnw.cmd -B -q verify "-Dtest=$unit" "-Dit.test=$it" "-Dsurefire.failIfNoSpecifiedTests=false" `
        "-Dfailsafe.failIfNoSpecifiedTests=false" *> $log
    if ($LASTEXITCODE -ne 0) {
        Write-Output "run $i/$($Runs): FAIL ($([int]$watch.Elapsed.TotalSeconds) s) - see $log"
        exit 1
    }
    Write-Output "run $i/$($Runs): PASS ($([int]$watch.Elapsed.TotalSeconds) s)"
}
Write-Output "all $Runs runs passed"
