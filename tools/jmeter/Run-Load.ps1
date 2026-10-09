param(
    [string]$JMeterHome = 'D:\ChromeDownload\apache-jmeter-5.6.3',
    [string]$HostAddress = '127.0.0.1',
    [int]$Port = 9999,
    [int]$Threads = 100,
    [int]$RampSeconds = 10,
    [int]$DurationSeconds = 300,
    [int]$Loops = -1,
    [int]$WarmupLoops = 1000,
    [int]$PayloadBytes = 1024,
    [ValidateSet('hessian', 'protostuff', 'json', 'jdk')][string]$Serializer = 'hessian',
    [int]$SlowPercent = 0,
    [int]$SlowMillis = 100
)
$ErrorActionPreference = 'Stop'
if ($Threads -lt 1 -or $DurationSeconds -lt 1 -or $Loops -eq 0 -or $Loops -lt -1 -or $WarmupLoops -lt 0 -or $RampSeconds -lt 0) {
    throw 'Invalid thread, duration, ramp or loop parameters.'
}
if (-not (Test-Path -LiteralPath (Join-Path $JMeterHome 'lib/ext/krpc-jmeter.jar'))) {
    throw 'Run Build-Plugin.ps1 first.'
}
$run = Join-Path $PSScriptRoot ('target/results/' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
New-Item -ItemType Directory -Path $run -Force | Out-Null
$scheduler = if ($Loops -gt 0) { 'false' } else { 'true' }
$jmeterArgs = @(
    '-n', '-t', (Join-Path $PSScriptRoot 'rpc-load.jmx'),
    '-l', (Join-Path $run 'samples.jtl'), '-j', (Join-Path $run 'jmeter.log'),
    '-e', '-o', (Join-Path $run 'report'),
    "-Jhost=$HostAddress", "-Jport=$Port", "-Jthreads=$Threads", "-JrampSeconds=$RampSeconds",
    "-JdurationSeconds=$DurationSeconds", "-Jloops=$Loops", "-Jscheduler=$scheduler",
    "-JwarmupLoops=$WarmupLoops", "-JpayloadBytes=$PayloadBytes", "-Jserializer=$Serializer",
    "-JslowPercent=$SlowPercent", "-JslowMillis=$SlowMillis",
    '-Jjmeter.reportgenerator.sample_filter=RPC echo.*',
    '-Jjmeter.save.saveservice.output_format=csv',
    '-Jjmeter.save.saveservice.print_field_names=true',
    '-Jjmeter.save.saveservice.response_data=false'
)
[ordered]@{
    startedAt = (Get-Date -Format o); hostAddress = $HostAddress; port = $Port
    threads = $Threads; rampSeconds = $RampSeconds; durationSeconds = $DurationSeconds
    loops = $Loops; warmupLoops = $WarmupLoops; payloadBytes = $PayloadBytes
    serializer = $Serializer; slowPercent = $SlowPercent; slowMillis = $SlowMillis
    scope = 'fixed-address transport echo; no registry, retries, rate limiting or tracing'
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $run 'parameters.json') -Encoding UTF8
& (Join-Path $JMeterHome 'bin/jmeter.bat') @jmeterArgs
if ($LASTEXITCODE -ne 0) { throw "JMeter failed. Inspect $run/jmeter.log" }
Write-Output "Report: $run/report/index.html"
Write-Output 'JMeter exit code alone does not prove RPC success: inspect Error % and response codes.'
