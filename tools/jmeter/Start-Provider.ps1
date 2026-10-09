param(
    [string]$JMeterHome = 'D:\ChromeDownload\apache-jmeter-5.6.3',
    [int]$Port = 9999,
    [ValidateSet('hessian', 'protostuff', 'json', 'jdk')][string]$Serializer = 'hessian',
    [int]$BusinessThreads = 16,
    [int]$QueueCapacity = 1024
)
$ErrorActionPreference = 'Stop'
$jar = Join-Path $PSScriptRoot 'target/krpc-jmeter.jar'
if (-not (Test-Path -LiteralPath $jar)) { throw 'Run Build-Plugin.ps1 first.' }
if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
    throw "Port $Port is occupied. Select another Port and use the same port in JMeter."
}
$classpath = "$jar;$JMeterHome/lib/*"
$loggingUri = ([System.Uri](Join-Path $PSScriptRoot 'provider-log4j2.xml')).AbsoluteUri
& java "-Dlog4j.configurationFile=$loggingUri" `
    "-Dkrpc.bench.port=$Port" "-Dkrpc.bench.serializer=$Serializer" `
    "-Dkrpc.bench.threads=$BusinessThreads" "-Dkrpc.bench.queue=$QueueCapacity" `
    -cp $classpath org.example.jmeter.JMeterProvider
if ($LASTEXITCODE -ne 0) { throw 'Provider exited with an error.' }
