param([string]$JMeterHome = 'D:\ChromeDownload\apache-jmeter-5.6.3')
$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
if (-not (Test-Path -LiteralPath (Join-Path $JMeterHome 'lib/ext/ApacheJMeter_java.jar'))) {
    throw 'JMeterHome does not contain JMeter Java Request support.'
}
Push-Location $repo
try {
    & mvn -q -pl krpc-core -am -DskipTests install
    if ($LASTEXITCODE -ne 0) { throw 'RPC build failed.' }
    & mvn -q -f (Join-Path $PSScriptRoot 'pom.xml') clean package
    if ($LASTEXITCODE -ne 0) { throw 'JMeter plugin build/tests failed.' }
    $jar = Join-Path $PSScriptRoot 'target/krpc-jmeter.jar'
    Copy-Item -LiteralPath $jar -Destination (Join-Path $JMeterHome 'lib/ext/krpc-jmeter.jar') -Force
    Write-Output 'Plugin installed. Restart JMeter to load it.'
} finally {
    Pop-Location
}
