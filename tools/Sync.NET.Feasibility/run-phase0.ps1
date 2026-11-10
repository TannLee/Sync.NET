param(
    [string]$PackageDirectory,
    [ValidateRange(1, 65535)][int]$RetentionPort = 4821,
    [ValidateRange(0, 3600)][int]$ReplayDelaySeconds = 0
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$phase = Join-Path $repo '.phase0'
if (-not $PackageDirectory) { $PackageDirectory = Join-Path $phase 'package' }
$PackageDirectory = (Resolve-Path -LiteralPath $PackageDirectory).Path
$server = (Resolve-Path (Join-Path $phase 'kinedb-x86_64-pc-windows-msvc/kinedb.exe')).Path
$probe = Join-Path $PSScriptRoot 'kinedb-sync-probe.mjs'
$credential = Join-Path $phase 'windows-server-root.xml'

function Invoke-PhaseCommand {
    param([string]$Label, [string]$Executable, [string[]]$Arguments)
    Write-Host "=== $Label ==="
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Label failed with exit code $LASTEXITCODE" }
}

if (Get-NetTCPConnection -State Listen -LocalPort $RetentionPort -ErrorAction SilentlyContinue) {
    throw "Retention port $RetentionPort is occupied. Pick a free local port."
}

$previousPassword = $env:KINEDB_ROOT_PASSWORD
$previousPort = $env:SYNCNET_KINEDB_PORT
$previousPackage = $env:SYNCNET_KINEDB_WASM_PACKAGE
$previousDelay = $env:SYNCNET_KINEDB_REPLAY_DELAY_SECONDS
$retentionServer = $null
Push-Location $repo
try {
    $env:SYNCNET_KINEDB_WASM_PACKAGE = $PackageDirectory
    $env:SYNCNET_KINEDB_PORT = '4820'
    $env:SYNCNET_KINEDB_REPLAY_DELAY_SECONDS = "$ReplayDelaySeconds"
    $env:KINEDB_ROOT_PASSWORD = (Import-Clixml $credential).GetNetworkCredential().Password

    Invoke-PhaseCommand 'Build' dotnet @('build', 'Sync.NET.slnx')
    Invoke-PhaseCommand 'Offline console' dotnet @('run', '--project', 'tools/Sync.NET.Feasibility', '--no-build', '--', $PackageDirectory)
    Invoke-PhaseCommand 'Offline integration test' dotnet @('test', 'Sync.NET.slnx', '--no-build')
    Invoke-PhaseCommand 'Replica sync, transactions and replay' node @($probe, $PackageDirectory)
    Invoke-PhaseCommand 'Paged one-shot pull' node @($probe, $PackageDirectory, 'paged')

    $runName = 'retention-' + [Guid]::NewGuid().ToString('N')
    $env:KINEDB_ROOT_PASSWORD = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
    $env:SYNCNET_KINEDB_PORT = "$RetentionPort"
    $retentionServer = Start-Process -FilePath $server -WorkingDirectory $phase -WindowStyle Hidden -PassThru `
        -ArgumentList @('--listen', '127.0.0.1', '--port', "$RetentionPort", '--data-dir', $runName, '--no-web', '--gc-retention', '0commits') `
        -RedirectStandardOutput (Join-Path $phase "$runName.stdout.log") `
        -RedirectStandardError (Join-Path $phase "$runName.stderr.log")

    $status = $null
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    do {
        $retentionServer.Refresh()
        if ($retentionServer.HasExited) { throw "Retention server exited. See .phase0/$runName.stderr.log" }
        $listener = Get-NetTCPConnection -State Listen -LocalPort $RetentionPort -ErrorAction SilentlyContinue |
            Where-Object OwningProcess -eq $retentionServer.Id
        if ($listener) {
            $status = curl.exe -s -o NUL -w '%{http_code}' --max-time 1 "http://127.0.0.1:$RetentionPort/health"
            if ($status -eq '200') { break }
        }
        Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $deadline)
    if ($status -ne '200') { throw "Retention server did not become healthy. See .phase0/$runName.stderr.log" }

    Invoke-PhaseCommand 'Expired checkpoint' node @($probe, $PackageDirectory, 'retention')
} finally {
    if ($retentionServer) {
        $retentionServer.Refresh()
        if (-not $retentionServer.HasExited) {
            Stop-Process -InputObject $retentionServer
            $retentionServer.WaitForExit()
        }
    }
    $env:KINEDB_ROOT_PASSWORD = $previousPassword
    $env:SYNCNET_KINEDB_PORT = $previousPort
    $env:SYNCNET_KINEDB_WASM_PACKAGE = $previousPackage
    $env:SYNCNET_KINEDB_REPLAY_DELAY_SECONDS = $previousDelay
    Pop-Location
}
