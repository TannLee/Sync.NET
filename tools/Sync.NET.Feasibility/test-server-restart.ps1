param(
    [Parameter(Mandatory)][string]$PackageDirectory,
    [ValidateRange(1, 65535)][int]$Port = 4821
)

$ErrorActionPreference = 'Stop'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$phase = Join-Path $repo '.phase0'
$server = (Resolve-Path (Join-Path $phase 'kinedb-x86_64-pc-windows-msvc/kinedb.exe')).Path
$PackageDirectory = (Resolve-Path -LiteralPath $PackageDirectory).Path
$probe = Join-Path $PSScriptRoot 'kinedb-sync-probe.mjs'
$revision = & $server --version
if ($LASTEXITCODE -ne 0 -or $revision -ne 'kinedb 3c2ace4d54857df44bf160fb71ddb2cb484d1009') {
    throw 'The server revision does not match the Phase 0 pin.'
}
if (Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue) {
    throw "Port $Port is occupied."
}

$runName = 'server-restart-' + [Guid]::NewGuid().ToString('N')
$runDirectory = (New-Item -ItemType Directory (Join-Path $phase $runName)).FullName
$replicaDirectory = (New-Item -ItemType Directory (Join-Path $runDirectory 'replica')).FullName
$table = 'phase0_' + [Guid]::NewGuid().ToString('N').Substring(0, 16)
$previousPassword = $env:KINEDB_ROOT_PASSWORD
$previousPort = $env:SYNCNET_KINEDB_PORT
$serverProcess = $null

function Start-ProbeServer {
    param([string]$LogName)
    $process = Start-Process -FilePath $server -WorkingDirectory $runDirectory -WindowStyle Hidden -PassThru `
        -ArgumentList @('--listen', '127.0.0.1', '--port', "$Port", '--data-dir', 'server', '--no-web') `
        -RedirectStandardOutput (Join-Path $runDirectory "$LogName.stdout.log") `
        -RedirectStandardError (Join-Path $runDirectory "$LogName.stderr.log")
    try {
        $deadline = [DateTime]::UtcNow.AddSeconds(30)
        do {
            $process.Refresh()
            if ($process.HasExited) { throw "Server exited. See $runDirectory/$LogName.stderr.log" }
            $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
                Where-Object OwningProcess -eq $process.Id
            if ($listener) {
                $status = curl.exe -s -o NUL -w '%{http_code}' --max-time 1 "http://127.0.0.1:$Port/health"
                if ($status -eq '200') { return $process }
            }
            Start-Sleep -Milliseconds 250
        } while ([DateTime]::UtcNow -lt $deadline)
        throw "Server did not become healthy. See $runDirectory/$LogName.stderr.log"
    } catch {
        $process.Refresh()
        if (-not $process.HasExited) {
            Stop-Process -InputObject $process
            $process.WaitForExit()
        }
        throw
    }
}

try {
    $env:KINEDB_ROOT_PASSWORD = [Guid]::NewGuid().ToString('N') + [Guid]::NewGuid().ToString('N')
    $env:SYNCNET_KINEDB_PORT = "$Port"
    $serverProcess = Start-ProbeServer 'before'
    & node $probe $PackageDirectory restart-prepare $replicaDirectory $table
    if ($LASTEXITCODE -ne 0) { throw "Prepare failed. See $runDirectory" }

    Stop-Process -InputObject $serverProcess
    $serverProcess.WaitForExit()
    $serverProcess = Start-ProbeServer 'after'
    & node $probe $PackageDirectory restart-verify $replicaDirectory $table
    if ($LASTEXITCODE -ne 0) { throw "Replay after server restart failed. See $runDirectory" }
} finally {
    if ($serverProcess) {
        $serverProcess.Refresh()
        if (-not $serverProcess.HasExited) {
            Stop-Process -InputObject $serverProcess
            $serverProcess.WaitForExit()
        }
    }
    $env:KINEDB_ROOT_PASSWORD = $previousPassword
    $env:SYNCNET_KINEDB_PORT = $previousPort
}
