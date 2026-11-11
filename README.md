# Sync.NET

Sync.NET is an experimental offline-first synchronization SDK for .NET 10. It lets an application read and write a local replica while disconnected, then push pending changes and pull updates from KineDB.

The current provider runs KineDB WebAssembly in a Node.js worker. Sync.NET supplies a typed .NET API, while KineDB owns the replica storage, write journal, conflict outcomes and pull checkpoints. The preview has been tested against a KineDB server on Windows x64.

## Advantages and trade-offs

| Advantage | Trade-off |
| --- | --- |
| Local reads and durable writes after the initial sync | A new replica needs one successful online sync before the application can use it offline. |
| Incremental pull with persistent checkpoints | A checkpoint can expire. The SDK offers scope recovery, which may require a full refill. |
| Per-write outcomes, retained refusals and replay | A lost acknowledgment can leave the push outcome unknown. Replay has been tested after client and server restarts and after a 60-second delay. Its maximum retention window is unverified. |
| Typed records and explicit sync from .NET | Node.js, the pinned Wasm package and a matching KineDB server are runtime dependencies. Direct Windows DLL integration is still unverified. |
| Collection dependencies, filtered replicas and attachment sync | The coordinator orders work at the collection level. Record-level dependencies and atomic transactions across .NET collections remain outside the preview. |

Local writes wait for an active sync to finish. Tests on the pinned engine showed that accepting a write while an earlier push was waiting for an acknowledgment could let the subsequent pull overwrite the newer value. The SDK queues those writes and reports success once the local commit finishes. Slow sync can therefore increase write latency.

## Requirements

- .NET 10 SDK and Node.js 22.3 or later.
- KineDB server at commit `3c2ace4d54857df44bf160fb71ddb2cb484d1009`.
- `@kinedb/embedded@0.0.0-g3c2ace4d5485`, extracted to a directory containing `package.json` and `src/kinedb.wasm`.

The SDK checks the package version, Wasm checksum and server revision. The NuGet packages contain Sync.NET code and its worker. Obtain the KineDB server and Wasm package separately from the vendor. [KineDB downloads and server setup](https://docs.kinedb.com/run) · [Embedded SDK](https://docs.kinedb.com/clients/embedded.html)

## Install from NuGet.org

Once the preview is published, run this command in your .NET 10 project. NuGet resolves `Sync.NET` as a dependency of the KineDB provider.

```powershell
dotnet add package Sync.NET.KineDB --version 0.1.0-preview.1 --source https://api.nuget.org/v3/index.json
```

**Publication status:** both preview packages have been built and tested. Upload to NuGet.org is pending. To try the implementation now, clone the repository and build the solution with `dotnet build Sync.NET.slnx`.

## Set up KineDB on Windows

Run these commands from the repository root. The archives are pinned to the same build and verified before extraction.

```powershell
$phase = (New-Item -ItemType Directory -Force .phase0).FullName
npm.cmd pack @kinedb/embedded@0.0.0-g3c2ace4d5485 --registry=https://git.kinedb.com/api/packages/kinedb/npm/ --pack-destination $phase
if ($LASTEXITCODE -ne 0) { throw 'Could not download the embedded package.' }
$wasmArchive = Join-Path $phase 'kinedb-embedded-0.0.0-g3c2ace4d5485.tgz'
if ((Get-FileHash $wasmArchive).Hash -ne 'A85AD580BA798344F487210DFE9667F1A93B9EA61D1898EEC8397903CA98D6BC') {
    throw 'Embedded package checksum mismatch.'
}
tar -xf $wasmArchive -C $phase
if ($LASTEXITCODE -ne 0) { throw 'Could not extract the embedded package.' }

$serverArchive = Join-Path $phase 'kinedb-x86_64-pc-windows-msvc.tar.gz'
$url = 'https://git.kinedb.com/api/packages/kinedb/generic/kinedb/3c2ace4d5485/kinedb-x86_64-pc-windows-msvc.tar.gz'
Invoke-WebRequest $url -OutFile $serverArchive
if ((Get-FileHash $serverArchive).Hash -ne '213D87C30330E816E0B43495FD836C528FF79A245AD78770B312F3590EB2F0E6') {
    throw 'Server archive checksum mismatch.'
}
tar -xf $serverArchive -C $phase
if ($LASTEXITCODE -ne 0) { throw 'Could not extract the server archive.' }
```

Start the server in a separate PowerShell window from the repository root. Use the same password when connecting from the sample or SQL shell.

```powershell
$env:KINEDB_ROOT_PASSWORD = Read-Host 'KineDB root password' -MaskInput
try {
    .\.phase0\kinedb-x86_64-pc-windows-msvc\kinedb.exe --listen 127.0.0.1 --port 4820 --data-dir .phase0/windows-server-data --no-web
} finally {
    Remove-Item Env:KINEDB_ROOT_PASSWORD
}
```

## Run the console sample

In a second PowerShell window at the repository root, initialize the sample table and sync each replica once. The sample uses `ws://127.0.0.1:4820/ws` and the `root` account by default.

```powershell
$package = (Resolve-Path .phase0/package).Path
$env:KINEDB_ROOT_PASSWORD = Read-Host 'KineDB root password' -MaskInput
try {
    dotnet build Sync.NET.slnx
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-a init
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-a sync
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-b sync

    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-a put demo 'Hello offline'
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-a sync
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-b sync
    dotnet run --project samples/Sync.NET.Console --no-build -- $package .phase0/demo-b get demo
} finally {
    Remove-Item Env:KINEDB_ROOT_PASSWORD
}
```

The `put` command commits to replica A locally. The next two sync commands send the record to the server and pull it into replica B. The final command prints the record with the text `Hello offline`. The sample also supports `list` and `delete <id>`.

## Use the SDK in your application

Provision a server table with a text key and a non-null JSON payload stored as text. The provider validates the two-column schema before push. You can generate the DDL with `KineDbCollection<T>.CreateTableSql(tableName)`.

```sql
CREATE TABLE IF NOT EXISTS demo_notes (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
```

Set `SYNCNET_KINEDB_WASM_PACKAGE` to the extracted package directory and `KINEDB_ROOT_PASSWORD` to the test account password, then use a collection as follows.

```csharp
using SyncDotNet;
using SyncDotNet.KineDB;

var package = Environment.GetEnvironmentVariable("SYNCNET_KINEDB_WASM_PACKAGE")
    ?? throw new InvalidOperationException("Set SYNCNET_KINEDB_WASM_PACKAGE.");
var mapping = new CollectionMapping<Note>("demo_notes", "note-v1", note => note.Id);
var root = Path.Combine(Path.GetTempPath(), "syncnet-demo-" + Guid.NewGuid().ToString("N"));
var options = new KineDbReplicaOptions(
    package, Path.Combine(root, "a"), new Uri("ws://127.0.0.1:4820/ws"), "root",
    _ => ValueTask.FromResult(Environment.GetEnvironmentVariable("KINEDB_ROOT_PASSWORD")
        ?? throw new InvalidOperationException("Set KINEDB_ROOT_PASSWORD.")));

using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
var token = timeout.Token;
await using var a = new KineDbCollection<Note>(mapping, options);
await using var b = new KineDbCollection<Note>(mapping,
    options with { ReplicaDirectory = Path.Combine(root, "b") });

await SyncAsync(a, token);
await SyncAsync(b, token);

var note = new Note(Guid.NewGuid().ToString("N"), "Hello offline");
await a.PutAsync(note, token);
await SyncAsync(a, token);
await SyncAsync(b, token);

if (await b.GetAsync(note.Id, token) != note)
    throw new InvalidOperationException("Replica B did not receive the record.");
Console.WriteLine(note.Text);

static async Task SyncAsync(ISyncCollection<Note> replica, CancellationToken token)
{
    var result = await replica.SynchronizeAsync(cancellationToken: token);
    if (result.Status != SyncStatus.UpToDate)
        throw new InvalidOperationException($"Sync {result.Status} at {result.Stage}. {result.Error}");
}

record Note(string Id, string Text);
```

Use a stable key and keep `schemaId` stable while the serialized representation remains compatible. Each directory is bound to its table, schema, endpoint, account and scope. Use separate directories for distinct replicas. For remote connections, configure a `wss://` endpoint ending in `/ws` and provide credentials through the callback.

### Results and recovery

- Inspect `Push.Outcomes` for applied writes, conflicts, refusals and replays. Use `ListRefusedAsync()` to read refusal records retained by KineDB.
- Treat `PushOutcomeUnknown` as an uncertain commit. A later sync can recover the outcome through KineDB replay. `PushRetryCount` enables up to two retries for transient failures within a five-second window and defaults to zero.
- Use `RecoverScopeAsync()` to push pending writes before refilling the current scope. Recovery stops before refill if that push has refusals or unknown outcomes.
- Configure `MaxPayloadBytes` and `MaxPendingLocalOperations` to bound the size of local writes and the number of local operations that can wait or execute. The `SyncDotNet.KineDB` meter exposes run counts, duration and push retries.

### Additional APIs

| API | Use |
| --- | --- |
| `ScopeKey` | Replicate one exact key. Writes outside that key are rejected. Server grants control access to server data. |
| `SyncPlan.RunAsync` | Order collection syncs by dependency. A failed prerequisite defers its dependents while independent steps continue. |
| `SyncScopeView.ReadUnionAsync` | Merge local reads from multiple replicas. Identical rows are deduplicated and conflicting values for the same key are reported as an error. |
| `KineDbCollection<T>.BootstrapAsync` | Bootstrap into a staging directory, validate the checkpoint and typed rows, then activate a new replica directory. Reports the snapshot commit when KineDB provides a pack. |
| `KineDbAttachments` | Sync files in a dedicated table. Local upload and download use streams and temporary files, with a default limit of 64 MiB per operation. |

## Tests and benchmarks

Integration tests start their own Windows server instances. Set both paths to run the KineDB tests.

```powershell
$env:SYNCNET_KINEDB_WASM_PACKAGE = (Resolve-Path .phase0/package).Path
$env:SYNCNET_KINEDB_SERVER_EXE = (Resolve-Path .phase0/kinedb-x86_64-pc-windows-msvc/kinedb.exe).Path
dotnet test Sync.NET.slnx -c Release
```

The preview passed **12/12 tests** on Windows x64, with **0 warnings and 0 errors** in the Release build. The suite covers two-replica convergence, persisted offline edits, conflicts, lost acknowledgments, schema checks, scope recovery, permission revocation and restoration, snapshot bootstrap and attachment round trips. The [feasibility probes](tools/Sync.NET.Feasibility) also cover interrupted and paged pulls, expired checkpoints, replay after server restart, and engine-level transactions across tables.

With the test server running and `$package` set, run the benchmark:

```powershell
dotnet run --project samples/Sync.NET.Console -- $package .phase0/benchmark benchmark 1000
```

The benchmark creates a dedicated table and verifies convergence. A loopback run with 1,000 rows recorded about **7.96 s** of offline writes, **10.10 ms** P95 write latency, **867 ms** for writer sync and **179 ms** for reader sync. It also verified 750 mixed changes and 250 conflict outcomes. The reported peak working set covers the .NET process. These measurements describe one test run.

## Preview boundaries

The tested deployment is Windows x64 with a Node/Wasm worker. Direct native integration in a .NET process, deployment on other platforms and installation on a separate clean machine remain unverified. Attachments have per-operation local limits, while an overall byte budget for a sync run remains open. The SDK preserves pending work when server access is revoked. Applications remain responsible for access to data already cached on a device.

Check the terms for the KineDB runtime separately before redistribution. The provider's preview NuGet packages expect users to supply that runtime.
