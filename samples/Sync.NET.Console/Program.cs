using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using SyncDotNet;
using SyncDotNet.KineDB;

if (args.Length < 3)
{
    Console.Error.WriteLine("Usage: <wasm-package-dir> <replica-dir> init|sync|list|get <id>|put <id> <text>|delete <id>|benchmark <count>");
    return 2;
}

var mapping = new CollectionMapping<Note>("phase1_notes", "note-v1", note => note.Id);
var endpoint = new Uri(Environment.GetEnvironmentVariable("SYNCNET_KINEDB_ENDPOINT") ?? "ws://127.0.0.1:4820/ws");
var user = Environment.GetEnvironmentVariable("SYNCNET_KINEDB_USER") ?? "root";
var options = new KineDbReplicaOptions(args[0], args[1], endpoint, user, _ => ValueTask.FromResult(GetPassword()));
var json = new JsonSerializerOptions { WriteIndented = true, Converters = { new JsonStringEnumConverter() } };

using var stop = new CancellationTokenSource(TimeSpan.FromMinutes(2));
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.Cancel();
};

try
{
    await using var replica = new KineDbCollection<Note>(mapping, options);
    if (args[2] == "benchmark" && args.Length == 4 && int.TryParse(args[3], out var count) && count is >= 1 and <= 5000)
    {
        await RunBenchmarkAsync(options, count, json, stop.Token);
        return 0;
    }

    switch (args[2])
    {
        case "init" when args.Length == 3:
            await ProvisionAsync(endpoint, user, KineDbCollection<Note>.CreateTableSql(mapping.Name), stop.Token);
            Console.WriteLine("Server table is ready.");
            return 0;
        case "sync" when args.Length == 3:
            var result = await replica.SynchronizeAsync(new ConsoleProgress(), stop.Token);
            Console.WriteLine(JsonSerializer.Serialize(result, json));
            return result.Status == SyncStatus.UpToDate ? 0 : 1;
        case "list" when args.Length == 3:
            Console.WriteLine(JsonSerializer.Serialize(await replica.ListAsync(stop.Token), json));
            return 0;
        case "get" when args.Length == 4:
            Console.WriteLine(JsonSerializer.Serialize(await replica.GetAsync(args[3], stop.Token), json));
            return 0;
        case "put" when args.Length == 5:
            await replica.PutAsync(new(args[3], args[4]), stop.Token);
            Console.WriteLine("Saved locally.");
            return 0;
        case "delete" when args.Length == 4:
            Console.WriteLine(await replica.DeleteAsync(args[3], stop.Token) ? "Deleted locally." : "Key not found.");
            return 0;
        default:
            Console.Error.WriteLine("Unknown command or wrong number of arguments.");
            return 2;
    }
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("Cancelled.");
    return 1;
}
catch (Exception error)
{
    Console.Error.WriteLine(error.Message);
    return 1;
}

static string GetPassword() =>
    Environment.GetEnvironmentVariable("KINEDB_ROOT_PASSWORD") ??
    throw new InvalidOperationException("Set KINEDB_ROOT_PASSWORD for init or sync.");

static async Task RunBenchmarkAsync(KineDbReplicaOptions options, int count, JsonSerializerOptions json, CancellationToken token)
{
    var table = "bench_" + Guid.NewGuid().ToString("N");
    var mapping = new CollectionMapping<Note>(table, "note-v1", note => note.Id);
    var directory = Path.Combine(options.ReplicaDirectory, table);
    await ProvisionAsync(options.Endpoint, options.User, KineDbCollection<Note>.CreateTableSql(table), token);
    try
    {
        await using var writer = new KineDbCollection<Note>(mapping, options with { ReplicaDirectory = Path.Combine(directory, "writer") });
        await using var reader = new KineDbCollection<Note>(mapping, options with { ReplicaDirectory = Path.Combine(directory, "reader") });
        await SyncOrThrowAsync(writer, token);
        await SyncOrThrowAsync(reader, token);

        var expected = Enumerable.Range(0, count).Select(i => new Note($"row-{i:D6}", new string('x', 256))).ToArray();
        var latencies = new double[count];
        var clock = Stopwatch.StartNew();
        for (var index = 0; index < expected.Length; index++)
        {
            var started = Stopwatch.GetTimestamp();
            await writer.PutAsync(expected[index], token);
            latencies[index] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }
        var writeTime = clock.Elapsed;
        clock.Restart();
        var pushed = await SyncOrThrowAsync(writer, token);
        var writerSyncTime = clock.Elapsed;
        clock.Restart();
        var pulled = await SyncOrThrowAsync(reader, token);
        var readerSyncTime = clock.Elapsed;
        if (pushed.Push?.Sent != count || pulled.Pull?.Upserts != count ||
            !expected.SequenceEqual(await reader.ListAsync(token)))
            throw new IOException("Benchmark replicas did not converge.");

        Array.Sort(latencies);
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            Rows = count,
            OfflineWriteMilliseconds = writeTime.TotalMilliseconds,
            OfflineWriteP95Milliseconds = latencies[(int)Math.Ceiling(count * 0.95) - 1],
            WriterSyncMilliseconds = writerSyncTime.TotalMilliseconds,
            ReaderSyncMilliseconds = readerSyncTime.TotalMilliseconds,
            ReplicaDirectory = directory
        }, json));
    }
    finally
    {
        using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await ProvisionAsync(options.Endpoint, options.User, $"DROP TABLE IF EXISTS {table}", cleanup.Token);
    }
}

static async Task<SyncResult> SyncOrThrowAsync(ISyncCollection<Note> replica, CancellationToken token)
{
    var result = await replica.SynchronizeAsync(cancellationToken: token);
    if (result.Status != SyncStatus.UpToDate)
        throw new IOException($"Benchmark synchronization failed at {result.Stage}: {result.Status}.");
    return result;
}

static async Task ProvisionAsync(Uri endpoint, string user, string sql, CancellationToken token)
{
    var httpEndpoint = new UriBuilder(endpoint) { Scheme = endpoint.Scheme == "wss" ? "https" : "http", Path = "/" };
    using var http = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = httpEndpoint.Uri };
    using var credentials = new StringContent(JsonSerializer.Serialize(new { username = user, password = GetPassword() }),
        Encoding.UTF8, "application/json");
    using var login = await http.PostAsync("login", credentials, token);
    login.EnsureSuccessStatusCode();
    using var reply = JsonDocument.Parse(await login.Content.ReadAsStringAsync(token));
    http.DefaultRequestHeaders.Authorization = new("Bearer", reply.RootElement.GetProperty("token").GetString());
    using var response = await http.PostAsync("sql", new StringContent(sql, Encoding.UTF8), token);
    response.EnsureSuccessStatusCode();
    using var result = JsonDocument.Parse(await response.Content.ReadAsStringAsync(token));
    if (result.RootElement.GetProperty("type").GetString() == "error")
        throw new IOException("Server table command failed.");
}

internal sealed record Note(string Id, string Text);

internal sealed class ConsoleProgress : IProgress<SyncStage>
{
    public void Report(SyncStage stage) => Console.Error.WriteLine(stage);
}
