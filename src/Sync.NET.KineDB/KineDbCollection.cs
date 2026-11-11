using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace SyncDotNet.KineDB;

public sealed record KineDbReplicaOptions(
    string PackageDirectory, string ReplicaDirectory, Uri Endpoint, string User,
    Func<CancellationToken, ValueTask<string>> GetPassword)
{
    public string NodeExecutable { get; init; } = "node";
}

public sealed class KineDbCollection<T> : ISyncCollection<T>, IRefusedWriteReader where T : class
{
    private readonly CollectionMapping<T> _mapping;
    private readonly KineDbReplicaOptions _options;
    private readonly string _packageDirectory;
    private readonly string _directory;
    private readonly SemaphoreSlim _lifecycle = new(1);
    // The pinned engine's pull can replace a row edited during a push.
    // Hold writes until push and pull both finish so acknowledged edits survive.
    private readonly SemaphoreSlim _mutation = new(1);
    private WorkerProcess? _worker;
    private bool _disposed;

    public KineDbCollection(CollectionMapping<T> mapping, KineDbReplicaOptions options)
    {
        ArgumentNullException.ThrowIfNull(mapping);
        ArgumentNullException.ThrowIfNull(options);
        ValidateName(mapping.Name);
        ArgumentException.ThrowIfNullOrWhiteSpace(mapping.SchemaId);
        ArgumentNullException.ThrowIfNull(mapping.KeySelector);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.User);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.NodeExecutable);
        ArgumentNullException.ThrowIfNull(options.GetPassword);
        ArgumentNullException.ThrowIfNull(options.Endpoint);
        var endpoint = options.Endpoint;
        if (!endpoint.IsAbsoluteUri || (endpoint.Scheme != "wss" && !(endpoint.Scheme == "ws" && endpoint.IsLoopback)) ||
            endpoint.AbsolutePath != "/ws" || endpoint.UserInfo.Length != 0 ||
            endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0)
        {
            throw new ArgumentException("Use a /ws endpoint over wss, or ws on loopback.", nameof(options));
        }

        _mapping = mapping;
        _options = options;
        _packageDirectory = Path.GetFullPath(options.PackageDirectory);
        _directory = Path.GetFullPath(options.ReplicaDirectory);
    }

    public static string CreateTableSql(string name)
    {
        ValidateName(name);
        return $"CREATE TABLE IF NOT EXISTS \"{name}\" (id TEXT PRIMARY KEY, payload TEXT NOT NULL)";
    }

    public async Task<T?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var rows = await SendAsync("get", cancellationToken, key: key);
        return rows.GetArrayLength() == 0 ? null : Map(rows[0]);
    }

    public async Task<IReadOnlyList<T>> ListAsync(CancellationToken cancellationToken = default)
    {
        var rows = await SendAsync("list", cancellationToken);
        return rows.EnumerateArray().Select(Map).ToArray();
    }

    public async Task PutAsync(T value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        var key = _mapping.KeySelector(value);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var payload = JsonSerializer.Serialize(value, _mapping.JsonOptions);
        await MutateAsync("put", key, payload, cancellationToken);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return (await MutateAsync("delete", key, null, cancellationToken)).GetBoolean();
    }

    public async Task<SyncResult> SynchronizeAsync(
        IProgress<SyncStage>? progress = null, CancellationToken cancellationToken = default)
    {
        var stage = SyncStage.CheckingServer;
        PushResult? push = null;
        PullResult? pull = null;
        var entered = false;
        try
        {
            await _mutation.WaitAsync(cancellationToken);
            entered = true;
            progress?.Report(stage);
            var ready = await CheckServerAsync(cancellationToken);
            var password = await _options.GetPassword(cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            await SendAsync("schema", cancellationToken, password: password, interruptible: true);
            if (ready)
            {
                stage = SyncStage.Pushing;
                progress?.Report(stage);
                var response = await SendAsync("push", cancellationToken, password: password, interruptible: true);
                push = ReadPush(response);
                if (!push.OutcomesComplete)
                {
                    return new(SyncStatus.Partial, stage, push, PushOutcomeUnknown: true,
                        Error: "KineDB did not return a usable outcome for every write.")
                        { FailureKind = SyncFailureKind.UnknownPushOutcome };
                }
            }

            stage = SyncStage.Pulling;
            progress?.Report(stage);
            pull = ReadPull(await SendAsync("pull", cancellationToken, password: password, interruptible: true));
            var partial = pull.GapReason is not null || push?.Outcomes.Any(
                outcome => outcome.Status is WriteStatus.Conflict or WriteStatus.Rejected) == true;
            stage = SyncStage.Completed;
            progress?.Report(stage);
            return new(partial ? SyncStatus.Partial : SyncStatus.UpToDate, stage, push, pull);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new(SyncStatus.Cancelled, stage, push, pull, PushOutcomeUnknown: stage == SyncStage.Pushing && push is null);
        }
        catch (KineDbOperationException error)
        {
            var unknown = stage == SyncStage.Pushing && (error.OutcomeUnknown || error.Retryable);
            var kind = unknown ? SyncFailureKind.UnknownPushOutcome : error.Configuration ? SyncFailureKind.Configuration :
                error.Retryable ? SyncFailureKind.Transient : SyncFailureKind.Provider;
            return new(push is not null ? SyncStatus.Partial : error.Retryable ? SyncStatus.Deferred : SyncStatus.Failed,
                stage, push, pull, unknown, error.Message) { FailureKind = kind };
        }
        catch (Exception error) when (error is IOException or InvalidDataException or JsonException or System.ComponentModel.Win32Exception)
        {
            var unknown = stage == SyncStage.Pushing && push is null;
            return new(push is not null ? SyncStatus.Partial : SyncStatus.Failed, stage, push, pull,
                PushOutcomeUnknown: unknown, Error: "The KineDB worker did not return a usable response.")
                { FailureKind = unknown ? SyncFailureKind.UnknownPushOutcome : SyncFailureKind.InvalidResponse };
        }
        finally
        {
            if (entered) _mutation.Release();
        }
    }

    private async Task<bool> CheckServerAsync(CancellationToken cancellationToken)
    {
        const int maxAttempts = 3;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return (await SendAsync("check", cancellationToken, interruptible: true)).GetBoolean();
            }
            catch (KineDbOperationException error) when (error.Retryable && attempt < maxAttempts)
            {
                var delay = TimeSpan.FromMilliseconds(100 * (1 << (attempt - 1)) + Random.Shared.Next(50));
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    public async Task<IReadOnlyList<RefusedWrite>> ListRefusedAsync(CancellationToken cancellationToken = default)
    {
        await _mutation.WaitAsync(cancellationToken);
        try
        {
            var password = await _options.GetPassword(cancellationToken);
            var rows = await SendAsync("refused", cancellationToken, password: password, interruptible: true);
            return rows.EnumerateArray().Select(row => new RefusedWrite(
                OptionalText(row, "seq"), OptionalText(row, "op"), row.GetProperty("pk"),
                OptionalText(row, "code"), OptionalText(row, "reason"),
                OptionalText(row, "readVersion"), OptionalText(row, "serverVersion"))).ToArray();
        }
        finally
        {
            _mutation.Release();
        }
    }

    private async Task<JsonElement> MutateAsync(string command, string key, string? payload, CancellationToken cancellationToken)
    {
        await _mutation.WaitAsync(cancellationToken);
        try
        {
            return await SendAsync(command, cancellationToken, key, payload);
        }
        finally
        {
            _mutation.Release();
        }
    }

    private async Task<JsonElement> SendAsync(
        string command, CancellationToken cancellationToken, string? key = null, string? payload = null,
        string? password = null, bool interruptible = false)
    {
        WorkerProcess worker;
        await _lifecycle.WaitAsync(cancellationToken);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_worker is null || _worker.Closed)
            {
                if (_worker is not null)
                    await _worker.DisposeAsync();
                Directory.CreateDirectory(_directory);
                var start = new ProcessStartInfo(_options.NodeExecutable) { UseShellExecute = false, CreateNoWindow = true };
                start.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "Sync.NET.KineDB", "worker.mjs"));
                foreach (var argument in new[] { _packageDirectory, _directory, _mapping.Name, _mapping.SchemaId,
                    _options.Endpoint.AbsoluteUri, _options.User })
                {
                    start.ArgumentList.Add(argument);
                }
                _worker = new WorkerProcess(start);
            }
            worker = _worker;
        }
        finally
        {
            _lifecycle.Release();
        }

        return await worker.SendAsync(command, key, payload, password, interruptible, cancellationToken);
    }

    private T Map(JsonElement row)
    {
        var value = JsonSerializer.Deserialize<T>(row[1].GetString()!, _mapping.JsonOptions)
            ?? throw new InvalidDataException("A replica row has an empty payload.");
        if (!string.Equals(_mapping.KeySelector(value), row[0].GetString(), StringComparison.Ordinal))
            throw new InvalidDataException("A replica row key does not match its payload.");
        return value;
    }

    private static PushResult ReadPush(JsonElement response)
    {
        if (!response.TryGetProperty("sent", out var count) || !count.TryGetInt32(out var sent) || sent < 0 ||
            !response.TryGetProperty("outcomes", out var reported) || reported.ValueKind != JsonValueKind.Array)
            throw new InvalidDataException("Invalid KineDB push report.");

        var outcomes = reported.EnumerateArray().Select(item => new WriteOutcome(
            OptionalText(item, "status") switch
            {
                "applied" => WriteStatus.Applied,
                "conflict" => WriteStatus.Conflict,
                "rejected" => WriteStatus.Rejected,
                "merged" => WriteStatus.Merged,
                _ => WriteStatus.Unknown
            },
            OptionalText(item, "seq"), item.ValueKind == JsonValueKind.Object &&
                item.TryGetProperty("replayed", out var replayed) && replayed.ValueKind == JsonValueKind.True,
            OptionalText(item, "version"), OptionalText(item, "serverVersion"), OptionalText(item, "reason"))).ToArray();
        var complete = sent == outcomes.Length &&
            outcomes.All(outcome => !string.IsNullOrEmpty(outcome.Sequence) && outcome.Status != WriteStatus.Unknown) &&
            outcomes.Select(outcome => outcome.Sequence).Distinct(StringComparer.Ordinal).Count() == sent;
        return new(sent, Array.AsReadOnly(outcomes), complete);
    }

    private static PullResult ReadPull(JsonElement response) => new(
        response.GetProperty("upserts").GetInt32(), response.GetProperty("removes").GetInt32(),
        response.GetProperty("full").GetBoolean(), OptionalText(response, "token"),
        OptionalText(response, "resumedFrom"), response.TryGetProperty("gap", out var gap) && gap.ValueKind == JsonValueKind.Object
            ? OptionalText(gap, "reason") ?? "unknown" : null);

    private static string? OptionalText(JsonElement obj, string name) =>
        obj.ValueKind == JsonValueKind.Object && obj.TryGetProperty(name, out var value) &&
            value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static void ValidateName(string name)
    {
        if (string.IsNullOrEmpty(name) || !Regex.IsMatch(name, "\\A[a-z][a-z0-9_]{0,62}\\z") ||
            name.StartsWith("syncnet_", StringComparison.Ordinal) || name.StartsWith("kinedb_", StringComparison.Ordinal))
        {
            throw new ArgumentException("Use a lowercase collection name with at most 63 letters, digits, or underscores.", nameof(name));
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _lifecycle.WaitAsync();
        try
        {
            if (_disposed) return;
            _disposed = true;
            if (_worker is not null) await _worker.DisposeAsync();
        }
        finally
        {
            _lifecycle.Release();
        }
    }
}
