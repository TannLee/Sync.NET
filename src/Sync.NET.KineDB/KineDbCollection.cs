using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace SyncDotNet.KineDB;

/// <summary>
/// Configures the pinned package, local replica directory and server identity.
/// The password callback supplies credentials when the provider needs to contact the server.
/// </summary>
public sealed record KineDbReplicaOptions(
    string PackageDirectory, string ReplicaDirectory, Uri Endpoint, string User,
    Func<CancellationToken, ValueTask<string>> GetPassword)
{
    /// <summary>
    /// Gets the Node.js executable name or path.
    /// </summary>
    public string NodeExecutable { get; init; } = "node";
    /// <summary>
    /// Gets an optional exact key filter. Each scope requires its own replica directory.
    /// </summary>
    public string? ScopeKey { get; init; }
    /// <summary>
    /// Gets the optional maximum UTF-8 size of a serialized local row.
    /// </summary>
    public int? MaxPayloadBytes { get; init; }
    /// <summary>
    /// Gets the optional cap on local operations that are executing or waiting for the mutation lock.
    /// </summary>
    public int? MaxPendingLocalOperations { get; init; }
    /// <summary>
    /// Gets the number of optional transient push retries, from zero to two, within the replay window.
    /// </summary>
    public int PushRetryCount { get; init; }
}

internal static class KineDbMetrics
{
    private static readonly Meter Meter = new("SyncDotNet.KineDB");
    internal static readonly Counter<long> Runs = Meter.CreateCounter<long>("syncnet.sync.runs");
    internal static readonly Counter<long> PushRetries = Meter.CreateCounter<long>("syncnet.push.retries");
    internal static readonly Histogram<double> Duration = Meter.CreateHistogram<double>("syncnet.sync.duration", "ms");
}

/// <summary>
/// Implements a typed collection backed by a single local KineDB replica and its native journal.
/// The server table must match the schema produced by CreateTableSql.
/// </summary>
public sealed class KineDbCollection<T> : ISyncCollection<T>, IRefusedWriteReader, ISyncScopeRecovery where T : class
{
    private readonly CollectionMapping<T> _mapping;
    private readonly KineDbReplicaOptions _options;
    private readonly string _packageDirectory;
    private readonly string _directory;
    private readonly bool _files;
    private readonly bool _snapshotBootstrap;
    private readonly SemaphoreSlim _lifecycle = new(1);
    // The pinned engine's pull can replace a row edited during a push.
    // Hold writes until push and pull both finish so acknowledged edits survive.
    private readonly SemaphoreSlim _mutation = new(1);
    private readonly SemaphoreSlim? _localAdmission;
    private WorkerProcess? _worker;
    private bool _disposed;

    /// <summary>
    /// Configures a replica. The worker is started lazily on the first operation.
    /// </summary>
    public KineDbCollection(CollectionMapping<T> mapping, KineDbReplicaOptions options) : this(mapping, options, false) { }

    internal KineDbCollection(CollectionMapping<T> mapping, KineDbReplicaOptions options, bool files,
        bool snapshotBootstrap = false)
    {
        ArgumentNullException.ThrowIfNull(mapping);
        ArgumentNullException.ThrowIfNull(options);
        ValidateName(mapping.Name);
        ArgumentException.ThrowIfNullOrWhiteSpace(mapping.SchemaId);
        ArgumentNullException.ThrowIfNull(mapping.KeySelector);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.User);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.NodeExecutable);
        ArgumentNullException.ThrowIfNull(options.GetPassword);
        if (options.MaxPayloadBytes is <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxPayloadBytes must be positive.");
        if (options.MaxPendingLocalOperations is <= 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxPendingLocalOperations must be positive.");
        if (options.PushRetryCount is < 0 or > 2)
            throw new ArgumentOutOfRangeException(nameof(options), "PushRetryCount must be between 0 and 2.");
        if (options.ScopeKey is { } scope && !Regex.IsMatch(scope, @"\A[A-Za-z0-9_-]{1,128}\z"))
            throw new ArgumentException("ScopeKey must contain 1–128 ASCII letters, digits, underscores or hyphens.", nameof(options));
        ArgumentNullException.ThrowIfNull(options.Endpoint);
        var endpoint = options.Endpoint;
        if (!endpoint.IsAbsoluteUri || (endpoint.Scheme != "wss" && !(endpoint.Scheme == "ws" && endpoint.IsLoopback)) ||
            endpoint.AbsolutePath != "/ws" || endpoint.UserInfo.Length != 0 ||
            endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0)
        {
            throw new ArgumentException("Use a /ws endpoint over wss, or ws on loopback.", nameof(options));
        }

        _mapping = mapping;
        _files = files;
        _snapshotBootstrap = snapshotBootstrap;
        _options = options;
        if (options.MaxPendingLocalOperations is { } limit)
            _localAdmission = new SemaphoreSlim(limit, limit);
        _packageDirectory = Path.GetFullPath(options.PackageDirectory);
        _directory = Path.GetFullPath(options.ReplicaDirectory);
    }

    /// <summary>
    /// Returns the server DDL for a text key and a non-null JSON payload stored as text.
    /// </summary>
    public static string CreateTableSql(string name)
    {
        ValidateName(name);
        return $"CREATE TABLE IF NOT EXISTS \"{name}\" (id TEXT PRIMARY KEY, payload TEXT NOT NULL)";
    }

    /// <summary>
    /// Bootstraps into a sibling staging directory, checks the checkpoint and typed rows, then activates the new replica.
    /// The target directory must be absent. A failed bootstrap leaves the target unavailable.
    /// </summary>
    public static async Task<SyncResult> BootstrapAsync(CollectionMapping<T> mapping, KineDbReplicaOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var target = Path.GetFullPath(options.ReplicaDirectory);
        if (Path.Exists(target))
            throw new IOException("Bootstrap requires a new replica directory.");
        var parent = Path.GetDirectoryName(target) ?? throw new ArgumentException("Invalid replica directory.", nameof(options));
        Directory.CreateDirectory(parent);
        var stage = Directory.CreateDirectory(Path.Combine(parent, ".syncnet-bootstrap-" + Guid.NewGuid().ToString("N")));
        try
        {
            SyncResult result;
            await using (var replica = new KineDbCollection<T>(mapping, options with { ReplicaDirectory = stage.FullName },
                files: false, snapshotBootstrap: true))
            {
                result = await replica.SynchronizeAsync(cancellationToken: cancellationToken);
                if (result.Status != SyncStatus.UpToDate) return result;
                if (string.IsNullOrEmpty(result.Pull?.Checkpoint))
                    return new(SyncStatus.Failed, SyncStage.Pulling, Pull: result.Pull, Error: "Bootstrap did not receive a checkpoint.")
                        { FailureKind = SyncFailureKind.InvalidResponse };
                await replica.ListAsync(cancellationToken);
            }
            cancellationToken.ThrowIfCancellationRequested();
            // Closing the staged worker releases its storage handles before the directory move.
            Directory.Move(stage.FullName, target);
            return result;
        }
        finally
        {
            if (Directory.Exists(stage.FullName))
                stage.Delete(recursive: true);
        }
    }

    /// <inheritdoc/>
    public async Task<T?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        var rows = await SendAsync("get", cancellationToken, key: key);
        return rows.GetArrayLength() == 0 ? null : Map(rows[0]);
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<T>> ListAsync(CancellationToken cancellationToken = default)
    {
        var rows = await SendAsync("list", cancellationToken);
        return rows.EnumerateArray().Select(Map).ToArray();
    }

    /// <inheritdoc/>
    public async Task PutAsync(T value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        var key = _mapping.KeySelector(value);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        RequireWritableKey(key);
        var payload = JsonSerializer.Serialize(value, _mapping.JsonOptions);
        if (_options.MaxPayloadBytes is { } limit && Encoding.UTF8.GetByteCount(payload) > limit)
            throw new ArgumentException("The serialized row exceeds MaxPayloadBytes.", nameof(value));
        await MutateAsync("put", key, payload, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        RequireWritableKey(key);
        return (await MutateAsync("delete", key, null, cancellationToken)).GetBoolean();
    }

    private void RequireWritableKey(string key)
    {
        if (_options.ScopeKey is { } scope && !string.Equals(key, scope, StringComparison.Ordinal))
            throw new ArgumentException("The key is outside the replica scope.", nameof(key));
    }

    /// <inheritdoc/>
    public Task<SyncResult> SynchronizeAsync(
        IProgress<SyncStage>? progress = null, CancellationToken cancellationToken = default) =>
        MeasureSyncAsync(false, progress, cancellationToken);

    /// <inheritdoc/>
    public Task<SyncResult> RecoverScopeAsync(
        IProgress<SyncStage>? progress = null, CancellationToken cancellationToken = default) =>
        MeasureSyncAsync(true, progress, cancellationToken);

    private async Task<SyncResult> MeasureSyncAsync(
        bool full, IProgress<SyncStage>? progress, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var result = await SynchronizeCoreAsync(full, progress, cancellationToken);
        var status = new KeyValuePair<string, object?>("status", result.Status.ToString());
        KineDbMetrics.Runs.Add(1, status);
        KineDbMetrics.Duration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, status);
        return result;
    }

    private async Task<SyncResult> SynchronizeCoreAsync(
        bool full, IProgress<SyncStage>? progress, CancellationToken cancellationToken)
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
                var (response, retries) = await PushAsync(password, cancellationToken);
                push = ReadPush(response) with { RetryAttempts = retries };
                if (!push.OutcomesComplete)
                {
                    // Preserve the uncertain push state for the caller before attempting a pull.
                    return new(SyncStatus.Partial, stage, push, PushOutcomeUnknown: true,
                        Error: "KineDB did not return a usable outcome for every write.")
                        { FailureKind = SyncFailureKind.UnknownPushOutcome };
                }
                if (full && push.Outcomes.Any(outcome => outcome.Status is WriteStatus.Conflict or WriteStatus.Rejected))
                    return new(SyncStatus.Partial, stage, push, Error: "Scope recovery stopped after a refused write.");
            }

            stage = SyncStage.Pulling;
            progress?.Report(stage);
            pull = ReadPull(await SendAsync(full ? "recover" : "pull", cancellationToken, password: password, interruptible: true));
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

    private async Task<(JsonElement Response, int Retries)> PushAsync(string password, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            return (await SendAsync("push", cancellationToken, password: password, interruptible: true), 0);
        }
        catch (KineDbOperationException error) when (error.Retryable && _options.PushRetryCount > 0)
        {
            // Count the first attempt against the time budget to keep retries close
            // to the original push, where replay has been verified.
            var remaining = TimeSpan.FromSeconds(5) - Stopwatch.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero) throw;

            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(remaining);
            try
            {
                for (var retry = 0; ; retry++)
                {
                    await Task.Delay(100 * (1 << retry) + Random.Shared.Next(50), deadline.Token);
                    KineDbMetrics.PushRetries.Add(1);
                    try
                    {
                        return (await SendAsync("push", deadline.Token, password: password, interruptible: true), retry + 1);
                    }
                    catch (KineDbOperationException retryError) when (retryError.Retryable && retry + 1 < _options.PushRetryCount)
                    {
                        // The native journal retains operations whose acknowledgments were lost.
                    }
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new KineDbOperationException("The push retry window expired.", true, true, false);
            }
            catch (KineDbOperationException retryError)
            {
                throw new KineDbOperationException(retryError.Message, retryError.Retryable, true, retryError.Configuration);
            }
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

    /// <summary>
    /// Reads refusal records retained by KineDB. The worker may need server credentials to open a session.
    /// </summary>
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

    internal async Task<JsonElement> MutateAsync(string command, string key, string? payload, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_localAdmission is not null && !_localAdmission.Wait(0))
            throw new IOException("The replica local operation queue is full.");
        try
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
        finally
        {
            _localAdmission?.Release();
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
                WorkerProcess.AddScriptArguments(start);
                foreach (var argument in new[] { _packageDirectory, _directory, _mapping.Name, _mapping.SchemaId,
                    _options.Endpoint.AbsoluteUri, _options.User })
                {
                    start.ArgumentList.Add(argument);
                }
                if (_options.ScopeKey is { } scope)
                    start.ArgumentList.Add(scope);
                else if (_files || _snapshotBootstrap)
                    start.ArgumentList.Add("");
                if (_files)
                    start.ArgumentList.Add("files");
                else if (_snapshotBootstrap)
                    start.ArgumentList.Add("bootstrap");
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
            ? OptionalText(gap, "reason") ?? "unknown" : null)
        { SnapshotCommit = response.TryGetProperty("pack", out var pack) ? OptionalText(pack, "commit") : null };

    private static string? OptionalText(JsonElement obj, string name) =>
        obj.ValueKind == JsonValueKind.Object && obj.TryGetProperty(name, out var value) &&
            value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    internal static void ValidateName(string name)
    {
        if (string.IsNullOrEmpty(name) || !Regex.IsMatch(name, "\\A[a-z][a-z0-9_]{0,62}\\z") ||
            name.StartsWith("syncnet_", StringComparison.Ordinal) || name.StartsWith("kinedb_", StringComparison.Ordinal))
        {
            throw new ArgumentException("Use a lowercase collection name with at most 63 letters, digits, or underscores.", nameof(name));
        }
    }

    /// <summary>
    /// Closes the local worker and releases the replica's storage handles.
    /// </summary>
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
