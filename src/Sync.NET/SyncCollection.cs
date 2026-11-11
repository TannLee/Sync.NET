using System.Text.Json;

namespace SyncDotNet;

public sealed class CollectionMapping<T>(
    string name, string schemaId, Func<T, string> keySelector, JsonSerializerOptions? jsonOptions = null)
    where T : class
{
    public string Name { get; } = name;
    public string SchemaId { get; } = schemaId;
    public Func<T, string> KeySelector { get; } = keySelector;
    public JsonSerializerOptions? JsonOptions { get; } = jsonOptions;
}

public interface ISyncCollection<T> : IAsyncDisposable where T : class
{
    Task<T?> GetAsync(string key, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<T>> ListAsync(CancellationToken cancellationToken = default);
    Task PutAsync(T value, CancellationToken cancellationToken = default);
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);
    Task<SyncResult> SynchronizeAsync(
        IProgress<SyncStage>? progress = null, CancellationToken cancellationToken = default);
}

public enum SyncStage { CheckingServer, Pushing, Pulling, Completed }
public enum SyncStatus { UpToDate, Partial, Deferred, Cancelled, Failed }
public enum WriteStatus { Applied, Conflict, Rejected, Merged, Unknown }

public sealed record WriteOutcome(
    WriteStatus Status, string? Sequence, bool Replayed,
    string? Version, string? ServerVersion, string? Reason);

public sealed record PushResult(int Sent, IReadOnlyList<WriteOutcome> Outcomes, bool OutcomesComplete);
public sealed record PullResult(
    int Upserts, int Removes, bool Full, string? Checkpoint, string? ResumedFrom, string? GapReason);

public sealed record SyncResult(
    SyncStatus Status, SyncStage Stage, PushResult? Push = null, PullResult? Pull = null,
    bool PushOutcomeUnknown = false, string? Error = null);
