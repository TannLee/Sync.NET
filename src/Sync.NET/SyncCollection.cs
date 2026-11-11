using System.Text.Json;

namespace SyncDotNet;

/// <summary>
/// Maps a domain object to a collection, a stable schema identifier and a storage key.
/// Keep the schema identifier stable while the serialized representation remains compatible.
/// </summary>
public sealed class CollectionMapping<T>(
    string name, string schemaId, Func<T, string> keySelector, JsonSerializerOptions? jsonOptions = null)
    where T : class
{
    /// <summary>
    /// Gets the collection name used by the provider.
    /// </summary>
    public string Name { get; } = name;
    /// <summary>
    /// Gets the caller-defined identity of the serialized schema.
    /// </summary>
    public string SchemaId { get; } = schemaId;
    /// <summary>
    /// Gets the function that returns the stable key for a row.
    /// </summary>
    public Func<T, string> KeySelector { get; } = keySelector;
    /// <summary>
    /// Gets the JSON options used to encode and decode values.
    /// </summary>
    public JsonSerializerOptions? JsonOptions { get; } = jsonOptions;
}

/// <summary>
/// Provides local replica operations and an explicit synchronization boundary.
/// Dispose the collection when its replica is no longer needed.
/// </summary>
public interface ISyncCollection<T> : IAsyncDisposable where T : class
{
    /// <summary>
    /// Reads a row from the local replica or returns null when it is absent.
    /// </summary>
    Task<T?> GetAsync(string key, CancellationToken cancellationToken = default);
    /// <summary>
    /// Reads all rows available in the configured local scope.
    /// </summary>
    Task<IReadOnlyList<T>> ListAsync(CancellationToken cancellationToken = default);
    /// <summary>
    /// Inserts or replaces a row locally. Completion means the provider accepted the local write.
    /// </summary>
    Task PutAsync(T value, CancellationToken cancellationToken = default);
    /// <summary>
    /// Deletes a row locally and returns whether a row was removed.
    /// </summary>
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);
    /// <summary>
    /// Runs synchronization and reports progress and the outcome of each completed stage.
    /// </summary>
    Task<SyncResult> SynchronizeAsync(
        IProgress<SyncStage>? progress = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// Identifies the stage reached by a synchronization run.
/// </summary>
public enum SyncStage { CheckingServer, Pushing, Pulling, Completed }
/// <summary>
/// Summarizes a run, including deferred work, partial completion and cancellation.
/// </summary>
public enum SyncStatus { UpToDate, Partial, Deferred, Cancelled, Failed }
/// <summary>
/// Represents the server outcome for an individual write.
/// </summary>
public enum WriteStatus { Applied, Conflict, Rejected, Merged, Unknown }
/// <summary>
/// Classifies a reported failure so callers can decide whether to retry or change configuration.
/// </summary>
public enum SyncFailureKind { Configuration, Transient, Provider, UnknownPushOutcome, InvalidResponse }

/// <summary>
/// Exposes the provider's retained records of conflicts and other refused writes.
/// </summary>
public interface IRefusedWriteReader
{
    /// <summary>
    /// Lists retained refusals without changing or acknowledging them.
    /// </summary>
    Task<IReadOnlyList<RefusedWrite>> ListRefusedAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Supports reloading the current replica scope after outstanding writes have been handled.
/// </summary>
public interface ISyncScopeRecovery
{
    /// <summary>
    /// Attempts to send pending writes before requesting a full reload of the bound scope.
    /// </summary>
    Task<SyncResult> RecoverScopeAsync(IProgress<SyncStage>? progress = null, CancellationToken cancellationToken = default);
}

/// <summary>
/// Describes a refused operation and the versions returned by the provider.
/// Key preserves the provider's JSON representation. Version identifiers are opaque strings.
/// </summary>
public sealed record RefusedWrite(string? Sequence, string? Operation, JsonElement Key,
    string? Code, string? Reason, string? ReadVersion, string? ServerVersion);

/// <summary>
/// Describes an individual push result. Replayed indicates that the server returned a previously recorded outcome.
/// </summary>
public sealed record WriteOutcome(
    WriteStatus Status, string? Sequence, bool Replayed,
    string? Version, string? ServerVersion, string? Reason);

/// <summary>
/// Reports the operations sent and their outcomes for the final push attempt.
/// OutcomesComplete indicates whether all operations in that attempt had usable, distinct results.
/// </summary>
public sealed record PushResult(int Sent, IReadOnlyList<WriteOutcome> Outcomes, bool OutcomesComplete)
{
    /// <summary>
    /// Gets the number of extra push attempts made during the synchronization run.
    /// </summary>
    public int RetryAttempts { get; init; }
}
/// <summary>
/// Reports rows added or removed, the checkpoint reached and any gap reported by the provider.
/// </summary>
public sealed record PullResult(
    int Upserts, int Removes, bool Full, string? Checkpoint, string? ResumedFrom, string? GapReason)
{
    /// <summary>
    /// Gets the server commit of the bootstrap snapshot when one was used.
    /// </summary>
    public string? SnapshotCommit { get; init; }
}

/// <summary>
/// Reports the outcome of a synchronization run and preserves results from completed stages.
/// PushOutcomeUnknown signals that an operation may have committed without a usable acknowledgment.
/// </summary>
public sealed record SyncResult(
    SyncStatus Status, SyncStage Stage, PushResult? Push = null, PullResult? Pull = null,
    bool PushOutcomeUnknown = false, string? Error = null)
{
    /// <summary>
    /// Gets a machine-readable failure category when available.
    /// </summary>
    public SyncFailureKind? FailureKind { get; init; }
}
