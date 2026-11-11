using System.Text.Json;

namespace SyncDotNet.KineDB;

/// <summary>
/// Carries the byte count and BLAKE3 digest reported by KineDB for a stored attachment.
/// </summary>
public sealed record AttachmentReceipt(long Bytes, string Blake3);

/// <summary>
/// Stores and synchronizes files in a dedicated KineDB table.
/// Transfers use temporary files to exchange streams with the Node worker and enforce a per-operation byte limit.
/// </summary>
public sealed class KineDbAttachments : IAsyncDisposable, ISyncScopeRecovery
{
    private readonly KineDbCollection<FileKey> _collection;
    private readonly long _maxBytes;

    /// <summary>
    /// Creates an attachment replica for a dedicated table. Synchronize once before reading or writing files.
    /// The byte budget applies to local upload and download operations.
    /// </summary>
    public KineDbAttachments(string table, KineDbReplicaOptions options, long maxBytes = 64 * 1024 * 1024)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (maxBytes is <= 0 or > 1_073_741_824)
            throw new ArgumentOutOfRangeException(nameof(maxBytes));
        if (options.ScopeKey is not null)
            throw new ArgumentException("Attachment replicas use a dedicated table.", nameof(options));

        _maxBytes = maxBytes;
        _collection = new(new(table, "syncnet-file-v1", file => file.Name), options, files: true);
    }

    /// <summary>
    /// Returns the server DDL for a table keyed by attachment name.
    /// </summary>
    public static string CreateTableSql(string table)
    {
        KineDbCollection<FileKey>.ValidateName(table);
        return $"CREATE TABLE IF NOT EXISTS \"{table}\" (name TEXT PRIMARY KEY)";
    }

    /// <summary>
    /// Stages the input stream and writes its content to the local journal. The caller owns the input stream.
    /// </summary>
    public async Task<AttachmentReceipt> PutAsync(string name, Stream source, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(source);
        var directory = Directory.CreateTempSubdirectory("syncnet-file-");
        var path = Path.Combine(directory.FullName, "content");
        try
        {
            long bytes = 0;
            await using (var stage = File.Create(path))
            {
                var buffer = new byte[64 * 1024];
                int read;
                while ((read = await source.ReadAsync(buffer, cancellationToken)) > 0)
                {
                    bytes += read;
                    if (bytes > _maxBytes)
                        throw new IOException("Attachment exceeds its byte budget.");
                    await stage.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
                }
            }
            var result = await _collection.MutateAsync("put-file", name, path, cancellationToken);
            return new(result.GetProperty("size").GetInt64(), result.GetProperty("hash").GetString()!);
        }
        finally
        {
            File.Delete(path);
            directory.Delete();
        }
    }

    /// <summary>
    /// Copies a local attachment into the caller-owned stream and returns false if the name is absent.
    /// A failed or cancelled copy to the destination may leave it partially written.
    /// </summary>
    public async Task<bool> CopyToAsync(string name, Stream destination, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(destination);
        var directory = Directory.CreateTempSubdirectory("syncnet-file-");
        var path = Path.Combine(directory.FullName, "content");
        try
        {
            var payload = JsonSerializer.Serialize(new { path, maxBytes = _maxBytes });
            var exists = (await _collection.MutateAsync("get-file", name, payload, cancellationToken)).GetBoolean();
            if (!exists) return false;
            await using var stage = File.OpenRead(path);
            await stage.CopyToAsync(destination, cancellationToken);
            return true;
        }
        finally
        {
            File.Delete(path);
            directory.Delete();
        }
    }

    /// <summary>
    /// Deletes a local attachment and records the deletion for the next push.
    /// </summary>
    public Task<bool> DeleteAsync(string name, CancellationToken cancellationToken = default) =>
        _collection.DeleteAsync(name, cancellationToken);

    /// <summary>
    /// Pushes local attachment changes and pulls changes into the replica.
    /// </summary>
    public Task<SyncResult> SynchronizeAsync(IProgress<SyncStage>? progress = null, CancellationToken cancellationToken = default) =>
        _collection.SynchronizeAsync(progress, cancellationToken);

    /// <summary>
    /// Pushes pending writes before requesting a complete reload of the attachment table.
    /// </summary>
    public Task<SyncResult> RecoverScopeAsync(IProgress<SyncStage>? progress = null, CancellationToken cancellationToken = default) =>
        _collection.RecoverScopeAsync(progress, cancellationToken);

    /// <summary>
    /// Closes the replica and releases its worker.
    /// </summary>
    public ValueTask DisposeAsync() => _collection.DisposeAsync();

    private sealed record FileKey(string Name);
}
