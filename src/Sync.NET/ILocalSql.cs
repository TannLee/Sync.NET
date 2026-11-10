using System.Text.Json;

namespace SyncDotNet;

// Phase 0 seam for local SQL batches. Each statement commits independently.
public interface ILocalSql
{
    Task<JsonElement[]> ExecuteAsync(
        string dataDirectory, IReadOnlyList<string> statements, CancellationToken cancellationToken);
}
