using System.Text.Json;

namespace SyncDotNet;

/// <summary>
/// Builds a read-only view over local replicas that may contain some of the same keys.
/// </summary>
public static class SyncScopeView
{
    /// <summary>
    /// Returns one row per key in ordinal order. Rows with the same key must serialize to equal JSON.
    /// Reads occur sequentially. Each replica supplies its own local snapshot.
    /// </summary>
    public static async Task<IReadOnlyList<T>> ReadUnionAsync<T>(
        CollectionMapping<T> mapping, IEnumerable<ISyncCollection<T>> replicas,
        CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(mapping);
        ArgumentNullException.ThrowIfNull(replicas);
        var rows = new SortedDictionary<string, T>(StringComparer.Ordinal);
        foreach (var replica in replicas)
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var row in await replica.ListAsync(cancellationToken))
            {
                var key = mapping.KeySelector(row);
                if (rows.TryAdd(key, row)) continue;
                var existing = JsonSerializer.SerializeToElement(rows[key], mapping.JsonOptions);
                var incoming = JsonSerializer.SerializeToElement(row, mapping.JsonOptions);
                if (!JsonElement.DeepEquals(existing, incoming))
                    throw new InvalidDataException("Overlapping replicas contain different values for one key.");
            }
        }
        return rows.Values.ToArray();
    }
}
