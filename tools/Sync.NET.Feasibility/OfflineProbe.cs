using System.Text.Json;

namespace SyncDotNet.Feasibility;

public static class OfflineProbe
{
    private const string Read = "SELECT id, payload FROM probe_rows ORDER BY id";

    public static async Task RunAsync(ILocalSql engine, CancellationToken cancellationToken)
    {
        var directory = Directory.CreateTempSubdirectory("sync-net-phase0-");
        try
        {
            var changes = await engine.ExecuteAsync(directory.FullName,
                [
                    "CREATE TABLE probe_rows (id INT PRIMARY KEY, payload TEXT)",
                    "INSERT INTO probe_rows (id, payload) VALUES (1, 'initial'), (2, 'remove')",
                    Read,
                    "UPDATE probe_rows SET payload = 'updated' WHERE id = 1",
                    Read,
                    "DELETE FROM probe_rows WHERE id = 2",
                    Read
                ], cancellationToken);
            Expect(changes[2], """[[1,"initial"],[2,"remove"]]""", "insert");
            Expect(changes[4], """[[1,"updated"],[2,"remove"]]""", "update");
            Expect(changes[6], """[[1,"updated"]]""", "delete");

            var reopened = await engine.ExecuteAsync(directory.FullName, [Read], cancellationToken);
            Expect(reopened[0], """[[1,"updated"]]""", "clean restart");
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    private static void Expect(JsonElement actual, string expected, string stage)
    {
        using var expectedRows = JsonDocument.Parse(expected);
        if (!JsonElement.DeepEquals(actual, expectedRows.RootElement))
            throw new InvalidDataException($"{stage}: expected {expected}, got {actual}.");
    }
}
