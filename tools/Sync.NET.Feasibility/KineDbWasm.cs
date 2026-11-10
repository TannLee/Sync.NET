using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;

namespace SyncDotNet.Feasibility;

public sealed class KineDbWasm : ILocalSql
{
    public const string Version = "0.0.0-g3c2ace4d5485";
    private const string WasmHash = "A8A30778D607FD524A0AF4893CA100062AF173F37C0707F0341CEB62B117BF81";
    private readonly string _packageDirectory;

    public KineDbWasm(string packageDirectory)
    {
        _packageDirectory = Path.GetFullPath(packageDirectory);
        var manifest = Path.Combine(_packageDirectory, "package.json");
        if (!File.Exists(manifest))
            throw new FileNotFoundException($"Install pinned @kinedb/embedded {Version} first.", manifest);

        using var metadata = JsonDocument.Parse(File.ReadAllText(manifest));
        if (metadata.RootElement.GetProperty("name").GetString() != "@kinedb/embedded" ||
            metadata.RootElement.GetProperty("version").GetString() != Version)
            throw new InvalidDataException($"Expected @kinedb/embedded {Version}.");

        using var wasm = File.OpenRead(Path.Combine(_packageDirectory, "src", "kinedb.wasm"));
        if (Convert.ToHexString(SHA256.HashData(wasm)) != WasmHash)
            throw new InvalidDataException("KineDB Wasm checksum does not match the Phase 0 pin.");
    }

    public async Task<JsonElement[]> ExecuteAsync(
        string dataDirectory, IReadOnlyList<string> statements, CancellationToken cancellationToken)
    {
        var start = new ProcessStartInfo("node")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        start.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "kinedb-probe.mjs"));
        start.ArgumentList.Add(_packageDirectory);
        start.ArgumentList.Add(dataDirectory);
        start.ArgumentList.Add(JsonSerializer.Serialize(statements));

        using var process = Process.Start(start) ?? throw new InvalidOperationException("Could not start Node.");
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync();
            throw;
        }

        var output = await stdout;
        var diagnostics = await stderr;
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"KineDB Node process exited {process.ExitCode}: {diagnostics}");

        return JsonSerializer.Deserialize<JsonElement[]>(output)
            ?? throw new InvalidDataException("KineDB Node process returned no result.");
    }
}
