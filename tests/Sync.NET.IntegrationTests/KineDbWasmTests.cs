using SyncDotNet.Feasibility;

namespace SyncDotNet.IntegrationTests;

public sealed class KineDbWasmTests
{
    [KineDbPackageFact]
    public async Task Offline_crud_survives_clean_process_restart()
    {
        var package = Environment.GetEnvironmentVariable("SYNCNET_KINEDB_WASM_PACKAGE");

        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        await OfflineProbe.RunAsync(new KineDbWasm(package!), timeout.Token);
    }
}


public sealed class KineDbPackageFactAttribute : FactAttribute
{
    public KineDbPackageFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("SYNCNET_KINEDB_WASM_PACKAGE")))
            Skip = "BLOCKED: set SYNCNET_KINEDB_WASM_PACKAGE to the pinned extracted package directory.";
    }
}
