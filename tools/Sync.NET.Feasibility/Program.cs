using SyncDotNet.Feasibility;

if (args.Length != 1)
{
    Console.Error.WriteLine("Usage: Sync.NET.Feasibility <extracted KineDB package directory>");
    return 2;
}

try
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
    await OfflineProbe.RunAsync(new KineDbWasm(args[0]), timeout.Token);
    Console.WriteLine("PASS: KineDB Wasm offline CRUD and clean process restart");
    Console.WriteLine("BLOCKED: in-process .NET native");
    Console.WriteLine("NOT RUN: server push/pull, conflicts, lost-response replay");
    return 0;
}
catch (FileNotFoundException error)
{
    Console.Error.WriteLine($"BLOCKED: {error.Message}");
    return 2;
}
catch (Exception error)
{
    Console.Error.WriteLine($"FAIL: {error.Message}");
    return 1;
}
