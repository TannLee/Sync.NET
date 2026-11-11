namespace SyncDotNet.IntegrationTests;

public sealed class SyncPlanTests
{
    [Fact]
    public async Task Failed_dependency_defers_child_without_stopping_independent_steps()
    {
        var calls = new List<string>();
        SyncPlanStep Step(string name, string[] dependencies, SyncStatus status) => new(name, dependencies, _ =>
        {
            calls.Add(name);
            return Task.FromResult(new SyncResult(status, SyncStage.Completed));
        });

        var results = await SyncPlan.RunAsync([
            Step("child", ["parent"], SyncStatus.UpToDate),
            Step("parent", [], SyncStatus.Failed),
            Step("independent", [], SyncStatus.UpToDate)
        ]);
        Assert.Equal(["parent", "independent"], calls);
        var child = Assert.Single(results, result => result.Name == "child");
        Assert.Equal(SyncStatus.Deferred, child.Result.Status);
        Assert.Equal(["parent"], child.BlockingDependencies);
    }

    [Fact]
    public async Task Cycle_is_rejected_before_sync_starts()
    {
        static Task<SyncResult> Unexpected(CancellationToken _) => throw new InvalidOperationException();
        await Assert.ThrowsAsync<ArgumentException>(() => SyncPlan.RunAsync([
            new("a", ["b"], Unexpected),
            new("b", ["a"], Unexpected)
        ]));
    }
}
