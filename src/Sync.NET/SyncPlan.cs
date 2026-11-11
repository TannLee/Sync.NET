namespace SyncDotNet;

/// <summary>
/// Defines a named synchronization operation and the names of steps that must succeed first.
/// </summary>
public sealed record SyncPlanStep(string Name, IReadOnlyList<string> DependsOn,
    Func<CancellationToken, Task<SyncResult>> Synchronize);

/// <summary>
/// Carries a step result and the immediate dependencies that prevented it from running.
/// </summary>
public sealed record SyncPlanResult(string Name, SyncResult Result, IReadOnlyList<string> BlockingDependencies);

/// <summary>
/// Runs collection-level synchronization in dependency order. Each step owns its own commit boundary.
/// </summary>
public static class SyncPlan
{
    /// <summary>
    /// Validates the entire dependency graph before calling any step. Independent steps can proceed after a failure.
    /// </summary>
    public static async Task<IReadOnlyList<SyncPlanResult>> RunAsync(
        IEnumerable<SyncPlanStep> steps, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(steps);
        var byName = steps.ToDictionary(step => step.Name, StringComparer.Ordinal);
        var ordered = new List<SyncPlanStep>();
        var visiting = new HashSet<string>(StringComparer.Ordinal);
        var visited = new HashSet<string>(StringComparer.Ordinal);
        foreach (var step in byName.Values)
            Visit(step);

        var results = new Dictionary<string, SyncPlanResult>(StringComparer.Ordinal);
        foreach (var step in ordered)
        {
            var blocked = step.DependsOn.Where(name => results[name].Result.Status != SyncStatus.UpToDate).ToArray();
            SyncResult result;
            if (cancellationToken.IsCancellationRequested)
                result = new(SyncStatus.Cancelled, SyncStage.CheckingServer);
            else if (blocked.Length != 0)
                result = new(SyncStatus.Deferred, SyncStage.CheckingServer, Error: "A dependency did not complete.");
            else
            {
                try
                {
                    result = await step.Synchronize(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    result = new(SyncStatus.Cancelled, SyncStage.CheckingServer);
                }
                catch (Exception)
                {
                    result = new(SyncStatus.Failed, SyncStage.CheckingServer, Error: "The synchronization step threw an exception.")
                        { FailureKind = SyncFailureKind.Provider };
                }
            }
            results.Add(step.Name, new(step.Name, result, blocked));
        }
        return ordered.Select(step => results[step.Name]).ToArray();

        void Visit(SyncPlanStep step)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(step.Name);
            ArgumentNullException.ThrowIfNull(step.DependsOn);
            ArgumentNullException.ThrowIfNull(step.Synchronize);
            if (visited.Contains(step.Name)) return;
            if (!visiting.Add(step.Name))
                throw new ArgumentException($"Dependency cycle at '{step.Name}'.", nameof(steps));
            foreach (var name in step.DependsOn)
            {
                if (!byName.TryGetValue(name, out var dependency))
                    throw new ArgumentException($"Unknown dependency '{name}' for '{step.Name}'.", nameof(steps));
                Visit(dependency);
            }
            visiting.Remove(step.Name);
            visited.Add(step.Name);
            ordered.Add(step);
        }
    }
}
