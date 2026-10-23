using System.Collections.Concurrent;
using DotCelery.Core.MultiTenancy;

namespace DotCelery.Worker.Services;

/// <summary>
/// Counts the work a worker is doing, so the heartbeat it reports to an <c>IWorkerRegistry</c>
/// reflects the live numbers.
/// </summary>
public sealed class WorkerActivity
{
    private readonly ConcurrentDictionary<string, Counts> _tenants = new(StringComparer.Ordinal);
    private Counts? Current =>
        TenantContext.Current.TenantId is { } tenant
            ? _tenants.GetOrAdd(tenant, _ => new Counts())
            : null;

    private sealed class Counts
    {
        public int Active;
        public long Processed;
    }

    private int _activeTasks;
    private long _processedTasks;

    /// <summary>
    /// Gets the number of tasks the worker is running right now.
    /// </summary>
    public int ActiveTasks =>
        Current is { } counts ? Volatile.Read(ref counts.Active) : Volatile.Read(ref _activeTasks);

    /// <summary>
    /// Gets the number of tasks whose outcome the worker has recorded since it started.
    /// </summary>
    public long ProcessedTasks =>
        Current is { } counts
            ? Interlocked.Read(ref counts.Processed)
            : Interlocked.Read(ref _processedTasks);

    /// <summary>
    /// Records that a task started running.
    /// </summary>
    public void IncrementActiveTasks()
    {
        Interlocked.Increment(ref _activeTasks);
        if (Current is { } counts)
            Interlocked.Increment(ref counts.Active);
    }

    /// <summary>
    /// Records that a task stopped running.
    /// </summary>
    public void DecrementActiveTasks()
    {
        Interlocked.Decrement(ref _activeTasks);
        if (Current is { } counts)
            Interlocked.Decrement(ref counts.Active);
    }

    /// <summary>
    /// Records that a task's outcome was stored.
    /// </summary>
    public void IncrementProcessedTasks()
    {
        Interlocked.Increment(ref _processedTasks);
        if (Current is { } counts)
            Interlocked.Increment(ref counts.Processed);
    }
}
