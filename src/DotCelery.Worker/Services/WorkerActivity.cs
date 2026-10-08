namespace DotCelery.Worker.Services;

/// <summary>
/// Counts the work a worker is doing, so the heartbeat it reports to an <c>IWorkerRegistry</c>
/// reflects the live numbers.
/// </summary>
public sealed class WorkerActivity
{
    private int _activeTasks;
    private long _processedTasks;

    /// <summary>
    /// Gets the number of tasks the worker is running right now.
    /// </summary>
    public int ActiveTasks => Volatile.Read(ref _activeTasks);

    /// <summary>
    /// Gets the number of tasks whose outcome the worker has recorded since it started.
    /// </summary>
    public long ProcessedTasks => Interlocked.Read(ref _processedTasks);

    /// <summary>
    /// Records that a task started running.
    /// </summary>
    public void IncrementActiveTasks() => Interlocked.Increment(ref _activeTasks);

    /// <summary>
    /// Records that a task stopped running.
    /// </summary>
    public void DecrementActiveTasks() => Interlocked.Decrement(ref _activeTasks);

    /// <summary>
    /// Records that a task's outcome was stored.
    /// </summary>
    public void IncrementProcessedTasks() => Interlocked.Increment(ref _processedTasks);
}
