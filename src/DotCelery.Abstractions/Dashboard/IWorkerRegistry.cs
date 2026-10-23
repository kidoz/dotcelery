namespace DotCelery.Core.Dashboard;

/// <summary>
/// Registry for tracking active workers across the distributed system.
/// </summary>
public interface IWorkerRegistry : IAsyncDisposable
{
    /// <summary>
    /// Registers a worker as active.
    /// </summary>
    /// <param name="worker">The worker information.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RegisterWorkerAsync(WorkerInfo worker, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a heartbeat for a worker, updating its last seen timestamp.
    /// </summary>
    /// <param name="workerId">The worker ID.</param>
    /// <param name="activeTasks">Number of currently active tasks.</param>
    /// <param name="processedCount">Total processed task count.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask HeartbeatAsync(
        string workerId,
        int activeTasks = 0,
        long processedCount = 0,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Unregisters a worker when it shuts down.
    /// </summary>
    /// <param name="workerId">The worker ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask UnregisterWorkerAsync(string workerId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all active workers.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async enumerable of worker information.</returns>
    IAsyncEnumerable<WorkerInfo> GetActiveWorkersAsync(
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets a specific worker by ID.
    /// </summary>
    /// <param name="workerId">The worker ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The worker info, or null if not found.</returns>
    ValueTask<WorkerInfo?> GetWorkerAsync(
        string workerId,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Cleans up workers that have missed heartbeats.
    /// </summary>
    /// <param name="timeout">How long since last heartbeat before marking offline.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Number of workers marked as offline/removed.</returns>
    ValueTask<int> CleanupStaleWorkersAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    );
}
