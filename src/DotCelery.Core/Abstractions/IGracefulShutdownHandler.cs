namespace DotCelery.Core.Abstractions;

/// <summary>
/// Handler for graceful shutdown of worker services.
/// Ensures pending tasks complete before the worker stops.
/// </summary>
public interface IGracefulShutdownHandler
{
    /// <summary>
    /// Gets the number of currently executing tasks.
    /// </summary>
    int ActiveTaskCount { get; }

    /// <summary>
    /// Gets whether a shutdown is in progress.
    /// </summary>
    bool IsShuttingDown { get; }

    /// <summary>
    /// Registers a task as in-flight.
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="message">The broker message.</param>
    /// <returns>A registration handle that must be disposed when the task completes.</returns>
    IDisposable RegisterTask(string taskId, BrokerMessage message);

    /// <summary>
    /// Initiates graceful shutdown and waits for completion.
    /// </summary>
    /// <param name="timeout">Maximum time to wait for tasks to complete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Shutdown result with details about completed/cancelled tasks.</returns>
    Task<GracefulShutdownResult> ShutdownAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets all in-flight broker messages for nacking on forced shutdown.
    /// </summary>
    /// <returns>Collection of broker messages that need to be nacked.</returns>
    IReadOnlyCollection<BrokerMessage> GetPendingMessages();
}

/// <summary>
/// Result of a graceful shutdown operation.
/// </summary>
public sealed record GracefulShutdownResult
{
    /// <summary>
    /// Gets the total tasks that were in-flight when shutdown started.
    /// </summary>
    public required int TotalTasks { get; init; }

    /// <summary>
    /// Gets the tasks that completed successfully during shutdown.
    /// </summary>
    public required int CompletedTasks { get; init; }

    /// <summary>
    /// Gets the tasks that were cancelled due to timeout.
    /// </summary>
    public required int CancelledTasks { get; init; }

    /// <summary>
    /// Gets the messages that were nacked for redelivery.
    /// </summary>
    public required int NackedMessages { get; init; }

    /// <summary>
    /// Gets the time taken for shutdown.
    /// </summary>
    public required TimeSpan Duration { get; init; }

    /// <summary>
    /// Gets whether shutdown completed within timeout.
    /// </summary>
    public bool CompletedGracefully => CancelledTasks == 0;
}
