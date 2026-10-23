namespace DotCelery.Core.Execution;

/// <summary>
/// Tracks currently executing tasks to prevent overlapping execution.
/// </summary>
public interface ITaskExecutionTracker : IAsyncDisposable
{
    /// <summary>
    /// Attempts to start tracking a task execution.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="taskId">The task ID.</param>
    /// <param name="key">Optional key to distinguish different instances (e.g., input hash).</param>
    /// <param name="timeout">How long before the execution tracking auto-expires.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if started successfully, false if another instance is already running.</returns>
    ValueTask<bool> TryStartAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Stops tracking a task execution.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="taskId">The task ID.</param>
    /// <param name="key">Optional key used when starting.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask StopAsync(
        string taskName,
        string taskId,
        string? key = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Checks if a task is currently executing.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="key">Optional key to check specific instance.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if executing, false otherwise.</returns>
    ValueTask<bool> IsExecutingAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the task ID of the currently executing instance.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="key">Optional key to check specific instance.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task ID or null if not executing.</returns>
    ValueTask<string?> GetExecutingTaskIdAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Extends the timeout for an executing task.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <param name="taskId">The task ID.</param>
    /// <param name="key">Optional key.</param>
    /// <param name="extension">Additional time to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if extended, false if not found.</returns>
    ValueTask<bool> ExtendAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? extension = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets all currently executing tasks.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Dictionary of task names to their executing task IDs.</returns>
    ValueTask<IReadOnlyDictionary<string, ExecutingTaskInfo>> GetAllExecutingAsync(
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// Information about an executing task.
/// </summary>
public sealed record ExecutingTaskInfo
{
    /// <summary>
    /// Gets the task ID.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the optional key.
    /// </summary>
    public string? Key { get; init; }

    /// <summary>
    /// Gets when execution started.
    /// </summary>
    public required DateTimeOffset StartedAt { get; init; }

    /// <summary>
    /// Gets when the tracking expires.
    /// </summary>
    public required DateTimeOffset ExpiresAt { get; init; }
}
