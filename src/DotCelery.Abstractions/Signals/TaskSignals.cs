using DotCelery.Core.Models;

namespace DotCelery.Core.Signals;

// ============================================================================
// Client-side signals (sent by CeleryClient)
// ============================================================================

/// <summary>
/// Signal emitted before a task is published to the broker.
/// </summary>
public sealed record BeforeTaskPublishSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the task input (may be null).
    /// </summary>
    public object? Input { get; init; }

    /// <summary>
    /// Gets the target queue.
    /// </summary>
    public required string Queue { get; init; }

    /// <summary>
    /// Gets the scheduled execution time (if any).
    /// </summary>
    public DateTimeOffset? Eta { get; init; }
}

/// <summary>
/// Signal emitted after a task is published to the broker.
/// </summary>
public sealed record AfterTaskPublishSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the target queue.
    /// </summary>
    public required string Queue { get; init; }
}

// ============================================================================
// Worker-side signals (sent by TaskExecutor)
// ============================================================================

/// <summary>
/// Signal emitted before task execution begins.
/// </summary>
public sealed record TaskPreRunSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the task input (may be null).
    /// </summary>
    public object? Input { get; init; }

    /// <summary>
    /// Gets the current retry count.
    /// </summary>
    public int RetryCount { get; init; }

    /// <summary>
    /// Gets the worker name.
    /// </summary>
    public string? Worker { get; init; }
}

/// <summary>
/// Signal emitted after task execution completes (regardless of success or failure).
/// </summary>
public sealed record TaskPostRunSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the final task state.
    /// </summary>
    public required TaskState State { get; init; }

    /// <summary>
    /// Gets the execution duration.
    /// </summary>
    public required TimeSpan Duration { get; init; }

    /// <summary>
    /// Gets the task result (if successful).
    /// </summary>
    public object? Result { get; init; }

    /// <summary>
    /// Gets the worker name.
    /// </summary>
    public string? Worker { get; init; }
}

/// <summary>
/// Signal emitted when a task completes successfully.
/// </summary>
public sealed record TaskSuccessSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the task result.
    /// </summary>
    public object? Result { get; init; }

    /// <summary>
    /// Gets the execution duration.
    /// </summary>
    public required TimeSpan Duration { get; init; }

    /// <summary>
    /// Gets the worker name.
    /// </summary>
    public string? Worker { get; init; }
}

/// <summary>
/// Signal emitted when a task fails with an exception.
/// </summary>
public sealed record TaskFailureSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the exception that caused the failure.
    /// </summary>
    public required Exception Exception { get; init; }

    /// <summary>
    /// Gets the retry count when failure occurred.
    /// </summary>
    public int RetryCount { get; init; }

    /// <summary>
    /// Gets the execution duration.
    /// </summary>
    public required TimeSpan Duration { get; init; }

    /// <summary>
    /// Gets the worker name.
    /// </summary>
    public string? Worker { get; init; }
}

/// <summary>
/// Signal emitted when a task is scheduled for retry.
/// </summary>
public sealed record TaskRetrySignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the exception that triggered the retry (if any).
    /// </summary>
    public Exception? Exception { get; init; }

    /// <summary>
    /// Gets the retry count.
    /// </summary>
    public int RetryCount { get; init; }

    /// <summary>
    /// Gets the countdown before retry (if any).
    /// </summary>
    public TimeSpan? Countdown { get; init; }

    /// <summary>
    /// Gets the worker name.
    /// </summary>
    public string? Worker { get; init; }
}

/// <summary>
/// Signal emitted when a task is revoked/cancelled.
/// </summary>
public sealed record TaskRevokedSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets whether the task was terminated while running.
    /// </summary>
    public bool Terminated { get; init; }

    /// <summary>
    /// Gets the worker name.
    /// </summary>
    public string? Worker { get; init; }
}

/// <summary>
/// Signal emitted when a task is permanently rejected.
/// </summary>
public sealed record TaskRejectedSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the rejection reason.
    /// </summary>
    public required string Reason { get; init; }

    /// <summary>
    /// Gets the worker name.
    /// </summary>
    public string? Worker { get; init; }
}

/// <summary>
/// Signal emitted when a task is requeued for later processing.
/// This occurs when a filter (e.g., partition lock) requests the task be requeued.
/// </summary>
public sealed record TaskRequeuedSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the reason for requeueing.
    /// </summary>
    public required string Reason { get; init; }

    /// <summary>
    /// Gets the delay before requeueing (if any).
    /// </summary>
    public TimeSpan? RequeueDelay { get; init; }

    /// <summary>
    /// Gets the worker name.
    /// </summary>
    public string? Worker { get; init; }
}
