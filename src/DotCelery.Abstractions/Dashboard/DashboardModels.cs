namespace DotCelery.Core.Dashboard;

/// <summary>
/// Statistics for a message queue.
/// </summary>
public sealed record QueueStats
{
    /// <summary>
    /// Gets the queue name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Gets the number of messages in the queue.
    /// </summary>
    public required long MessageCount { get; init; }

    /// <summary>
    /// Gets the number of consumers for the queue.
    /// </summary>
    public int ConsumerCount { get; init; }

    /// <summary>
    /// Gets the number of messages being processed.
    /// </summary>
    public long InProgressCount { get; init; }

    /// <summary>
    /// Gets the timestamp of the last message.
    /// </summary>
    public DateTimeOffset? LastMessageAt { get; init; }
}

/// <summary>
/// Information about a worker instance.
/// </summary>
public sealed record WorkerInfo
{
    /// <summary>
    /// Gets the unique worker identifier.
    /// </summary>
    public required string WorkerId { get; init; }

    /// <summary>
    /// Gets the worker hostname.
    /// </summary>
    public required string Hostname { get; init; }

    /// <summary>
    /// Gets the process ID.
    /// </summary>
    public int ProcessId { get; init; }

    /// <summary>
    /// Gets the queues the worker is consuming from.
    /// </summary>
    public required IReadOnlyList<string> Queues { get; init; }

    /// <summary>
    /// Gets the concurrency level (number of worker threads).
    /// </summary>
    public int Concurrency { get; init; }

    /// <summary>
    /// Gets the number of tasks currently being processed.
    /// </summary>
    public int ActiveTasks { get; init; }

    /// <summary>
    /// Gets the total tasks processed by this worker.
    /// </summary>
    public long ProcessedCount { get; init; }

    /// <summary>
    /// Gets when the worker started.
    /// </summary>
    public required DateTimeOffset StartedAt { get; init; }

    /// <summary>
    /// Gets the last heartbeat timestamp.
    /// </summary>
    public required DateTimeOffset LastHeartbeat { get; init; }

    /// <summary>
    /// Gets the worker status.
    /// </summary>
    public WorkerStatus Status { get; init; } = WorkerStatus.Online;

    /// <summary>
    /// Gets the software version.
    /// </summary>
    public string? Version { get; init; }
}

/// <summary>
/// Worker status.
/// </summary>
public enum WorkerStatus
{
    /// <summary>
    /// Worker is online and processing tasks.
    /// </summary>
    Online,

    /// <summary>
    /// Worker is online but not accepting new tasks.
    /// </summary>
    Paused,

    /// <summary>
    /// Worker has missed heartbeats and may be offline.
    /// </summary>
    Unresponsive,

    /// <summary>
    /// Worker is offline.
    /// </summary>
    Offline,
}

/// <summary>
/// Summary of a task for dashboard display.
/// </summary>
public sealed record TaskSummary
{
    /// <summary>
    /// Gets the task ID.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the task name.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the current task state.
    /// </summary>
    public required string State { get; init; }

    /// <summary>
    /// Gets the queue the task is/was in.
    /// </summary>
    public string? Queue { get; init; }

    /// <summary>
    /// Gets the worker that executed the task.
    /// </summary>
    public string? Worker { get; init; }

    /// <summary>
    /// Gets when the task was sent.
    /// </summary>
    public DateTimeOffset? SentAt { get; init; }

    /// <summary>
    /// Gets when the task started executing.
    /// </summary>
    public DateTimeOffset? StartedAt { get; init; }

    /// <summary>
    /// Gets when the task completed.
    /// </summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>
    /// Gets the execution duration.
    /// </summary>
    public TimeSpan? Duration { get; init; }

    /// <summary>
    /// Gets the retry count.
    /// </summary>
    public int Retries { get; init; }

    /// <summary>
    /// Gets the exception message if failed.
    /// </summary>
    public string? ExceptionMessage { get; init; }

    /// <summary>
    /// Gets the ETA if scheduled.
    /// </summary>
    public DateTimeOffset? Eta { get; init; }
}

/// <summary>
/// Dashboard metrics for a time window.
/// </summary>
public sealed record DashboardMetrics
{
    /// <summary>
    /// Gets the time window for these metrics.
    /// </summary>
    public required TimeSpan Window { get; init; }

    /// <summary>
    /// Gets the total tasks processed in the window.
    /// </summary>
    public long TotalProcessed { get; init; }

    /// <summary>
    /// Gets the successful tasks count.
    /// </summary>
    public long SuccessCount { get; init; }

    /// <summary>
    /// Gets the failed tasks count.
    /// </summary>
    public long FailureCount { get; init; }

    /// <summary>
    /// Gets the retried tasks count.
    /// </summary>
    public long RetryCount { get; init; }

    /// <summary>
    /// Gets the revoked tasks count.
    /// </summary>
    public long RevokedCount { get; init; }

    /// <summary>
    /// Gets the average execution time.
    /// </summary>
    public TimeSpan? AverageExecutionTime { get; init; }

    /// <summary>
    /// Gets the tasks per second throughput.
    /// </summary>
    public double TasksPerSecond { get; init; }

    /// <summary>
    /// Gets the success rate (0-1).
    /// </summary>
    public double SuccessRate => TotalProcessed > 0 ? (double)SuccessCount / TotalProcessed : 0;

    /// <summary>
    /// Gets metrics broken down by task name.
    /// </summary>
    public IReadOnlyDictionary<string, TaskMetrics>? ByTaskName { get; init; }
}

/// <summary>
/// Metrics for a specific task type.
/// </summary>
public sealed record TaskMetrics
{
    /// <summary>
    /// Gets the task name.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the total executions.
    /// </summary>
    public long TotalCount { get; init; }

    /// <summary>
    /// Gets the success count.
    /// </summary>
    public long SuccessCount { get; init; }

    /// <summary>
    /// Gets the failure count.
    /// </summary>
    public long FailureCount { get; init; }

    /// <summary>
    /// Gets the average execution time.
    /// </summary>
    public TimeSpan? AverageExecutionTime { get; init; }
}
