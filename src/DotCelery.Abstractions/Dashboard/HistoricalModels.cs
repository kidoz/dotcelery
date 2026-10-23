namespace DotCelery.Core.Dashboard;

/// <summary>
/// Represents a snapshot of metrics at a point in time.
/// </summary>
public sealed record MetricsSnapshot
{
    /// <summary>
    /// Gets the timestamp when this snapshot was recorded.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the task name (null for aggregate metrics across all tasks).
    /// </summary>
    public string? TaskName { get; init; }

    /// <summary>
    /// Gets the queue name (null for aggregate metrics across all queues).
    /// </summary>
    public string? Queue { get; init; }

    /// <summary>
    /// Gets the number of successful task completions.
    /// </summary>
    public long SuccessCount { get; init; }

    /// <summary>
    /// Gets the number of failed task executions.
    /// </summary>
    public long FailureCount { get; init; }

    /// <summary>
    /// Gets the number of task retries.
    /// </summary>
    public long RetryCount { get; init; }

    /// <summary>
    /// Gets the number of revoked tasks.
    /// </summary>
    public long RevokedCount { get; init; }

    /// <summary>
    /// Gets the average execution time for completed tasks.
    /// </summary>
    public TimeSpan? AverageExecutionTime { get; init; }

    /// <summary>
    /// Gets the total processed tasks in this snapshot.
    /// </summary>
    public long TotalProcessed => SuccessCount + FailureCount;
}

/// <summary>
/// Represents aggregated metrics over a time range.
/// </summary>
public sealed record AggregatedMetrics
{
    /// <summary>
    /// Gets the start of the aggregation period.
    /// </summary>
    public required DateTimeOffset From { get; init; }

    /// <summary>
    /// Gets the end of the aggregation period.
    /// </summary>
    public required DateTimeOffset To { get; init; }

    /// <summary>
    /// Gets the granularity used for aggregation.
    /// </summary>
    public required MetricsGranularity Granularity { get; init; }

    /// <summary>
    /// Gets the total number of processed tasks.
    /// </summary>
    public long TotalProcessed { get; init; }

    /// <summary>
    /// Gets the number of successful completions.
    /// </summary>
    public long SuccessCount { get; init; }

    /// <summary>
    /// Gets the number of failures.
    /// </summary>
    public long FailureCount { get; init; }

    /// <summary>
    /// Gets the number of retries.
    /// </summary>
    public long RetryCount { get; init; }

    /// <summary>
    /// Gets the number of revocations.
    /// </summary>
    public long RevokedCount { get; init; }

    /// <summary>
    /// Gets the average execution time.
    /// </summary>
    public TimeSpan? AverageExecutionTime { get; init; }

    /// <summary>
    /// Gets the tasks per second rate.
    /// </summary>
    public double TasksPerSecond { get; init; }

    /// <summary>
    /// Gets the success rate as a decimal (0-1).
    /// </summary>
    public double SuccessRate => TotalProcessed > 0 ? (double)SuccessCount / TotalProcessed : 0;
}

/// <summary>
/// Represents a single data point in a time series.
/// </summary>
public sealed record MetricsDataPoint
{
    /// <summary>
    /// Gets the timestamp for this data point.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the number of successful completions in this period.
    /// </summary>
    public long SuccessCount { get; init; }

    /// <summary>
    /// Gets the number of failures in this period.
    /// </summary>
    public long FailureCount { get; init; }

    /// <summary>
    /// Gets the number of retries in this period.
    /// </summary>
    public long RetryCount { get; init; }

    /// <summary>
    /// Gets the tasks per second rate in this period.
    /// </summary>
    public double TasksPerSecond { get; init; }

    /// <summary>
    /// Gets the average execution time in this period.
    /// </summary>
    public TimeSpan? AverageExecutionTime { get; init; }

    /// <summary>
    /// Gets the total processed in this period.
    /// </summary>
    public long TotalProcessed => SuccessCount + FailureCount;
}

/// <summary>
/// Represents a summary of metrics for a specific task name.
/// </summary>
public sealed record TaskMetricsSummary
{
    /// <summary>
    /// Gets the task name.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the total number of executions.
    /// </summary>
    public long TotalCount { get; init; }

    /// <summary>
    /// Gets the number of successful executions.
    /// </summary>
    public long SuccessCount { get; init; }

    /// <summary>
    /// Gets the number of failed executions.
    /// </summary>
    public long FailureCount { get; init; }

    /// <summary>
    /// Gets the average execution time.
    /// </summary>
    public TimeSpan? AverageExecutionTime { get; init; }

    /// <summary>
    /// Gets the minimum execution time.
    /// </summary>
    public TimeSpan? MinExecutionTime { get; init; }

    /// <summary>
    /// Gets the maximum execution time.
    /// </summary>
    public TimeSpan? MaxExecutionTime { get; init; }

    /// <summary>
    /// Gets the success rate as a decimal (0-1).
    /// </summary>
    public double SuccessRate => TotalCount > 0 ? (double)SuccessCount / TotalCount : 0;
}

/// <summary>
/// Granularity levels for metrics aggregation.
/// </summary>
public enum MetricsGranularity
{
    /// <summary>
    /// Per-minute aggregation.
    /// </summary>
    Minute,

    /// <summary>
    /// Per-hour aggregation.
    /// </summary>
    Hour,

    /// <summary>
    /// Per-day aggregation.
    /// </summary>
    Day,

    /// <summary>
    /// Per-week aggregation.
    /// </summary>
    Week,
}
