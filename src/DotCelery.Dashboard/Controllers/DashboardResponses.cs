using DotCelery.Core.Dashboard;

namespace DotCelery.Dashboard.Controllers;

/// <summary>
/// Response for dashboard overview.
/// </summary>
public sealed record DashboardOverviewResponse
{
    /// <summary>
    /// Gets task counts by state.
    /// </summary>
    public required IReadOnlyDictionary<string, long> StateCounts { get; init; }

    /// <summary>
    /// Gets the total worker count.
    /// </summary>
    public int WorkerCount { get; init; }

    /// <summary>
    /// Gets the active worker count.
    /// </summary>
    public int ActiveWorkers { get; init; }

    /// <summary>
    /// Gets queue statistics.
    /// </summary>
    public required IReadOnlyList<QueueStats> Queues { get; init; }

    /// <summary>
    /// Gets dashboard metrics.
    /// </summary>
    public required DashboardMetrics Metrics { get; init; }

    /// <summary>
    /// Gets the count of delayed tasks.
    /// </summary>
    public long? DelayedTaskCount { get; init; }
}

/// <summary>
/// Response for task list.
/// </summary>
public sealed record TaskListResponse
{
    /// <summary>
    /// Gets the tasks.
    /// </summary>
    public required IReadOnlyList<TaskSummary> Tasks { get; init; }

    /// <summary>
    /// Gets the current page.
    /// </summary>
    public int Page { get; init; }

    /// <summary>
    /// Gets the page size.
    /// </summary>
    public int PageSize { get; init; }

    /// <summary>
    /// Gets whether there are more pages.
    /// </summary>
    public bool HasMore { get; init; }

    /// <summary>Gets whether the scan budget was exhausted before completing the preview.</summary>
    public bool Truncated { get; init; }
}

/// <summary>
/// Request to revoke a task.
/// </summary>
public sealed record RevokeTaskRequest
{
    /// <summary>
    /// Gets whether to terminate a running task.
    /// </summary>
    public bool Terminate { get; init; }

    /// <summary>
    /// Gets whether to use immediate cancellation.
    /// </summary>
    public bool Immediate { get; init; }
}

/// <summary>
/// Response for filter preview.
/// </summary>
public sealed record FilterPreviewResponse
{
    /// <summary>
    /// Gets the matching tasks.
    /// </summary>
    public required IReadOnlyList<TaskSummary> Tasks { get; init; }

    /// <summary>
    /// Gets the current page.
    /// </summary>
    public int Page { get; init; }

    /// <summary>
    /// Gets the page size.
    /// </summary>
    public int PageSize { get; init; }

    /// <summary>
    /// Gets whether there are more pages.
    /// </summary>
    public bool HasMore { get; init; }

    /// <summary>Gets whether the scan budget was exhausted before completing the preview.</summary>
    public bool Truncated { get; init; }
}
