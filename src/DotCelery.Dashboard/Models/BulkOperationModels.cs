using DotCelery.Core.Models;

namespace DotCelery.Dashboard.Models;

/// <summary>
/// Filter criteria for selecting tasks.
/// </summary>
public sealed record TaskFilter
{
    /// <summary>
    /// Gets or sets the task state to filter by.
    /// </summary>
    public TaskState? State { get; init; }

    /// <summary>
    /// Gets or sets the task name pattern (supports glob patterns).
    /// </summary>
    public string? TaskNamePattern { get; init; }

    /// <summary>
    /// Gets or sets the queue name to filter by.
    /// </summary>
    public string? Queue { get; init; }

    /// <summary>
    /// Gets or sets the minimum completion date filter.
    /// </summary>
    public DateTimeOffset? CompletedAfter { get; init; }

    /// <summary>
    /// Gets or sets the maximum completion date filter.
    /// </summary>
    public DateTimeOffset? CompletedBefore { get; init; }
}

/// <summary>
/// Request for bulk task revocation.
/// </summary>
public sealed record BulkRevokeRequest
{
    /// <summary>
    /// Gets or sets specific task IDs to revoke.
    /// </summary>
    public IReadOnlyList<string>? TaskIds { get; init; }

    /// <summary>
    /// Gets or sets a filter to select tasks for revocation.
    /// </summary>
    public TaskFilter? Filter { get; init; }

    /// <summary>
    /// Gets or sets whether to terminate running tasks.
    /// </summary>
    public bool Terminate { get; init; }

    /// <summary>
    /// Gets or sets whether to use immediate cancellation.
    /// </summary>
    public bool Immediate { get; init; }
}

/// <summary>
/// Request for bulk task retry.
/// </summary>
public sealed record BulkRetryRequest
{
    /// <summary>
    /// Gets or sets specific task IDs to retry.
    /// </summary>
    public IReadOnlyList<string>? TaskIds { get; init; }

    /// <summary>
    /// Gets or sets a filter to select tasks for retry.
    /// </summary>
    public TaskFilter? Filter { get; init; }

    /// <summary>
    /// Gets or sets the target queue for retried tasks (optional).
    /// </summary>
    public string? TargetQueue { get; init; }
}

/// <summary>
/// Response from a bulk operation.
/// </summary>
public sealed record BulkOperationResponse
{
    /// <summary>
    /// Gets the total number of tasks processed.
    /// </summary>
    public required int ProcessedCount { get; init; }

    /// <summary>
    /// Gets the number of successful operations.
    /// </summary>
    public required int SuccessCount { get; init; }

    /// <summary>
    /// Gets the number of failed operations.
    /// </summary>
    public required int FailureCount { get; init; }

    /// <summary>
    /// Gets the IDs of tasks that failed.
    /// </summary>
    public IReadOnlyList<string>? FailedTaskIds { get; init; }

    /// <summary>
    /// Gets an optional message.
    /// </summary>
    public string? Message { get; init; }
}
