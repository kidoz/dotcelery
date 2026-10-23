using System.ComponentModel.DataAnnotations;
using DotCelery.Core.Models;

namespace DotCelery.Dashboard.Models;

/// <summary>Filters the retained task history. All supplied criteria are combined.</summary>
public sealed record TaskSearchRequest
{
    /// <summary>Gets the task state; null includes every state.</summary>
    public TaskState? State { get; init; }

    /// <summary>Gets the exact task name.</summary>
    [StringLength(500)]
    public string? TaskName { get; init; }

    /// <summary>Gets a case-insensitive substring of the task ID or name.</summary>
    [StringLength(500)]
    public string? Search { get; init; }

    /// <summary>Gets the exact queue name.</summary>
    [StringLength(500)]
    public string? Queue { get; init; }

    /// <summary>Gets the exact worker name.</summary>
    [StringLength(500)]
    public string? Worker { get; init; }

    /// <summary>Gets the earliest task update time, inclusive.</summary>
    public DateTimeOffset? From { get; init; }

    /// <summary>Gets the latest task update time, exclusive.</summary>
    public DateTimeOffset? Until { get; init; }

    /// <summary>Gets whether to list the oldest updates first.</summary>
    public bool OldestFirst { get; init; }

    /// <summary>Gets the one-based page number.</summary>
    public int Page { get; init; } = 1;

    /// <summary>Gets the requested page size, capped by the dashboard configuration.</summary>
    public int PageSize { get; init; } = 25;

    internal bool Matches(DotCelery.Core.Dashboard.TaskSummary task)
    {
        var updated = task.CompletedAt ?? task.StartedAt ?? task.SentAt;
        return (!State.HasValue || task.State == State.Value.ToString())
            && (string.IsNullOrEmpty(TaskName) || task.TaskName == TaskName)
            && (string.IsNullOrEmpty(Queue) || task.Queue == Queue)
            && (string.IsNullOrEmpty(Worker) || task.Worker == Worker)
            && (
                string.IsNullOrWhiteSpace(Search)
                || task.TaskId.Contains(Search.Trim(), StringComparison.OrdinalIgnoreCase)
                || task.TaskName.Contains(Search.Trim(), StringComparison.OrdinalIgnoreCase)
            )
            && (!From.HasValue || updated >= From)
            && (!Until.HasValue || updated < Until);
    }
}
