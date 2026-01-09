namespace DotCelery.Core.Batches;

/// <summary>
/// Represents a batch of tasks.
/// </summary>
public sealed record Batch
{
    /// <summary>
    /// Gets the unique batch ID.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the batch name (optional).
    /// </summary>
    public string? Name { get; init; }

    /// <summary>
    /// Gets the current batch state.
    /// </summary>
    public required BatchState State { get; init; }

    /// <summary>
    /// Gets the IDs of all tasks in the batch.
    /// </summary>
    public required IReadOnlyList<string> TaskIds { get; init; }

    /// <summary>
    /// Gets the IDs of completed tasks.
    /// </summary>
    public IReadOnlyList<string> CompletedTaskIds { get; init; } = [];

    /// <summary>
    /// Gets the IDs of failed tasks.
    /// </summary>
    public IReadOnlyList<string> FailedTaskIds { get; init; } = [];

    /// <summary>
    /// Gets when the batch was created.
    /// </summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    /// Gets when the batch completed (if completed).
    /// </summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>
    /// Gets the callback task ID to execute on completion (if any).
    /// </summary>
    public string? CallbackTaskId { get; init; }

    /// <summary>
    /// Gets the total number of tasks in the batch.
    /// </summary>
    public int TotalTasks => TaskIds.Count;

    /// <summary>
    /// Gets the number of completed tasks.
    /// </summary>
    public int CompletedCount => CompletedTaskIds.Count;

    /// <summary>
    /// Gets the number of failed tasks.
    /// </summary>
    public int FailedCount => FailedTaskIds.Count;

    /// <summary>
    /// Gets the number of pending tasks.
    /// </summary>
    public int PendingCount => TotalTasks - CompletedCount - FailedCount;

    /// <summary>
    /// Gets the progress as a percentage (0-100).
    /// </summary>
    public double Progress =>
        TotalTasks > 0 ? (CompletedCount + FailedCount) * 100.0 / TotalTasks : 0;

    /// <summary>
    /// Gets whether all tasks have finished (either completed or failed).
    /// </summary>
    public bool IsFinished => PendingCount == 0;
}
