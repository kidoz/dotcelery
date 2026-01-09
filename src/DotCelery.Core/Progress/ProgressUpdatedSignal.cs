using DotCelery.Core.Signals;

namespace DotCelery.Core.Progress;

/// <summary>
/// Signal emitted when task progress is updated.
/// </summary>
public sealed record ProgressUpdatedSignal : ITaskSignal
{
    /// <inheritdoc />
    public required string TaskId { get; init; }

    /// <inheritdoc />
    public required string TaskName { get; init; }

    /// <inheritdoc />
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the progress information.
    /// </summary>
    public required ProgressInfo Progress { get; init; }

    /// <summary>
    /// Gets the worker name reporting the progress.
    /// </summary>
    public string? Worker { get; init; }
}
