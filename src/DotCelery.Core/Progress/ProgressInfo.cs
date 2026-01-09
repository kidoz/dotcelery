namespace DotCelery.Core.Progress;

/// <summary>
/// Represents task progress information reported during execution.
/// </summary>
public sealed record ProgressInfo
{
    /// <summary>
    /// Gets the progress percentage (0-100).
    /// </summary>
    public required double Percentage { get; init; }

    /// <summary>
    /// Gets the optional status message.
    /// </summary>
    public string? Message { get; init; }

    /// <summary>
    /// Gets custom progress data.
    /// </summary>
    public IReadOnlyDictionary<string, object>? Data { get; init; }

    /// <summary>
    /// Gets when this progress was reported.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Gets the current step description (e.g., "3 of 10").
    /// </summary>
    public string? CurrentStep { get; init; }

    /// <summary>
    /// Gets the estimated time remaining.
    /// </summary>
    public TimeSpan? EstimatedRemaining { get; init; }

    /// <summary>
    /// Gets the number of items processed (if applicable).
    /// </summary>
    public long? ItemsProcessed { get; init; }

    /// <summary>
    /// Gets the total number of items (if applicable).
    /// </summary>
    public long? TotalItems { get; init; }
}
