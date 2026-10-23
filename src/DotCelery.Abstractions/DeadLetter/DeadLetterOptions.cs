namespace DotCelery.Core.DeadLetter;

/// <summary>
/// Configuration options for the dead letter queue.
/// </summary>
public sealed class DeadLetterOptions
{
    /// <summary>
    /// Gets or sets whether dead letter queue is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets how long messages are retained in the DLQ.
    /// Default is 7 days.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Gets or sets the maximum number of messages to keep in the DLQ.
    /// Oldest messages are removed when exceeded. Default is 10,000.
    /// </summary>
    public int MaxMessages { get; set; } = 10_000;

    /// <summary>
    /// Gets or sets whether to include stack traces in DLQ messages.
    /// </summary>
    public bool IncludeStackTrace { get; set; } = true;

    /// <summary>
    /// Gets or sets the reasons that should trigger dead-lettering.
    /// By default, all failures are dead-lettered.
    /// </summary>
    public DeadLetterReason[] Reasons { get; set; } =
    [
        DeadLetterReason.MaxRetriesExceeded,
        DeadLetterReason.Rejected,
        DeadLetterReason.TimeLimitExceeded,
        DeadLetterReason.Expired,
        DeadLetterReason.UnknownTask,
        DeadLetterReason.Failed,
    ];
}
