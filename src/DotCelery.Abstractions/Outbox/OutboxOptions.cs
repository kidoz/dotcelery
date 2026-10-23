namespace DotCelery.Core.Outbox;

/// <summary>
/// Configuration options for the transactional outbox.
/// </summary>
public sealed class OutboxOptions
{
    /// <summary>
    /// Gets or sets whether the outbox is enabled. Default: false.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets the interval for dispatching pending messages.
    /// Default: 1 second.
    /// </summary>
    public TimeSpan DispatchInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the batch size for dispatching messages.
    /// Default: 100.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Gets or sets the maximum retry attempts for failed messages.
    /// Default: 5.
    /// </summary>
    public int MaxRetries { get; set; } = 5;

    /// <summary>
    /// Gets or sets the retention period for dispatched messages.
    /// Messages are cleaned up after this period. Default: 7 days.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Gets or sets the cleanup interval for old messages.
    /// Default: 1 hour.
    /// </summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);
}

/// <summary>
/// Configuration options for the inbox (deduplication).
/// </summary>
public sealed class InboxOptions
{
    /// <summary>
    /// Gets or sets whether inbox deduplication is enabled. Default: false.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Gets or sets how long to keep processed message IDs.
    /// Should be longer than the maximum expected message redelivery time.
    /// Default: 7 days.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Gets or sets the cleanup interval. Default: 1 hour.
    /// </summary>
    public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromHours(1);
}
