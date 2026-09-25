namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// Options for the stores built on the storage primitives.
/// </summary>
public sealed class StorageStoreOptions
{
    /// <summary>
    /// Gets or sets the prefix of the collection, queue, lease, counter, and channel names the
    /// stores use. Applications that share storage but not tasks need different prefixes.
    /// Default is <c>dotcelery</c>.
    /// </summary>
    public string Prefix { get; set; } = "dotcelery";

    /// <summary>
    /// Gets or sets how long a claimed delayed message, outbox message, or signal is hidden
    /// from other consumers. If the consumer does not finish in time, for example because it
    /// crashed, the item is delivered again. Default is 5 minutes.
    /// </summary>
    public TimeSpan ClaimTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets how many times an outbox message is attempted before it is moved to the
    /// failed messages. Default is 5.
    /// </summary>
    public int OutboxMaxAttempts { get; set; } = 5;

    /// <summary>
    /// Gets or sets how long to wait before retrying an outbox message that failed to publish.
    /// Default is 5 seconds.
    /// </summary>
    public TimeSpan OutboxRetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets how long outbox messages that exhausted their attempts are kept.
    /// Default is 7 days.
    /// </summary>
    public TimeSpan OutboxFailedRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Gets or sets how long the inbox remembers a processed message. Default is 7 days.
    /// </summary>
    public TimeSpan InboxRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Gets or sets how long a revocation is kept when <see cref="Models.RevokeOptions.Expiry"/>
    /// is not set. Default is 7 days.
    /// </summary>
    public TimeSpan RevocationRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Gets or sets how often revocation subscribers poll when the storage provider cannot
    /// push notifications. Default is 1 second.
    /// </summary>
    public TimeSpan RevocationPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets how long a task execution is tracked when no timeout is given.
    /// Default is 1 hour.
    /// </summary>
    public TimeSpan ExecutionTimeout { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Gets or sets how often expired entries are deleted from storage. Expired entries are
    /// never returned, but they take space until they are deleted. Default is 5 minutes.
    /// </summary>
    public TimeSpan PurgeInterval { get; set; } = TimeSpan.FromMinutes(5);

    internal string Name(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(Prefix);
        return $"{Prefix}.{name}";
    }
}
