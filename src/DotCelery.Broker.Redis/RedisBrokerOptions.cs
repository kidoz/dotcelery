namespace DotCelery.Broker.Redis;

/// <summary>
/// Configuration options for the Redis Streams message broker.
/// </summary>
public sealed class RedisBrokerOptions
{
    /// <summary>
    /// Gets or sets the Redis connection string.
    /// Example: "localhost:6379" or "redis://localhost:6379/0"
    /// </summary>
    public string ConnectionString { get; set; } = "localhost:6379";

    /// <summary>
    /// Gets or sets the database index to use.
    /// </summary>
    public int Database { get; set; }

    /// <summary>
    /// Gets or sets the connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the sync timeout for operations.
    /// </summary>
    public TimeSpan SyncTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets whether to abort on connect fail.
    /// </summary>
    public bool AbortOnConnectFail { get; set; }

    /// <summary>
    /// Gets or sets the key prefix for Redis streams.
    /// Each queue maps to a stream: {StreamKeyPrefix}{queueName}
    /// </summary>
    public string StreamKeyPrefix { get; set; } = "dotcelery:stream:";

    /// <summary>
    /// Gets or sets the consumer group name.
    /// All workers share this consumer group for distributed processing.
    /// </summary>
    public string ConsumerGroupName { get; set; } = "dotcelery-workers";

    /// <summary>
    /// Gets or sets the consumer name for this instance.
    /// If null, a unique name is auto-generated using machine name and process ID.
    /// </summary>
    public string? ConsumerName { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of messages to fetch per read.
    /// </summary>
    public int PrefetchCount { get; set; } = 10;

    /// <summary>
    /// Gets or sets the timeout for blocking reads.
    /// Lower values provide more responsive shutdown but more Redis calls.
    /// </summary>
    public TimeSpan BlockTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the idle time threshold for claiming pending messages.
    /// Messages idle longer than this are reclaimed by other consumers.
    /// </summary>
    public TimeSpan ClaimTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets whether to automatically create streams and consumer groups.
    /// </summary>
    public bool AutoCreateStreams { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum stream length for trimming.
    /// If set, streams are trimmed to approximately this length using MAXLEN ~.
    /// </summary>
    public long? MaxStreamLength { get; set; }

    /// <summary>
    /// Gets or sets the interval for checking and reclaiming pending messages.
    /// </summary>
    public TimeSpan PendingCheckInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum message size in bytes.
    /// Messages larger than this will be rejected.
    /// Default is 10 MB. Set to 0 to disable size checking.
    /// </summary>
    public int MaxMessageSizeBytes { get; set; } = 10 * 1024 * 1024;
}
