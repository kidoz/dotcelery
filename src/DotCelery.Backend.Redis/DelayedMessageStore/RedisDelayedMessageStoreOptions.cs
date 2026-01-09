namespace DotCelery.Backend.Redis.DelayedMessageStore;

/// <summary>
/// Configuration options for the Redis delayed message store.
/// </summary>
public sealed class RedisDelayedMessageStoreOptions
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
    /// Gets or sets the Redis key for the delayed messages sorted set.
    /// Messages are stored with their delivery time as the score.
    /// </summary>
    public string DelayedMessagesKey { get; set; } = "dotcelery:delay:messages";

    /// <summary>
    /// Gets or sets the Redis key for the taskId to message mapping.
    /// Used for efficient removal by taskId.
    /// </summary>
    public string TaskIdMappingKey { get; set; } = "dotcelery:delay:taskmap";

    /// <summary>
    /// Gets or sets the batch size for retrieving due messages.
    /// </summary>
    public int BatchSize { get; set; } = 100;

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
}
