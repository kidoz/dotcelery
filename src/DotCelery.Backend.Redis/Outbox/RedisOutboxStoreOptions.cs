namespace DotCelery.Backend.Redis.Outbox;

/// <summary>
/// Configuration options for <see cref="RedisOutboxStore"/>.
/// </summary>
public sealed class RedisOutboxStoreOptions
{
    /// <summary>
    /// Gets or sets the Redis connection string.
    /// </summary>
    public string ConnectionString { get; set; } = "localhost:6379";

    /// <summary>
    /// Gets or sets the Redis database number.
    /// </summary>
    public int Database { get; set; }

    /// <summary>
    /// Gets or sets the key prefix for outbox messages.
    /// </summary>
    public string MessageKeyPrefix { get; set; } = "dotcelery:outbox:msg:";

    /// <summary>
    /// Gets or sets the key for the pending messages sorted set.
    /// </summary>
    public string PendingSetKey { get; set; } = "dotcelery:outbox:pending";

    /// <summary>
    /// Gets or sets the connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the sync operation timeout.
    /// </summary>
    public TimeSpan SyncTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets whether to abort on connect fail.
    /// </summary>
    public bool AbortOnConnectFail { get; set; }

    /// <summary>
    /// Gets or sets the TTL for dispatched messages before cleanup.
    /// </summary>
    public TimeSpan DispatchedMessageTtl { get; set; } = TimeSpan.FromDays(7);
}
