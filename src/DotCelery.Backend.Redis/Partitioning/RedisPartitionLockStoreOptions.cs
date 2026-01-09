namespace DotCelery.Backend.Redis.Partitioning;

/// <summary>
/// Options for <see cref="RedisPartitionLockStore"/>.
/// </summary>
public sealed class RedisPartitionLockStoreOptions
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
    /// Gets or sets the key prefix for partition locks.
    /// </summary>
    public string LockKeyPrefix { get; set; } = "dotcelery:partition-lock:";

    /// <summary>
    /// Gets or sets the connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the sync timeout.
    /// </summary>
    public TimeSpan SyncTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets whether to abort on connection failure.
    /// </summary>
    public bool AbortOnConnectFail { get; set; }
}
