namespace DotCelery.Backend.Redis.Execution;

/// <summary>
/// Options for <see cref="RedisTaskExecutionTracker"/>.
/// </summary>
public sealed class RedisTaskExecutionTrackerOptions
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
    /// Gets or sets the key prefix for execution tracking.
    /// </summary>
    public string ExecutionKeyPrefix { get; set; } = "dotcelery:execution:";

    /// <summary>
    /// Gets or sets the hash key for storing execution metadata.
    /// </summary>
    public string ExecutionIndexKey { get; set; } = "dotcelery:executions";

    /// <summary>
    /// Gets or sets the default timeout for execution tracking.
    /// </summary>
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromHours(1);

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
