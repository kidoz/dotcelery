namespace DotCelery.Backend.Redis.Metrics;

/// <summary>
/// Options for <see cref="RedisQueueMetrics"/>.
/// </summary>
public sealed class RedisQueueMetricsOptions
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
    /// Gets or sets the key prefix for queue metrics.
    /// </summary>
    public string MetricsKeyPrefix { get; set; } = "dotcelery:metrics:queue:";

    /// <summary>
    /// Gets or sets the hash key for running tasks.
    /// </summary>
    public string RunningTasksKey { get; set; } = "dotcelery:metrics:running";

    /// <summary>
    /// Gets or sets the set key for known queues.
    /// </summary>
    public string QueuesKey { get; set; } = "dotcelery:metrics:queues";

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
