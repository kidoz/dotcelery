namespace DotCelery.Backend.Redis.Signals;

/// <summary>
/// Configuration options for <see cref="RedisSignalStore"/>.
/// </summary>
public sealed class RedisSignalStoreOptions
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
    /// Gets or sets the key prefix for signal queue keys.
    /// </summary>
    public string KeyPrefix { get; set; } = "celery:signals";

    /// <summary>
    /// Gets or sets the visibility timeout for processing signals.
    /// Signals not acknowledged within this time will be returned to the queue.
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the synchronous operation timeout.
    /// </summary>
    public TimeSpan SyncTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets whether to abort on connection failure.
    /// </summary>
    public bool AbortOnConnectFail { get; set; }

    /// <summary>
    /// Gets the pending queue key.
    /// </summary>
    public string PendingQueueKey => $"{KeyPrefix}:pending";

    /// <summary>
    /// Gets the processing hash key.
    /// </summary>
    public string ProcessingKey => $"{KeyPrefix}:processing";
}
