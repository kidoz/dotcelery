namespace DotCelery.Backend.Redis;

/// <summary>
/// Configuration options for the Redis result backend.
/// </summary>
public sealed class RedisBackendOptions
{
    /// <summary>
    /// Gets or sets the Redis connection string.
    /// Example: "localhost:6379" or "redis://localhost:6379/0"
    /// </summary>
    public string ConnectionString { get; set; } = "localhost:6379";

    /// <summary>
    /// Gets or sets the key prefix for task results.
    /// </summary>
    public string KeyPrefix { get; set; } = "celery-task-meta-";

    /// <summary>
    /// Gets or sets the key prefix for task state.
    /// </summary>
    public string StateKeyPrefix { get; set; } = "celery-task-state-";

    /// <summary>
    /// Gets or sets the default result expiry time.
    /// </summary>
    public TimeSpan DefaultExpiry { get; set; } = TimeSpan.FromDays(1);

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
    /// Gets or sets the polling interval when waiting for results.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets whether to use pub/sub for result notifications.
    /// When enabled, waiting for results uses pub/sub instead of polling.
    /// </summary>
    public bool UsePubSub { get; set; } = true;

    /// <summary>
    /// Gets or sets the pub/sub channel prefix for result notifications.
    /// </summary>
    public string PubSubChannelPrefix { get; set; } = "celery-task-done-";
}
