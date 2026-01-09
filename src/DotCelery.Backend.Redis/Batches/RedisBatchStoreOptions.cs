namespace DotCelery.Backend.Redis.Batches;

/// <summary>
/// Configuration options for <see cref="RedisBatchStore"/>.
/// </summary>
public sealed class RedisBatchStoreOptions
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
    /// Gets or sets the key prefix for batch data.
    /// </summary>
    public string BatchKeyPrefix { get; set; } = "dotcelery:batch:";

    /// <summary>
    /// Gets or sets the key for the task-to-batch index.
    /// </summary>
    public string TaskToBatchKey { get; set; } = "dotcelery:task-batch-index";

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
    /// Gets or sets the default batch TTL (time to live) after completion.
    /// </summary>
    public TimeSpan? BatchTtl { get; set; } = TimeSpan.FromDays(7);
}
