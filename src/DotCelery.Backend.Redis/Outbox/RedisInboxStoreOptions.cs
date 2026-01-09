namespace DotCelery.Backend.Redis.Outbox;

/// <summary>
/// Configuration options for <see cref="RedisInboxStore"/>.
/// </summary>
public sealed class RedisInboxStoreOptions
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
    /// Gets or sets the key for the processed messages hash.
    /// </summary>
    public string ProcessedMessagesKey { get; set; } = "dotcelery:inbox:processed";

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
    /// Gets or sets the default TTL for processed message records.
    /// </summary>
    public TimeSpan DefaultTtl { get; set; } = TimeSpan.FromDays(7);
}
