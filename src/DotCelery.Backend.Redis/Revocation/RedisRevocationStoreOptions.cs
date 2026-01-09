namespace DotCelery.Backend.Redis.Revocation;

/// <summary>
/// Configuration options for the Redis revocation store.
/// </summary>
public sealed class RedisRevocationStoreOptions
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
    /// Gets or sets the Redis key for the revocations hash.
    /// Stores task revocation entries keyed by task ID.
    /// </summary>
    public string RevocationsKey { get; set; } = "dotcelery:revoked";

    /// <summary>
    /// Gets or sets the Redis key for the revocation expiry sorted set.
    /// Used for efficient cleanup of expired revocations.
    /// </summary>
    public string RevocationExpiryKey { get; set; } = "dotcelery:revoked:expiry";

    /// <summary>
    /// Gets or sets the pub/sub channel for revocation notifications.
    /// </summary>
    public string RevocationChannel { get; set; } = "dotcelery:revocation";

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
