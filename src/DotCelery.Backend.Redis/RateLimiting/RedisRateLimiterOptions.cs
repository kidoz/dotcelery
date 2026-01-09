namespace DotCelery.Backend.Redis.RateLimiting;

/// <summary>
/// Configuration options for the Redis rate limiter.
/// </summary>
public sealed class RedisRateLimiterOptions
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
    /// Gets or sets the key prefix for rate limit counters.
    /// </summary>
    public string KeyPrefix { get; set; } = "dotcelery:ratelimit:";

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
