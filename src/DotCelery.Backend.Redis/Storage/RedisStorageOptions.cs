namespace DotCelery.Backend.Redis.Storage;

/// <summary>
/// Options for the Redis storage primitives.
/// </summary>
public sealed class RedisStorageOptions
{
    /// <summary>
    /// The connection string used when none is configured. Suitable for local development only.
    /// </summary>
    public const string DevelopmentDefaultConnectionString = "localhost:6379";

    /// <summary>
    /// Gets or sets the Redis connection string. Default is <c>localhost:6379</c>.
    /// </summary>
    public string ConnectionString { get; set; } = DevelopmentDefaultConnectionString;

    /// <summary>
    /// Gets or sets the database number. Default is 0.
    /// </summary>
    public int Database { get; set; }

    /// <summary>
    /// Gets or sets the prefix of every key the storage uses. Default is <c>dotcelery:</c>.
    /// </summary>
    public string KeyPrefix { get; set; } = "dotcelery:";

    /// <summary>
    /// Gets or sets how long to wait for a connection. Default is 5 seconds.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets how long to wait for a command. Default is 5 seconds.
    /// </summary>
    public TimeSpan SyncTimeout { get; set; } = TimeSpan.FromSeconds(5);
}
