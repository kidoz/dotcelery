namespace DotCelery.Backend.Redis.DeadLetter;

/// <summary>
/// Options for the Redis dead letter store.
/// </summary>
public sealed class RedisDeadLetterStoreOptions
{
    /// <summary>
    /// Gets or sets the Redis connection string.
    /// </summary>
    public string Configuration { get; set; } = "localhost:6379";

    /// <summary>
    /// Gets or sets the Redis key for the sorted set index.
    /// </summary>
    public string IndexKey { get; set; } = "dotcelery:dlq:index";

    /// <summary>
    /// Gets or sets the Redis key for the hash data store.
    /// </summary>
    public string DataKey { get; set; } = "dotcelery:dlq:data";

    /// <summary>
    /// Gets or sets the maximum number of messages to keep.
    /// </summary>
    public int MaxMessages { get; set; } = 10_000;
}
