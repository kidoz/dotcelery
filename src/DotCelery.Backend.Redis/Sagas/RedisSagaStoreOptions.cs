using DotCelery.Core.Resilience;

namespace DotCelery.Backend.Redis.Sagas;

/// <summary>
/// Options for <see cref="RedisSagaStore"/>.
/// </summary>
public sealed class RedisSagaStoreOptions
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
    /// Gets or sets the key prefix for sagas.
    /// </summary>
    public string SagaKeyPrefix { get; set; } = "dotcelery:saga:";

    /// <summary>
    /// Gets or sets the key for task-to-saga mapping.
    /// </summary>
    public string TaskToSagaKey { get; set; } = "dotcelery:task-to-saga";

    /// <summary>
    /// Gets or sets the key prefix for saga state indexing.
    /// </summary>
    public string StateIndexKeyPrefix { get; set; } = "dotcelery:saga-state:";

    /// <summary>
    /// Gets or sets the TTL for completed sagas.
    /// </summary>
    public TimeSpan? CompletedSagaTtl { get; set; } = TimeSpan.FromDays(7);

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

    /// <summary>
    /// Gets or sets the TTL for task-to-saga mappings.
    /// When set, old mappings will be automatically expired.
    /// Defaults to null (no expiration - cleanup via saga deletion only).
    /// </summary>
    public TimeSpan? TaskMappingTtl { get; set; }

    /// <summary>
    /// Gets or sets the resilience options for retry behavior.
    /// </summary>
    public ResilienceOptions Resilience { get; set; } = new();
}
