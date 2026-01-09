using DotCelery.Core.Dashboard;

namespace DotCelery.Backend.Redis.Historical;

/// <summary>
/// Options for <see cref="RedisHistoricalDataStore"/>.
/// </summary>
public sealed class RedisHistoricalDataStoreOptions
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
    /// Gets or sets the key prefix for metrics snapshots.
    /// </summary>
    public string SnapshotKeyPrefix { get; set; } = "dotcelery:historical:snapshot:";

    /// <summary>
    /// Gets or sets the sorted set key for time-indexed snapshots.
    /// </summary>
    public string TimeIndexKey { get; set; } = "dotcelery:historical:index";

    /// <summary>
    /// Gets or sets the hash key for task-specific metrics.
    /// </summary>
    public string TaskMetricsKeyPrefix { get; set; } = "dotcelery:historical:task:";

    /// <summary>
    /// Gets or sets the retention period for historical data.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(30);

    /// <summary>
    /// Gets or sets the maximum number of data points for time series queries.
    /// </summary>
    public int MaxDataPoints { get; set; } = 1000;

    /// <summary>
    /// Gets or sets the default granularity for queries.
    /// </summary>
    public MetricsGranularity DefaultGranularity { get; set; } = MetricsGranularity.Hour;

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
