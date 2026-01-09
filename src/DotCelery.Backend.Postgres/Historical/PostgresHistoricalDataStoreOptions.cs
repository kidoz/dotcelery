using DotCelery.Core.Dashboard;

namespace DotCelery.Backend.Postgres.Historical;

/// <summary>
/// Options for <see cref="PostgresHistoricalDataStore"/>.
/// </summary>
public sealed class PostgresHistoricalDataStoreOptions
{
    /// <summary>
    /// Gets or sets the PostgreSQL connection string.
    /// </summary>
    public string ConnectionString { get; set; } = "Host=localhost;Database=dotcelery";

    /// <summary>
    /// Gets or sets the schema name.
    /// </summary>
    public string Schema { get; set; } = "public";

    /// <summary>
    /// Gets or sets the snapshots table name.
    /// </summary>
    public string SnapshotsTableName { get; set; } = "dotcelery_metrics_snapshots";

    /// <summary>
    /// Gets or sets whether to auto-create tables.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the command timeout.
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the retention period.
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
}
