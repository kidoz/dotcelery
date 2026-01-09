namespace DotCelery.Backend.Postgres.Metrics;

/// <summary>
/// Options for <see cref="PostgresQueueMetrics"/>.
/// </summary>
public sealed class PostgresQueueMetricsOptions
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
    /// Gets or sets the queue metrics table name.
    /// </summary>
    public string MetricsTableName { get; set; } = "dotcelery_queue_metrics";

    /// <summary>
    /// Gets or sets the running tasks table name.
    /// </summary>
    public string RunningTasksTableName { get; set; } = "dotcelery_running_tasks";

    /// <summary>
    /// Gets or sets whether to auto-create tables.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the command timeout.
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
