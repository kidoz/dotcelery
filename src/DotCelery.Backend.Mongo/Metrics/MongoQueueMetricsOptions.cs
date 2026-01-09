namespace DotCelery.Backend.Mongo.Metrics;

/// <summary>
/// Options for <see cref="MongoQueueMetrics"/>.
/// </summary>
public sealed class MongoQueueMetricsOptions
{
    /// <summary>
    /// Gets or sets the MongoDB connection string.
    /// </summary>
    public string ConnectionString { get; set; } = "mongodb://localhost:27017";

    /// <summary>
    /// Gets or sets the database name.
    /// </summary>
    public string DatabaseName { get; set; } = "dotcelery";

    /// <summary>
    /// Gets or sets the metrics collection name.
    /// </summary>
    public string MetricsCollectionName { get; set; } = "queue_metrics";

    /// <summary>
    /// Gets or sets the running tasks collection name.
    /// </summary>
    public string RunningTasksCollectionName { get; set; } = "running_tasks";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;
}
